/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fugue.coordinator.manager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages.*;
import org.apache.flink.fugue.common.state.MigrationContext;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Migration Manager that coordinates the two-phase migration protocol.
 * Manages the state machine transitions and RPC communication with TaskManagers.
 */
public class MigrationManager {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationManager.class);

    /** The job this manager is responsible for. */
    private final JobID jobId;

    /** Map of migration ID to migration context. */
    private final Map<Long, MigrationContext> activeMigrations;

    /** Map of partition ID to current migration (ensures no overlapping migrations). */
    private final Map<Integer, Long> partitionToMigration;

    /** Lock for thread-safe access. */
    private final ReentrantReadWriteLock lock;

    /** Executor for async operations. */
    private final ScheduledExecutorService executor;

    /** RPC gateway for communication with TaskManagers. */
    private final MigrationRpcGateway rpcGateway;

    /** Configuration for migration parameters. */
    private PolicyConfiguration configuration;

    /** Counter for generating unique migration IDs. */
    private final AtomicLong migrationIdCounter;

    /** Callback for barrier injection. */
    private BarrierInjectionCallback barrierCallback;

    /** Active timers for timeouts. */
    private final Map<Long, ScheduledFuture<?>> timeoutTimers;

    public MigrationManager(JobID jobId, MigrationRpcGateway rpcGateway) {
        this.jobId = jobId;
        this.activeMigrations = new ConcurrentHashMap<>();
        this.partitionToMigration = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.executor = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r);
            t.setName("fugue-migration-manager-" + jobId);
            t.setDaemon(true);
            return t;
        });
        this.rpcGateway = rpcGateway;
        this.configuration = new PolicyConfiguration();
        this.migrationIdCounter = new AtomicLong(0);
        this.timeoutTimers = new ConcurrentHashMap<>();
    }

    /**
     * Start new migrations for the given plans.
     */
    public CompletableFuture<List<Long>> startMigrations(List<MigrationPlan> plans) {
        lock.writeLock().lock();
        try {
            List<CompletableFuture<Long>> futures = new ArrayList<>();

            for (MigrationPlan plan : plans) {
                // Check if partition is already being migrated
                if (partitionToMigration.containsKey(plan.getPartitionId())) {
                    LOG.warn("Partition {} is already being migrated", plan.getPartitionId());
                    futures.add(CompletableFuture.completedFuture(-1L));
                    continue;
                }

                long migrationId = migrationIdCounter.incrementAndGet();
                MigrationContext context = new MigrationContext(plan, migrationId);

                activeMigrations.put(migrationId, context);
                partitionToMigration.put(plan.getPartitionId(), migrationId);

                CompletableFuture<Long> future = initiateMigration(context);
                futures.add(future);

                LOG.info("Started migration {} for partition {}: {} -> {}",
                        migrationId, plan.getPartitionId(),
                        plan.getSourceInstance().getSubtaskIndex(),
                        plan.getTargetInstance().getSubtaskIndex());
            }

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futures.stream()
                            .map(CompletableFuture::join)
                            .filter(id -> id > 0)
                            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Initiate a migration by transitioning to INIT state.
     */
    private CompletableFuture<Long> initiateMigration(MigrationContext context) {
        if (!context.transitionTo(MigrationState.INIT)) {
            LOG.error("Failed to transition migration {} to INIT state", context.getMigrationId());
            return CompletableFuture.completedFuture(-1L);
        }

        // Set up timeout timer
        long timeout = configuration.getLong("migration.init.timeout", 30000L);
        ScheduledFuture<?> timer = executor.schedule(
                () -> handleTimeout(context.getMigrationId(), MigrationState.INIT),
                timeout, TimeUnit.MILLISECONDS);
        timeoutTimers.put(context.getMigrationId(), timer);

        // Send prepare messages to source and target
        CompletableFuture<Void> sourcePrepare = prepareSource(context);
        CompletableFuture<Void> targetPrepare = prepareTarget(context);

        return CompletableFuture.allOf(sourcePrepare, targetPrepare)
                .thenCompose(v -> {
                    cancelTimeout(context.getMigrationId());
                    return startPreCopy(context);
                })
                .thenApply(v -> context.getMigrationId())
                .exceptionally(throwable -> {
                    LOG.error("Migration {} failed during initialization",
                            context.getMigrationId(), throwable);
                    abortMigration(context.getMigrationId(), throwable.getMessage());
                    return -1L;
                });
    }

    /**
     * Prepare source operator for migration.
     */
    private CompletableFuture<Void> prepareSource(MigrationContext context) {
        MigrationPlan plan = context.getMigrationPlan();

        // Determine target address and port (would be obtained from TaskManager registry)
        String targetAddress = plan.getTargetInstance().getTaskManagerLocation();
        int targetPort = 8888; // Default state transfer port

        PrepareMigrationSource message = new PrepareMigrationSource(
                context.getMigrationId(),
                plan,
                targetAddress,
                targetPort);

        return rpcGateway.sendToTaskManager(
                plan.getSourceInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> {
                    if (response instanceof MigrationResponse) {
                        MigrationResponse resp = (MigrationResponse) response;
                        if (!resp.isSuccess()) {
                            throw new RuntimeException("Failed to prepare source: " + resp.getMessage());
                        }
                    }
                });
    }

    /**
     * Prepare target operator for migration.
     */
    private CompletableFuture<Void> prepareTarget(MigrationContext context) {
        MigrationPlan plan = context.getMigrationPlan();
        int listenPort = 8888; // Default state transfer port

        PrepareMigrationTarget message = new PrepareMigrationTarget(
                context.getMigrationId(),
                plan,
                listenPort);

        return rpcGateway.sendToTaskManager(
                plan.getTargetInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> {
                    if (response instanceof MigrationResponse) {
                        MigrationResponse resp = (MigrationResponse) response;
                        if (!resp.isSuccess()) {
                            throw new RuntimeException("Failed to prepare target: " + resp.getMessage());
                        }
                    }
                });
    }

    /**
     * Start pre-copy phase.
     */
    private CompletableFuture<Void> startPreCopy(MigrationContext context) {
        if (!context.transitionTo(MigrationState.PRE_COPY)) {
            LOG.error("Failed to transition migration {} to PRE_COPY state", context.getMigrationId());
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid state transition"));
        }

        LOG.info("Starting pre-copy phase for migration {}", context.getMigrationId());

        // Get rate limit from configuration
        long rateLimitMbps = configuration.getLong(
                PolicyConfiguration.NETWORK_BANDWIDTH_LIMIT, 100L);
        long rateLimitBytesPerSec = rateLimitMbps * 1024 * 1024;

        StartPreCopy message = new StartPreCopy(
                context.getMigrationId(),
                context.getMigrationPlan().getPartitionId(),
                rateLimitBytesPerSec);

        return rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                message)
                .thenCompose(response -> monitorPreCopy(context));
    }

    /**
     * Monitor pre-copy progress and handle convergence.
     */
    private CompletableFuture<Void> monitorPreCopy(MigrationContext context) {
        return CompletableFuture.runAsync(() -> {
            long migrationId = context.getMigrationId();
            int maxRounds = configuration.getInt(PolicyConfiguration.MAX_PRECOPY_ROUNDS, 10);

            while (context.getCurrentState() == MigrationState.PRE_COPY) {
                try {
                    // Wait for pre-copy round completion
                    Thread.sleep(5000); // Check every 5 seconds

                    // In real implementation, this would wait for PreCopyRoundComplete messages
                    context.incrementPreCopyRound();

                    // Simulate convergence check
                    if (context.getPreCopyRounds() >= 3 || context.hasConverged()) {
                        LOG.info("Pre-copy converged for migration {} after {} rounds",
                                migrationId, context.getPreCopyRounds());

                        // Transition to await barrier
                        if (context.transitionTo(MigrationState.AWAIT_BARRIER)) {
                            injectBarrier(context);
                        }
                        break;
                    }

                    if (context.getPreCopyRounds() >= maxRounds) {
                        LOG.warn("Pre-copy failed to converge for migration {} after {} rounds",
                                migrationId, maxRounds);
                        abortMigration(migrationId, "Pre-copy convergence timeout");
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, executor);
    }

    /**
     * Inject migration barrier into the dataflow.
     */
    private void injectBarrier(MigrationContext context) {
        LOG.info("Injecting barrier for migration {}", context.getMigrationId());

        context.setBarrierInjected(true);

        if (barrierCallback != null) {
            MigrationBarrier barrier = MigrationBarrier.createStandalone(
                    context.getMigrationId(),
                    Collections.singletonList(context.getMigrationPlan()));

            barrierCallback.injectBarrier(barrier);
        }

        // Set up timer for barrier alignment
        long timeout = configuration.getLong("migration.barrier.timeout", 60000L);
        executor.schedule(
                () -> checkBarrierAlignment(context),
                timeout,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Check if barrier has been aligned and proceed with finalization.
     */
    private void checkBarrierAlignment(MigrationContext context) {
        // In real implementation, this would be triggered by barrier alignment notification
        if (context.getCurrentState() == MigrationState.AWAIT_BARRIER) {
            if (context.transitionTo(MigrationState.FINALIZING)) {
                transferFinalDelta(context);
            }
        }
    }

    /**
     * Transfer final delta after barrier alignment.
     */
    private void transferFinalDelta(MigrationContext context) {
        LOG.info("Transferring final delta for migration {}", context.getMigrationId());

        TransferFinalDelta message = new TransferFinalDelta(
                context.getMigrationId(),
                context.getMigrationPlan().getPartitionId());

        rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> {
                    context.setFinalDeltaSent(true);
                    commitMigration(context);
                })
                .exceptionally(throwable -> {
                    LOG.error("Failed to transfer final delta for migration {}",
                            context.getMigrationId(), throwable);
                    abortMigration(context.getMigrationId(), throwable.getMessage());
                    return null;
                });
    }

    /**
     * Commit the migration.
     */
    private void commitMigration(MigrationContext context) {
        if (!context.transitionTo(MigrationState.COMMITTED)) {
            LOG.error("Failed to transition migration {} to COMMITTED state",
                    context.getMigrationId());
            return;
        }

        LOG.info("Migration {} committed successfully in {}ms",
                context.getMigrationId(), context.getDuration());

        // Clean up
        finalizeMigration(context.getMigrationId(), true);

        // Send commit messages to source and target
        CommitMigration message = new CommitMigration(
                context.getMigrationId(),
                context.getMigrationPlan().getPartitionId());

        rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                message);
        rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getTargetInstance().getTaskManagerLocation(),
                message);
    }

    /**
     * Abort a migration.
     */
    public void abortMigration(long migrationId, String reason) {
        lock.writeLock().lock();
        try {
            MigrationContext context = activeMigrations.get(migrationId);
            if (context == null) {
                return;
            }

            context.forceTransitionTo(MigrationState.ABORTED);
            context.setLastError(reason);

            LOG.warn("Migration {} aborted: {}", migrationId, reason);

            // Send abort messages
            AbortMigration message = new AbortMigration(migrationId, reason);

            rpcGateway.sendToTaskManager(
                    context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                    message);
            rpcGateway.sendToTaskManager(
                    context.getMigrationPlan().getTargetInstance().getTaskManagerLocation(),
                    message);

            finalizeMigration(migrationId, false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle RPC message from TaskManager.
     */
    public void handleMessage(MigrationMessage message) {
        long migrationId = message.getMigrationId();
        MigrationContext context = activeMigrations.get(migrationId);

        if (context == null) {
            LOG.warn("Received message for unknown migration: {}", migrationId);
            return;
        }

        if (message instanceof PreCopyRoundComplete) {
            handlePreCopyRoundComplete(context, (PreCopyRoundComplete) message);
        } else if (message instanceof FinalDeltaComplete) {
            handleFinalDeltaComplete(context, (FinalDeltaComplete) message);
        } else if (message instanceof StateUpdate) {
            handleStateUpdate(context, (StateUpdate) message);
        } else if (message instanceof MigrationStats) {
            handleMigrationStats(context, (MigrationStats) message);
        }
    }

    /**
     * Handle pre-copy round completion.
     */
    private void handlePreCopyRoundComplete(MigrationContext context, PreCopyRoundComplete message) {
        LOG.info("Pre-copy round {} complete for migration {}, deltaSize={}, converged={}",
                message.getRoundNumber(), context.getMigrationId(),
                message.getDeltaSize(), message.isConverged());

        context.updateDeltaSize(message.getDeltaSize());
        context.addBytesTransferred(message.getBytesTransferred());

        if (message.isConverged() && context.getCurrentState() == MigrationState.PRE_COPY) {
            if (context.transitionTo(MigrationState.AWAIT_BARRIER)) {
                injectBarrier(context);
            }
        }
    }

    /**
     * Handle final delta completion.
     */
    private void handleFinalDeltaComplete(MigrationContext context, FinalDeltaComplete message) {
        LOG.info("Final delta complete for migration {}, size={}",
                context.getMigrationId(), message.getFinalDeltaSize());

        context.addBytesTransferred(message.getFinalDeltaSize());

        if (context.getCurrentState() == MigrationState.FINALIZING) {
            commitMigration(context);
        }
    }

    /**
     * Handle state update from TaskManager.
     */
    private void handleStateUpdate(MigrationContext context, StateUpdate message) {
        LOG.info("State update for migration {}: {} - {}",
                context.getMigrationId(), message.getNewState(), message.getDetails());
    }

    /**
     * Handle migration statistics.
     */
    private void handleMigrationStats(MigrationContext context, MigrationStats message) {
        LOG.info("Migration {} stats: duration={}ms, transferred={} bytes, rounds={}, retention={}%",
                context.getMigrationId(),
                message.getDuration(),
                message.getBytesTransferred(),
                message.getPreCopyRounds(),
                message.getThroughputRetention() * 100);
    }

    /**
     * Handle timeout for a migration state.
     */
    private void handleTimeout(long migrationId, MigrationState state) {
        MigrationContext context = activeMigrations.get(migrationId);
        if (context != null && context.getCurrentState() == state) {
            LOG.error("Migration {} timed out in state {}", migrationId, state);
            abortMigration(migrationId, "Timeout in state " + state);
        }
    }

    /**
     * Cancel timeout timer for a migration.
     */
    private void cancelTimeout(long migrationId) {
        ScheduledFuture<?> timer = timeoutTimers.remove(migrationId);
        if (timer != null) {
            timer.cancel(false);
        }
    }

    /**
     * Finalize a migration (cleanup).
     */
    private void finalizeMigration(long migrationId, boolean success) {
        lock.writeLock().lock();
        try {
            MigrationContext context = activeMigrations.remove(migrationId);
            if (context != null) {
                partitionToMigration.remove(context.getMigrationPlan().getPartitionId());
                cancelTimeout(migrationId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get active migrations.
     */
    public Collection<MigrationContext> getActiveMigrations() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(activeMigrations.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get migration by ID.
     */
    public MigrationContext getMigration(long migrationId) {
        return activeMigrations.get(migrationId);
    }

    /**
     * Set barrier injection callback.
     */
    public void setBarrierCallback(BarrierInjectionCallback callback) {
        this.barrierCallback = callback;
    }

    /**
     * Update configuration.
     */
    public void updateConfiguration(PolicyConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Shutdown the manager.
     */
    public void shutdown() {
        // Abort all active migrations
        lock.writeLock().lock();
        try {
            for (MigrationContext context : activeMigrations.values()) {
                abortMigration(context.getMigrationId(), "Manager shutdown");
            }
        } finally {
            lock.writeLock().unlock();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Callback interface for barrier injection.
     */
    public interface BarrierInjectionCallback {
        void injectBarrier(MigrationBarrier barrier);
    }

    /**
     * RPC gateway interface for TaskManager communication.
     */
    public interface MigrationRpcGateway {
        CompletableFuture<MigrationMessage> sendToTaskManager(String taskManagerLocation, MigrationMessage message);
    }
}