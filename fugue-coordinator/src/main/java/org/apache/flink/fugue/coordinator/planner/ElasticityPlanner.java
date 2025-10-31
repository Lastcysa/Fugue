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

package org.apache.flink.fugue.coordinator.planner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Main elasticity planner that coordinates scaling decisions.
 * Periodically evaluates system metrics and triggers migrations when needed.
 */
public class ElasticityPlanner {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticityPlanner.class);

    /** The job ID this planner is responsible for. */
    private final JobID jobId;

    /** The elasticity policy to use for decisions. */
    private ElasticityPolicy policy;

    /** Metrics collector for gathering system metrics. */
    private final MetricsCollector metricsCollector;

    /** Executor for periodic evaluation. */
    private final ScheduledExecutorService scheduler;

    /** Future for the periodic evaluation task. */
    private ScheduledFuture<?> evaluationTask;

    /** Lock for thread-safe operations. */
    private final ReentrantLock plannerLock;

    /** Flag indicating if planner is active. */
    private final AtomicBoolean isActive;

    /** Callback for when new migration plans are generated. */
    private MigrationPlanCallback planCallback;

    /** Configuration for the planner. */
    private PolicyConfiguration configuration;

    /** History of recent scaling actions. */
    private final List<ScalingHistory> scalingHistory;

    public ElasticityPlanner(JobID jobId) {
        this.jobId = jobId;
        this.policy = new ThresholdBasedPolicy(jobId);
        this.metricsCollector = new MetricsCollector();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r);
                    t.setName("fugue-elasticity-planner-" + jobId);
                    t.setDaemon(true);
                    return t;
                });
        this.plannerLock = new ReentrantLock();
        this.isActive = new AtomicBoolean(false);
        this.configuration = new PolicyConfiguration();
        this.scalingHistory = new ArrayList<>();
    }

    /**
     * Start the elasticity planner.
     */
    public void start() {
        plannerLock.lock();
        try {
            if (isActive.get()) {
                LOG.warn("ElasticityPlanner is already active");
                return;
            }

            isActive.set(true);
            long evaluationInterval = configuration.getLong(
                    PolicyConfiguration.EVALUATION_INTERVAL, 30000L);

            evaluationTask = scheduler.scheduleWithFixedDelay(
                    this::evaluate,
                    evaluationInterval,
                    evaluationInterval,
                    TimeUnit.MILLISECONDS);

            LOG.info("ElasticityPlanner started with evaluation interval: {}ms", evaluationInterval);
        } finally {
            plannerLock.unlock();
        }
    }

    /**
     * Stop the elasticity planner.
     */
    public void stop() {
        plannerLock.lock();
        try {
            if (!isActive.get()) {
                return;
            }

            isActive.set(false);

            if (evaluationTask != null) {
                evaluationTask.cancel(false);
                evaluationTask = null;
            }

            LOG.info("ElasticityPlanner stopped");
        } finally {
            plannerLock.unlock();
        }
    }

    /**
     * Shutdown the planner and release resources.
     */
    public void shutdown() {
        stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Perform evaluation and generate migration plans if needed.
     */
    private void evaluate() {
        if (!isActive.get()) {
            return;
        }

        try {
            LOG.debug("Starting elasticity evaluation");

            // Collect current metrics
            SystemMetrics metrics = metricsCollector.collectMetrics();
            if (metrics == null) {
                LOG.debug("No metrics available, skipping evaluation");
                return;
            }

            // Evaluate scaling needs
            ElasticityPolicy.ScalingAction action = policy.evaluate(metrics);
            if (action == null) {
                LOG.debug("No scaling action needed");
                return;
            }

            LOG.info("Scaling action decided: {}", action);

            // Filter out overlapping migration plans
            List<MigrationPlan> filteredPlans = filterOverlappingPlans(action.getMigrationPlans());
            if (filteredPlans.isEmpty()) {
                LOG.info("No valid migration plans after filtering");
                return;
            }

            // Record scaling history
            recordScalingAction(action, metrics);

            // Trigger migration plans
            if (planCallback != null) {
                planCallback.onNewMigrationPlans(filteredPlans);
            }

        } catch (Exception e) {
            LOG.error("Error during elasticity evaluation", e);
        }
    }

    /**
     * Filter out overlapping migration plans.
     * Ensures no partition is involved in multiple migrations.
     */
    private List<MigrationPlan> filterOverlappingPlans(List<MigrationPlan> plans) {
        if (plans == null || plans.isEmpty()) {
            return new ArrayList<>();
        }

        List<MigrationPlan> filtered = new ArrayList<>();
        List<Integer> usedPartitions = new ArrayList<>();

        for (MigrationPlan plan : plans) {
            if (!usedPartitions.contains(plan.getPartitionId())) {
                filtered.add(plan);
                usedPartitions.add(plan.getPartitionId());

                // Limit concurrent migrations
                int maxConcurrent = configuration.getInt(
                        PolicyConfiguration.MAX_CONCURRENT_MIGRATIONS, 10);
                if (filtered.size() >= maxConcurrent) {
                    break;
                }
            }
        }

        return filtered;
    }

    /**
     * Record scaling action in history.
     */
    private void recordScalingAction(ElasticityPolicy.ScalingAction action, SystemMetrics metrics) {
        ScalingHistory history = new ScalingHistory(
                System.currentTimeMillis(),
                action.getDecision(),
                action.getReason(),
                metrics.getJobMetrics().getTotalThroughput(),
                metrics.getJobMetrics().getP99Latency());

        scalingHistory.add(history);

        // Keep only recent history (last 100 entries)
        if (scalingHistory.size() > 100) {
            scalingHistory.remove(0);
        }
    }

    /**
     * Manually trigger an evaluation.
     */
    public void triggerEvaluation() {
        scheduler.execute(this::evaluate);
    }

    /**
     * Set the elasticity policy.
     */
    public void setPolicy(ElasticityPolicy policy) {
        plannerLock.lock();
        try {
            this.policy = policy;
            LOG.info("Elasticity policy updated");
        } finally {
            plannerLock.unlock();
        }
    }

    /**
     * Update planner configuration.
     */
    public void updateConfiguration(PolicyConfiguration configuration) {
        plannerLock.lock();
        try {
            this.configuration = configuration;
            this.policy.updateConfiguration(configuration);

            // Restart with new evaluation interval if needed
            if (isActive.get()) {
                stop();
                start();
            }

            LOG.info("Planner configuration updated");
        } finally {
            plannerLock.unlock();
        }
    }

    /**
     * Set the callback for new migration plans.
     */
    public void setPlanCallback(MigrationPlanCallback callback) {
        this.planCallback = callback;
    }

    /**
     * Get the current configuration.
     */
    public PolicyConfiguration getConfiguration() {
        return configuration.copy();
    }

    /**
     * Get scaling history.
     */
    public List<ScalingHistory> getScalingHistory() {
        return new ArrayList<>(scalingHistory);
    }

    /**
     * Check if planner is active.
     */
    public boolean isActive() {
        return isActive.get();
    }

    /**
     * Callback interface for new migration plans.
     */
    public interface MigrationPlanCallback {
        void onNewMigrationPlans(List<MigrationPlan> plans);
    }

    /**
     * Scaling history entry.
     */
    public static class ScalingHistory {
        private final long timestamp;
        private final ElasticityPolicy.ScalingDecision decision;
        private final String reason;
        private final double throughput;
        private final double latency;

        public ScalingHistory(
                long timestamp,
                ElasticityPolicy.ScalingDecision decision,
                String reason,
                double throughput,
                double latency) {
            this.timestamp = timestamp;
            this.decision = decision;
            this.reason = reason;
            this.throughput = throughput;
            this.latency = latency;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public ElasticityPolicy.ScalingDecision getDecision() {
            return decision;
        }

        public String getReason() {
            return reason;
        }

        public double getThroughput() {
            return throughput;
        }

        public double getLatency() {
            return latency;
        }
    }

    /**
     * Metrics collector stub.
     * In real implementation, this would integrate with Flink's metrics system.
     */
    private static class MetricsCollector {
        public SystemMetrics collectMetrics() {
            // Stub implementation - would integrate with Flink metrics
            return new SystemMetrics();
        }
    }
}