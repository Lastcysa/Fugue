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

package org.apache.flink.fugue.integration.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.coordinator.manager.MigrationManager;
import org.apache.flink.fugue.coordinator.planner.ElasticityPlanner;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;
import org.apache.flink.fugue.coordinator.rpc.FugueRpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Service that integrates Fugue into Flink's JobMaster.
 * Manages the lifecycle of ElasticityPlanner and MigrationManager.
 */
public class FugueJobMasterService {
    private static final Logger LOG = LoggerFactory.getLogger(FugueJobMasterService.class);

    /** The job this service manages. */
    private final JobID jobId;

    /** Elasticity planner for making scaling decisions. */
    private final ElasticityPlanner elasticityPlanner;

    /** Migration manager for coordinating migrations. */
    private final MigrationManager migrationManager;

    /** RPC gateway for communication with TaskManagers. */
    private final FugueRpcGateway rpcGateway;

    /** Configuration. */
    private final PolicyConfiguration configuration;

    /** Whether the service is started. */
    private volatile boolean started = false;

    /**
     * Create Fugue service for a job.
     *
     * @param jobId The job ID
     * @param rpcService Flink's RPC service
     * @param configuration Fugue configuration
     */
    public FugueJobMasterService(
            JobID jobId,
            RpcService rpcService,
            PolicyConfiguration configuration) {
        this.jobId = jobId;
        this.configuration = configuration;

        // Create RPC gateway
        this.rpcGateway = new FugueRpcGateway(rpcService);

        // Create migration manager
        this.migrationManager = new MigrationManager(jobId, rpcGateway);
        this.migrationManager.updateConfiguration(configuration);

        // Set up barrier injection callback
        this.migrationManager.setBarrierCallback(this::injectBarrier);

        // Create elasticity planner
        this.elasticityPlanner = new ElasticityPlanner(jobId);
        this.elasticityPlanner.updateConfiguration(configuration);

        // Set up migration plan callback
        this.elasticityPlanner.setPlanCallback(this::onNewMigrationPlans);

        LOG.info("Fugue JobMaster service created for job {}", jobId);
    }

    /**
     * Start the Fugue service.
     * This should be called when the JobMaster starts.
     */
    public void start() {
        if (started) {
            LOG.warn("Fugue service already started");
            return;
        }

        LOG.info("Starting Fugue service for job {}", jobId);

        // Start elasticity planner
        elasticityPlanner.start();

        started = true;

        LOG.info("Fugue service started for job {}", jobId);
    }

    /**
     * Stop the Fugue service.
     * This should be called when the JobMaster stops.
     */
    public void stop() {
        if (!started) {
            return;
        }

        LOG.info("Stopping Fugue service for job {}", jobId);

        // Stop elasticity planner
        elasticityPlanner.stop();

        // Shutdown migration manager
        migrationManager.shutdown();

        // Close RPC gateway
        rpcGateway.close();

        started = false;

        LOG.info("Fugue service stopped for job {}", jobId);
    }

    /**
     * Callback when new migration plans are generated.
     */
    private void onNewMigrationPlans(List<MigrationPlan> plans) {
        LOG.info("Received {} new migration plans from elasticity planner", plans.size());

        // Start migrations through migration manager
        migrationManager.startMigrations(plans)
                .thenAccept(migrationIds -> {
                    LOG.info("Started {} migrations: {}", migrationIds.size(), migrationIds);
                })
                .exceptionally(throwable -> {
                    LOG.error("Failed to start migrations", throwable);
                    return null;
                });
    }

    /**
     * Inject migration barrier into the dataflow.
     * This needs to be integrated with Flink's source operators.
     */
    private void injectBarrier(MigrationBarrier barrier) {
        LOG.info("Injecting migration barrier: {}", barrier);

        // TODO: Integration point with Flink's source operators
        // In a full implementation, this would:
        // 1. Get all source operators for the job
        // 2. Send barrier injection command to each source
        // 3. Sources emit the barrier into their output streams

        // For now, log the injection
        LOG.warn("Barrier injection not fully integrated - requires Flink source operator hooks");
    }

    /**
     * Trigger manual evaluation (for testing or manual scaling).
     */
    public void triggerEvaluation() {
        if (!started) {
            LOG.warn("Cannot trigger evaluation - service not started");
            return;
        }

        LOG.info("Manual evaluation triggered");
        elasticityPlanner.triggerEvaluation();
    }

    /**
     * Update configuration dynamically.
     */
    public void updateConfiguration(PolicyConfiguration newConfiguration) {
        LOG.info("Updating Fugue configuration");

        elasticityPlanner.updateConfiguration(newConfiguration);
        migrationManager.updateConfiguration(newConfiguration);

        LOG.info("Configuration updated");
    }

    /**
     * Get current status.
     */
    public ServiceStatus getStatus() {
        return new ServiceStatus(
                started,
                elasticityPlanner.isActive(),
                elasticityPlanner.getScalingHistory().size(),
                migrationManager.getActiveMigrations().size());
    }

    /**
     * Service status information.
     */
    public static class ServiceStatus {
        private final boolean started;
        private final boolean plannerActive;
        private final int scalingHistorySize;
        private final int activeMigrations;

        public ServiceStatus(boolean started, boolean plannerActive,
                           int scalingHistorySize, int activeMigrations) {
            this.started = started;
            this.plannerActive = plannerActive;
            this.scalingHistorySize = scalingHistorySize;
            this.activeMigrations = activeMigrations;
        }

        public boolean isStarted() {
            return started;
        }

        public boolean isPlannerActive() {
            return plannerActive;
        }

        public int getScalingHistorySize() {
            return scalingHistorySize;
        }

        public int getActiveMigrations() {
            return activeMigrations;
        }

        @Override
        public String toString() {
            return String.format("FugueStatus{started=%b, plannerActive=%b, history=%d, migrations=%d}",
                    started, plannerActive, scalingHistorySize, activeMigrations);
        }
    }
}