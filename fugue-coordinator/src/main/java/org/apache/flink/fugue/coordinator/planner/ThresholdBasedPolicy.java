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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Threshold-based elasticity policy implementation.
 * Makes scaling decisions based on configurable thresholds for various metrics.
 */
public class ThresholdBasedPolicy implements ElasticityPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(ThresholdBasedPolicy.class);

    private PolicyConfiguration configuration;
    private long lastScalingTime;
    private final JobID jobId;
    private final ConsistentHashRouter router;

    public ThresholdBasedPolicy(JobID jobId) {
        this.jobId = jobId;
        this.configuration = new PolicyConfiguration();
        this.lastScalingTime = 0;
        this.router = new ConsistentHashRouter();
    }

    @Override
    public ScalingAction evaluate(SystemMetrics metrics) {
        // Check cooldown period
        long currentTime = System.currentTimeMillis();
        long cooldownPeriod = configuration.getLong(
                PolicyConfiguration.COOLDOWN_PERIOD, 60000L);

        if (currentTime - lastScalingTime < cooldownPeriod) {
            LOG.debug("Still in cooldown period, skipping evaluation");
            return null;
        }

        // Check for scale-out conditions
        ScalingAction scaleOutAction = checkScaleOutConditions(metrics);
        if (scaleOutAction != null) {
            LOG.info("Scale-out triggered: {}", scaleOutAction.getReason());
            return scaleOutAction;
        }

        // Check for scale-in conditions
        ScalingAction scaleInAction = checkScaleInConditions(metrics);
        if (scaleInAction != null) {
            LOG.info("Scale-in triggered: {}", scaleInAction.getReason());
            return scaleInAction;
        }

        // Check for rebalancing needs
        ScalingAction rebalanceAction = checkRebalanceConditions(metrics);
        if (rebalanceAction != null) {
            LOG.info("Rebalance triggered: {}", rebalanceAction.getReason());
            return rebalanceAction;
        }

        return null;
    }

    private ScalingAction checkScaleOutConditions(SystemMetrics metrics) {
        SystemMetrics.JobMetrics jobMetrics = metrics.getJobMetrics();

        // Check CPU threshold
        double cpuThreshold = configuration.getDouble(
                PolicyConfiguration.CPU_SCALE_OUT_THRESHOLD, 0.8);
        if (jobMetrics.getCpuUtilization() > cpuThreshold) {
            return createScaleOutAction(metrics,
                    String.format("CPU utilization %.2f%% exceeds threshold %.2f%%",
                            jobMetrics.getCpuUtilization() * 100, cpuThreshold * 100));
        }

        // Check memory threshold
        double memThreshold = configuration.getDouble(
                PolicyConfiguration.MEMORY_SCALE_OUT_THRESHOLD, 0.85);
        long totalMemory = Runtime.getRuntime().maxMemory();
        double memUsage = (double) jobMetrics.getTotalMemoryUsage() / totalMemory;
        if (memUsage > memThreshold) {
            return createScaleOutAction(metrics,
                    String.format("Memory usage %.2f%% exceeds threshold %.2f%%",
                            memUsage * 100, memThreshold * 100));
        }

        // Check latency threshold
        double latencyThreshold = configuration.getDouble(
                PolicyConfiguration.LATENCY_SCALE_OUT_THRESHOLD, 1000.0);
        if (jobMetrics.getP99Latency() > latencyThreshold) {
            return createScaleOutAction(metrics,
                    String.format("P99 latency %.2fms exceeds threshold %.2fms",
                            jobMetrics.getP99Latency(), latencyThreshold));
        }

        // Check backpressure
        double backpressureThreshold = configuration.getDouble(
                PolicyConfiguration.BACKPRESSURE_THRESHOLD, 0.5);
        if (jobMetrics.isBackpressured()) {
            OperatorID bottleneck = metrics.findBottleneck();
            if (bottleneck != null) {
                SystemMetrics.OperatorMetrics opMetrics = metrics.getOperatorMetrics(bottleneck);
                if (opMetrics != null && opMetrics.getBackpressure() > backpressureThreshold) {
                    return createScaleOutAction(metrics,
                            String.format("Operator %s backpressure %.2f%% exceeds threshold %.2f%%",
                                    bottleneck, opMetrics.getBackpressure() * 100,
                                    backpressureThreshold * 100));
                }
            }
        }

        return null;
    }

    private ScalingAction checkScaleInConditions(SystemMetrics metrics) {
        SystemMetrics.JobMetrics jobMetrics = metrics.getJobMetrics();

        // Check if resources are underutilized
        double cpuThreshold = configuration.getDouble(
                PolicyConfiguration.CPU_SCALE_IN_THRESHOLD, 0.3);
        double memThreshold = configuration.getDouble(
                PolicyConfiguration.MEMORY_SCALE_IN_THRESHOLD, 0.4);

        long totalMemory = Runtime.getRuntime().maxMemory();
        double memUsage = (double) jobMetrics.getTotalMemoryUsage() / totalMemory;

        if (jobMetrics.getCpuUtilization() < cpuThreshold && memUsage < memThreshold) {
            return createScaleInAction(metrics,
                    String.format("Resources underutilized: CPU %.2f%%, Memory %.2f%%",
                            jobMetrics.getCpuUtilization() * 100, memUsage * 100));
        }

        return null;
    }

    private ScalingAction checkRebalanceConditions(SystemMetrics metrics) {
        // Check for load imbalance across operators
        for (SystemMetrics.OperatorMetrics opMetrics : metrics.getOperatorMetrics().values()) {
            double imbalance = calculateLoadImbalance(opMetrics);
            if (imbalance > 0.3) { // 30% imbalance threshold
                return createRebalanceAction(metrics, opMetrics,
                        String.format("Load imbalance %.2f%% detected in operator %s",
                                imbalance * 100, opMetrics.getOperatorId()));
            }
        }

        return null;
    }

    private double calculateLoadImbalance(SystemMetrics.OperatorMetrics opMetrics) {
        if (opMetrics.getPartitionMetrics().isEmpty()) {
            return 0.0;
        }

        // Calculate average load per subtask
        Map<Integer, Double> subtaskLoads = new HashMap<>();
        for (SystemMetrics.PartitionMetrics partition : opMetrics.getPartitionMetrics().values()) {
            int subtask = partition.getCurrentSubtask();
            double load = partition.getInputRate() * partition.getProcessingTime();
            subtaskLoads.merge(subtask, load, Double::sum);
        }

        if (subtaskLoads.size() <= 1) {
            return 0.0;
        }

        // Calculate imbalance as (max - min) / average
        double maxLoad = Collections.max(subtaskLoads.values());
        double minLoad = Collections.min(subtaskLoads.values());
        double avgLoad = subtaskLoads.values().stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);

        return avgLoad > 0 ? (maxLoad - minLoad) / avgLoad : 0.0;
    }

    private ScalingAction createScaleOutAction(SystemMetrics metrics, String reason) {
        SystemMetrics.JobMetrics jobMetrics = metrics.getJobMetrics();
        int currentParallelism = jobMetrics.getTotalParallelism();
        double scaleFactor = configuration.getDouble(
                PolicyConfiguration.SCALE_OUT_FACTOR, 1.5);
        int maxParallelism = configuration.getInt(
                PolicyConfiguration.MAX_PARALLELISM, 128);

        int targetParallelism = Math.min(
                (int) Math.ceil(currentParallelism * scaleFactor),
                maxParallelism);

        if (targetParallelism == currentParallelism) {
            LOG.warn("Cannot scale out further, already at max parallelism");
            return null;
        }

        // Find bottleneck operator for targeted scaling
        OperatorID bottleneck = metrics.findBottleneck();
        if (bottleneck == null) {
            bottleneck = metrics.getOperatorMetrics().keySet().iterator().next();
        }

        List<MigrationPlan> migrationPlans = generateMigrationPlans(
                metrics.getOperatorMetrics(bottleneck),
                currentParallelism,
                targetParallelism);

        lastScalingTime = System.currentTimeMillis();
        return new ScalingAction(
                ScalingDecision.SCALE_OUT,
                targetParallelism,
                migrationPlans,
                reason);
    }

    private ScalingAction createScaleInAction(SystemMetrics metrics, String reason) {
        SystemMetrics.JobMetrics jobMetrics = metrics.getJobMetrics();
        int currentParallelism = jobMetrics.getTotalParallelism();
        double scaleFactor = configuration.getDouble(
                PolicyConfiguration.SCALE_IN_FACTOR, 0.75);
        int minParallelism = configuration.getInt(
                PolicyConfiguration.MIN_PARALLELISM, 1);

        int targetParallelism = Math.max(
                (int) Math.floor(currentParallelism * scaleFactor),
                minParallelism);

        if (targetParallelism == currentParallelism) {
            LOG.warn("Cannot scale in further, already at min parallelism");
            return null;
        }

        // Select operator with lowest load for scale-in
        SystemMetrics.OperatorMetrics targetOperator = null;
        double minLoad = Double.MAX_VALUE;

        for (SystemMetrics.OperatorMetrics opMetrics : metrics.getOperatorMetrics().values()) {
            double load = opMetrics.getCpuUsage() * opMetrics.getInputRate();
            if (load < minLoad) {
                minLoad = load;
                targetOperator = opMetrics;
            }
        }

        if (targetOperator == null) {
            return null;
        }

        List<MigrationPlan> migrationPlans = generateMigrationPlans(
                targetOperator,
                currentParallelism,
                targetParallelism);

        lastScalingTime = System.currentTimeMillis();
        return new ScalingAction(
                ScalingDecision.SCALE_IN,
                targetParallelism,
                migrationPlans,
                reason);
    }

    private ScalingAction createRebalanceAction(
            SystemMetrics metrics,
            SystemMetrics.OperatorMetrics opMetrics,
            String reason) {

        int currentParallelism = opMetrics.getParallelism();
        List<MigrationPlan> migrationPlans = generateRebalancePlans(opMetrics);

        if (migrationPlans.isEmpty()) {
            return null;
        }

        lastScalingTime = System.currentTimeMillis();
        return new ScalingAction(
                ScalingDecision.REBALANCE,
                currentParallelism,
                migrationPlans,
                reason);
    }

    private List<MigrationPlan> generateMigrationPlans(
            SystemMetrics.OperatorMetrics opMetrics,
            int currentParallelism,
            int targetParallelism) {

        List<MigrationPlan> plans = new ArrayList<>();
        if (opMetrics == null) {
            return plans;
        }

        // Update consistent hash router with new parallelism
        router.updateParallelism(targetParallelism);

        // Generate migration plans for affected partitions
        for (SystemMetrics.PartitionMetrics partition : opMetrics.getPartitionMetrics().values()) {
            int oldSubtask = partition.getCurrentSubtask();
            int newSubtask = router.getTargetSubtask(partition.getPartitionId());

            if (oldSubtask != newSubtask) {
                MigrationPlan.OperatorInstance source = new MigrationPlan.OperatorInstance(
                        new ExecutionAttemptID(),
                        oldSubtask,
                        "tm-" + oldSubtask);

                MigrationPlan.OperatorInstance target = new MigrationPlan.OperatorInstance(
                        new ExecutionAttemptID(),
                        newSubtask,
                        "tm-" + newSubtask);

                MigrationPlan plan = new MigrationPlan(
                        partition.getPartitionId(),
                        opMetrics.getOperatorId(),
                        source,
                        target,
                        jobId,
                        partition.getStateSize());

                plans.add(plan);
            }
        }

        return plans;
    }

    private List<MigrationPlan> generateRebalancePlans(SystemMetrics.OperatorMetrics opMetrics) {
        List<MigrationPlan> plans = new ArrayList<>();

        // Calculate ideal distribution
        int parallelism = opMetrics.getParallelism();
        int totalPartitions = opMetrics.getPartitionMetrics().size();
        int partitionsPerSubtask = totalPartitions / parallelism;

        // Count partitions per subtask
        Map<Integer, List<SystemMetrics.PartitionMetrics>> subtaskPartitions = new HashMap<>();
        for (SystemMetrics.PartitionMetrics partition : opMetrics.getPartitionMetrics().values()) {
            subtaskPartitions.computeIfAbsent(partition.getCurrentSubtask(), k -> new ArrayList<>())
                    .add(partition);
        }

        // Move partitions from overloaded to underloaded subtasks
        for (int subtask = 0; subtask < parallelism; subtask++) {
            List<SystemMetrics.PartitionMetrics> partitions = subtaskPartitions.getOrDefault(
                    subtask, new ArrayList<>());

            if (partitions.size() > partitionsPerSubtask + 1) {
                // Overloaded subtask - move excess partitions
                int excess = partitions.size() - partitionsPerSubtask;
                for (int i = 0; i < excess && i < partitions.size(); i++) {
                    SystemMetrics.PartitionMetrics partition = partitions.get(i);
                    int targetSubtask = findUnderloadedSubtask(subtaskPartitions, partitionsPerSubtask);

                    if (targetSubtask >= 0 && targetSubtask != subtask) {
                        MigrationPlan.OperatorInstance source = new MigrationPlan.OperatorInstance(
                                new ExecutionAttemptID(),
                                subtask,
                                "tm-" + subtask);

                        MigrationPlan.OperatorInstance target = new MigrationPlan.OperatorInstance(
                                new ExecutionAttemptID(),
                                targetSubtask,
                                "tm-" + targetSubtask);

                        MigrationPlan plan = new MigrationPlan(
                                partition.getPartitionId(),
                                opMetrics.getOperatorId(),
                                source,
                                target,
                                jobId,
                                partition.getStateSize());

                        plans.add(plan);

                        // Update tracking
                        subtaskPartitions.computeIfAbsent(targetSubtask, k -> new ArrayList<>())
                                .add(partition);
                    }
                }
            }
        }

        return plans;
    }

    private int findUnderloadedSubtask(
            Map<Integer, List<SystemMetrics.PartitionMetrics>> subtaskPartitions,
            int targetLoad) {

        int minLoad = Integer.MAX_VALUE;
        int targetSubtask = -1;

        for (Map.Entry<Integer, List<SystemMetrics.PartitionMetrics>> entry :
                subtaskPartitions.entrySet()) {
            if (entry.getValue().size() < targetLoad && entry.getValue().size() < minLoad) {
                minLoad = entry.getValue().size();
                targetSubtask = entry.getKey();
            }
        }

        return targetSubtask;
    }

    @Override
    public void updateConfiguration(PolicyConfiguration config) {
        this.configuration = config.copy();
    }

    @Override
    public PolicyConfiguration getConfiguration() {
        return configuration.copy();
    }

    @Override
    public void reset() {
        lastScalingTime = 0;
    }

    /**
     * Consistent hash router for partition assignment.
     */
    private static class ConsistentHashRouter {
        private int parallelism;

        public void updateParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public int getTargetSubtask(int partitionId) {
            // Simple modulo for now, can be replaced with consistent hashing
            return partitionId % parallelism;
        }
    }
}