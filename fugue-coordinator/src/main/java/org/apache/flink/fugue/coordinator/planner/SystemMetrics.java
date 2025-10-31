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

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * System metrics used for elasticity decisions.
 */
public class SystemMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Operator-level metrics. */
    private final Map<OperatorID, OperatorMetrics> operatorMetrics;

    /** Global job metrics. */
    private final JobMetrics jobMetrics;

    /** Timestamp when metrics were collected. */
    private final long timestamp;

    public SystemMetrics() {
        this.operatorMetrics = new HashMap<>();
        this.jobMetrics = new JobMetrics();
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Metrics for a single operator.
     */
    public static class OperatorMetrics implements Serializable {
        private static final long serialVersionUID = 1L;

        private final OperatorID operatorId;
        private double cpuUsage;           // 0.0 to 1.0
        private long memoryUsage;          // bytes
        private long stateSize;            // bytes
        private double inputRate;          // records/sec
        private double outputRate;         // records/sec
        private double processingLatency;  // milliseconds
        private double backpressure;       // 0.0 to 1.0
        private int parallelism;
        private Map<Integer, PartitionMetrics> partitionMetrics;

        public OperatorMetrics(OperatorID operatorId) {
            this.operatorId = operatorId;
            this.partitionMetrics = new HashMap<>();
        }

        // Getters and setters
        public OperatorID getOperatorId() {
            return operatorId;
        }

        public double getCpuUsage() {
            return cpuUsage;
        }

        public void setCpuUsage(double cpuUsage) {
            this.cpuUsage = cpuUsage;
        }

        public long getMemoryUsage() {
            return memoryUsage;
        }

        public void setMemoryUsage(long memoryUsage) {
            this.memoryUsage = memoryUsage;
        }

        public long getStateSize() {
            return stateSize;
        }

        public void setStateSize(long stateSize) {
            this.stateSize = stateSize;
        }

        public double getInputRate() {
            return inputRate;
        }

        public void setInputRate(double inputRate) {
            this.inputRate = inputRate;
        }

        public double getOutputRate() {
            return outputRate;
        }

        public void setOutputRate(double outputRate) {
            this.outputRate = outputRate;
        }

        public double getProcessingLatency() {
            return processingLatency;
        }

        public void setProcessingLatency(double processingLatency) {
            this.processingLatency = processingLatency;
        }

        public double getBackpressure() {
            return backpressure;
        }

        public void setBackpressure(double backpressure) {
            this.backpressure = backpressure;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public Map<Integer, PartitionMetrics> getPartitionMetrics() {
            return partitionMetrics;
        }

        public void setPartitionMetrics(Map<Integer, PartitionMetrics> partitionMetrics) {
            this.partitionMetrics = partitionMetrics;
        }
    }

    /**
     * Metrics for a single partition.
     */
    public static class PartitionMetrics implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int partitionId;
        private long stateSize;
        private double inputRate;
        private double processingTime;
        private int currentSubtask;

        public PartitionMetrics(int partitionId) {
            this.partitionId = partitionId;
        }

        // Getters and setters
        public int getPartitionId() {
            return partitionId;
        }

        public long getStateSize() {
            return stateSize;
        }

        public void setStateSize(long stateSize) {
            this.stateSize = stateSize;
        }

        public double getInputRate() {
            return inputRate;
        }

        public void setInputRate(double inputRate) {
            this.inputRate = inputRate;
        }

        public double getProcessingTime() {
            return processingTime;
        }

        public void setProcessingTime(double processingTime) {
            this.processingTime = processingTime;
        }

        public int getCurrentSubtask() {
            return currentSubtask;
        }

        public void setCurrentSubtask(int currentSubtask) {
            this.currentSubtask = currentSubtask;
        }
    }

    /**
     * Job-level metrics.
     */
    public static class JobMetrics implements Serializable {
        private static final long serialVersionUID = 1L;

        private double averageLatency;         // milliseconds
        private double p99Latency;             // milliseconds
        private double totalThroughput;        // records/sec
        private double cpuUtilization;         // 0.0 to 1.0
        private long totalMemoryUsage;         // bytes
        private int totalParallelism;
        private int availableTaskSlots;
        private boolean isBackpressured;
        private long checkpointDuration;       // milliseconds
        private long lastCheckpointSize;       // bytes

        // Getters and setters
        public double getAverageLatency() {
            return averageLatency;
        }

        public void setAverageLatency(double averageLatency) {
            this.averageLatency = averageLatency;
        }

        public double getP99Latency() {
            return p99Latency;
        }

        public void setP99Latency(double p99Latency) {
            this.p99Latency = p99Latency;
        }

        public double getTotalThroughput() {
            return totalThroughput;
        }

        public void setTotalThroughput(double totalThroughput) {
            this.totalThroughput = totalThroughput;
        }

        public double getCpuUtilization() {
            return cpuUtilization;
        }

        public void setCpuUtilization(double cpuUtilization) {
            this.cpuUtilization = cpuUtilization;
        }

        public long getTotalMemoryUsage() {
            return totalMemoryUsage;
        }

        public void setTotalMemoryUsage(long totalMemoryUsage) {
            this.totalMemoryUsage = totalMemoryUsage;
        }

        public int getTotalParallelism() {
            return totalParallelism;
        }

        public void setTotalParallelism(int totalParallelism) {
            this.totalParallelism = totalParallelism;
        }

        public int getAvailableTaskSlots() {
            return availableTaskSlots;
        }

        public void setAvailableTaskSlots(int availableTaskSlots) {
            this.availableTaskSlots = availableTaskSlots;
        }

        public boolean isBackpressured() {
            return isBackpressured;
        }

        public void setBackpressured(boolean backpressured) {
            isBackpressured = backpressured;
        }

        public long getCheckpointDuration() {
            return checkpointDuration;
        }

        public void setCheckpointDuration(long checkpointDuration) {
            this.checkpointDuration = checkpointDuration;
        }

        public long getLastCheckpointSize() {
            return lastCheckpointSize;
        }

        public void setLastCheckpointSize(long lastCheckpointSize) {
            this.lastCheckpointSize = lastCheckpointSize;
        }
    }

    // Main class methods
    public Map<OperatorID, OperatorMetrics> getOperatorMetrics() {
        return operatorMetrics;
    }

    public void addOperatorMetrics(OperatorID operatorId, OperatorMetrics metrics) {
        operatorMetrics.put(operatorId, metrics);
    }

    public OperatorMetrics getOperatorMetrics(OperatorID operatorId) {
        return operatorMetrics.get(operatorId);
    }

    public JobMetrics getJobMetrics() {
        return jobMetrics;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Find the bottleneck operator based on metrics.
     */
    public OperatorID findBottleneck() {
        OperatorID bottleneck = null;
        double maxBackpressure = 0.0;

        for (Map.Entry<OperatorID, OperatorMetrics> entry : operatorMetrics.entrySet()) {
            if (entry.getValue().getBackpressure() > maxBackpressure) {
                maxBackpressure = entry.getValue().getBackpressure();
                bottleneck = entry.getKey();
            }
        }

        return bottleneck;
    }

    @Override
    public String toString() {
        return String.format("SystemMetrics{operators=%d, throughput=%.2f rec/s, latency=%.2fms, cpu=%.2f}",
                operatorMetrics.size(),
                jobMetrics.getTotalThroughput(),
                jobMetrics.getAverageLatency(),
                jobMetrics.getCpuUtilization());
    }
}