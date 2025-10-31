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

package org.apache.flink.fugue.runtime.routing;

import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages partition-to-subtask routing tables for operators.
 * Provides atomic updates during migration barrier alignment.
 */
public class RoutingTableManager {
    private static final Logger LOG = LoggerFactory.getLogger(RoutingTableManager.class);

    /** Routing tables per operator. */
    private final Map<OperatorID, PartitionRoutingTable> operatorTables;

    /** Global version counter for routing updates. */
    private final AtomicInteger versionCounter;

    public RoutingTableManager() {
        this.operatorTables = new ConcurrentHashMap<>();
        this.versionCounter = new AtomicInteger(0);
    }

    /**
     * Initialize routing table for an operator.
     */
    public void initializeOperator(OperatorID operatorId, int parallelism, int numPartitions) {
        LOG.info("Initializing routing table for operator {}: parallelism={}, partitions={}",
                operatorId, parallelism, numPartitions);

        PartitionRoutingTable table = new PartitionRoutingTable(
                operatorId,
                parallelism,
                numPartitions);

        operatorTables.put(operatorId, table);

        LOG.info("Routing table initialized for operator {}", operatorId);
    }

    /**
     * Get target subtask for a partition.
     */
    public int getTargetSubtask(OperatorID operatorId, int partitionId) {
        PartitionRoutingTable table = operatorTables.get(operatorId);
        if (table == null) {
            throw new IllegalStateException("No routing table for operator " + operatorId);
        }

        return table.getTargetSubtask(partitionId);
    }

    /**
     * Get target subtask for a key (using key-group assignment).
     */
    public int getTargetSubtaskForKey(OperatorID operatorId, Object key, int maxParallelism) {
        int keyGroup = KeyGroupAssigner.assignKeyGroup(key, maxParallelism);
        return getTargetSubtask(operatorId, keyGroup);
    }

    /**
     * Apply migration barrier updates atomically.
     * Updates routing for all affected partitions.
     */
    public void applyBarrierUpdates(MigrationBarrier barrier) {
        LOG.info("Applying routing updates for migration barrier {}", barrier.getMigrationId());

        int newVersion = versionCounter.incrementAndGet();

        for (MigrationPlan plan : barrier.getMigrationPlans()) {
            OperatorID operatorId = plan.getOperatorId();
            int partitionId = plan.getPartitionId();
            int newSubtask = plan.getTargetInstance().getSubtaskIndex();

            PartitionRoutingTable table = operatorTables.get(operatorId);
            if (table == null) {
                LOG.warn("No routing table found for operator {}", operatorId);
                continue;
            }

            int oldSubtask = table.updateRoute(partitionId, newSubtask, newVersion);

            LOG.info("Updated routing for operator {}, partition {}: {} -> {} (version={})",
                    operatorId, partitionId, oldSubtask, newSubtask, newVersion);
        }

        LOG.info("Routing updates applied for migration barrier {} (version={})",
                barrier.getMigrationId(), newVersion);
    }

    /**
     * Get routing table for an operator.
     */
    public PartitionRoutingTable getRoutingTable(OperatorID operatorId) {
        return operatorTables.get(operatorId);
    }

    /**
     * Get current version.
     */
    public int getCurrentVersion() {
        return versionCounter.get();
    }

    /**
     * Shutdown manager.
     */
    public void shutdown() {
        LOG.info("Shutting down RoutingTableManager");
        operatorTables.clear();
    }

    /**
     * Routing table for a single operator.
     */
    public static class PartitionRoutingTable {
        private final OperatorID operatorId;
        private final int parallelism;
        private final int numPartitions;

        /** Current routing: partition -> subtask. */
        private final int[] partitionToSubtask;

        /** Version of each route. */
        private final int[] routeVersions;

        /** Lock for atomic updates. */
        private final ReadWriteLock lock;

        public PartitionRoutingTable(OperatorID operatorId, int parallelism, int numPartitions) {
            this.operatorId = operatorId;
            this.parallelism = parallelism;
            this.numPartitions = numPartitions;
            this.partitionToSubtask = new int[numPartitions];
            this.routeVersions = new int[numPartitions];
            this.lock = new ReentrantReadWriteLock();

            // Initialize with default routing (round-robin)
            for (int i = 0; i < numPartitions; i++) {
                partitionToSubtask[i] = i % parallelism;
                routeVersions[i] = 0;
            }
        }

        /**
         * Get target subtask for partition.
         */
        public int getTargetSubtask(int partitionId) {
            if (partitionId < 0 || partitionId >= numPartitions) {
                throw new IllegalArgumentException(
                        "Invalid partition ID: " + partitionId + " (max: " + numPartitions + ")");
            }

            lock.readLock().lock();
            try {
                return partitionToSubtask[partitionId];
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Update routing for a partition atomically.
         * Returns the previous subtask.
         */
        public int updateRoute(int partitionId, int newSubtask, int version) {
            if (partitionId < 0 || partitionId >= numPartitions) {
                throw new IllegalArgumentException(
                        "Invalid partition ID: " + partitionId + " (max: " + numPartitions + ")");
            }

            if (newSubtask < 0 || newSubtask >= parallelism) {
                throw new IllegalArgumentException(
                        "Invalid subtask index: " + newSubtask + " (max: " + parallelism + ")");
            }

            lock.writeLock().lock();
            try {
                int oldSubtask = partitionToSubtask[partitionId];
                partitionToSubtask[partitionId] = newSubtask;
                routeVersions[partitionId] = version;
                return oldSubtask;
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Get routing version for a partition.
         */
        public int getRouteVersion(int partitionId) {
            lock.readLock().lock();
            try {
                return routeVersions[partitionId];
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Get all routes as a snapshot.
         */
        public Map<Integer, Integer> getRoutesSnapshot() {
            lock.readLock().lock();
            try {
                Map<Integer, Integer> snapshot = new ConcurrentHashMap<>();
                for (int i = 0; i < numPartitions; i++) {
                    snapshot.put(i, partitionToSubtask[i]);
                }
                return snapshot;
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Get distribution statistics.
         */
        public DistributionStats getDistributionStats() {
            lock.readLock().lock();
            try {
                int[] countsPerSubtask = new int[parallelism];

                for (int i = 0; i < numPartitions; i++) {
                    int subtask = partitionToSubtask[i];
                    countsPerSubtask[subtask]++;
                }

                return new DistributionStats(operatorId, parallelism, countsPerSubtask);
            } finally {
                lock.readLock().unlock();
            }
        }

        public OperatorID getOperatorId() {
            return operatorId;
        }

        public int getParallelism() {
            return parallelism;
        }

        public int getNumPartitions() {
            return numPartitions;
        }
    }

    /**
     * Distribution statistics for a routing table.
     */
    public static class DistributionStats {
        private final OperatorID operatorId;
        private final int parallelism;
        private final int[] partitionsPerSubtask;

        public DistributionStats(OperatorID operatorId, int parallelism, int[] partitionsPerSubtask) {
            this.operatorId = operatorId;
            this.parallelism = parallelism;
            this.partitionsPerSubtask = partitionsPerSubtask;
        }

        public int getPartitionCount(int subtask) {
            return partitionsPerSubtask[subtask];
        }

        public int getMinPartitions() {
            int min = Integer.MAX_VALUE;
            for (int count : partitionsPerSubtask) {
                min = Math.min(min, count);
            }
            return min;
        }

        public int getMaxPartitions() {
            int max = 0;
            for (int count : partitionsPerSubtask) {
                max = Math.max(max, count);
            }
            return max;
        }

        public double getAveragePartitions() {
            int sum = 0;
            for (int count : partitionsPerSubtask) {
                sum += count;
            }
            return (double) sum / parallelism;
        }

        public double getImbalance() {
            double avg = getAveragePartitions();
            if (avg == 0) {
                return 0;
            }
            return (double) (getMaxPartitions() - getMinPartitions()) / avg;
        }

        @Override
        public String toString() {
            return String.format("DistributionStats{operator=%s, parallelism=%d, min=%d, max=%d, avg=%.2f, imbalance=%.2f}",
                    operatorId, parallelism, getMinPartitions(), getMaxPartitions(),
                    getAveragePartitions(), getImbalance());
        }
    }

    /**
     * Key group assigner for consistent hashing.
     */
    public static class KeyGroupAssigner {

        /**
         * Assign a key to a key group using MurmurHash.
         */
        public static int assignKeyGroup(Object key, int maxParallelism) {
            int hash = murmurHash(key.hashCode());
            return Math.abs(hash % maxParallelism);
        }

        /**
         * MurmurHash3 32-bit variant.
         */
        private static int murmurHash(int input) {
            int k = input;

            k *= 0xcc9e2d51;
            k = Integer.rotateLeft(k, 15);
            k *= 0x1b873593;

            int h = 0;
            h ^= k;
            h = Integer.rotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;

            // Finalization
            h ^= 4;
            h ^= (h >>> 16);
            h *= 0x85ebca6b;
            h ^= (h >>> 13);
            h *= 0xc2b2ae35;
            h ^= (h >>> 16);

            return h;
        }
    }
}