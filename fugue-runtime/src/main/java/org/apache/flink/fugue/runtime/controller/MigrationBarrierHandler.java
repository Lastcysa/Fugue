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

package org.apache.flink.fugue.runtime.controller;

import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.runtime.routing.RoutingTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles migration barrier alignment in the dataflow.
 * Coordinates barrier propagation and routing table updates.
 */
public class MigrationBarrierHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationBarrierHandler.class);

    /** Number of input channels. */
    private final int numberOfChannels;

    /** Routing table manager for updating routes. */
    private final RoutingTableManager routingTableManager;

    /** Migration controller for notifying about barrier arrival. */
    private final MigrationController migrationController;

    /** Barrier alignment state per migration. */
    private final Map<Long, BarrierAlignmentState> alignmentStates;

    public MigrationBarrierHandler(
            int numberOfChannels,
            RoutingTableManager routingTableManager,
            MigrationController migrationController) {
        this.numberOfChannels = numberOfChannels;
        this.routingTableManager = routingTableManager;
        this.migrationController = migrationController;
        this.alignmentStates = new ConcurrentHashMap<>();
    }

    /**
     * Handle incoming migration barrier from an input channel.
     */
    public void processMigrationBarrier(MigrationBarrier barrier, int channelIndex) {
        long migrationId = barrier.getMigrationId();

        LOG.debug("Received migration barrier {} from channel {}", migrationId, channelIndex);

        // Get or create alignment state
        BarrierAlignmentState state = alignmentStates.computeIfAbsent(
                migrationId,
                id -> new BarrierAlignmentState(id, numberOfChannels));

        // Mark channel as received
        boolean aligned = state.markChannelReceived(channelIndex);

        if (aligned) {
            // All input channels have received the barrier - alignment complete
            handleBarrierAligned(barrier);
        } else {
            // Block this channel until alignment
            state.blockChannel(channelIndex);
            LOG.debug("Blocking channel {} for migration {} until alignment ({}/ channels)",
                    channelIndex, migrationId, state.getReceivedCount(), numberOfChannels);
        }
    }

    /**
     * Handle barrier alignment completion.
     */
    private void handleBarrierAligned(MigrationBarrier barrier) {
        long migrationId = barrier.getMigrationId();

        LOG.info("Migration barrier {} aligned across all {} channels",
                migrationId, numberOfChannels);

        BarrierAlignmentState state = alignmentStates.get(migrationId);

        try {
            // 1. Atomically update routing tables
            if (!barrier.getMigrationPlans().isEmpty()) {
                routingTableManager.applyBarrierUpdates(barrier);
                LOG.info("Routing tables updated for migration {}", migrationId);
            }

            // 2. Notify migration controller
            migrationController.handleBarrierArrival(barrier);

            // 3. Unblock all channels
            state.unblockAllChannels();
            LOG.info("All channels unblocked for migration {}", migrationId);

            // 4. Propagate barrier downstream
            propagateBarrier(barrier);

        } finally {
            // Cleanup alignment state
            alignmentStates.remove(migrationId);
        }
    }

    /**
     * Propagate barrier to downstream operators.
     */
    private void propagateBarrier(MigrationBarrier barrier) {
        // In real implementation, this would emit the barrier to output channels
        LOG.debug("Propagating migration barrier {} downstream", barrier.getMigrationId());
    }

    /**
     * Check if a channel is blocked for a migration.
     */
    public boolean isChannelBlocked(int channelIndex, long migrationId) {
        BarrierAlignmentState state = alignmentStates.get(migrationId);
        return state != null && state.isChannelBlocked(channelIndex);
    }

    /**
     * Reset handler state (e.g., on checkpoint restore).
     */
    public void reset() {
        LOG.info("Resetting migration barrier handler");

        for (BarrierAlignmentState state : alignmentStates.values()) {
            state.unblockAllChannels();
        }

        alignmentStates.clear();
    }

    /**
     * State for tracking barrier alignment across input channels.
     */
    private static class BarrierAlignmentState {
        private final long migrationId;
        private final int numberOfChannels;
        private final boolean[] channelReceived;
        private final boolean[] channelBlocked;
        private final AtomicInteger receivedCount;
        private final long startTime;

        public BarrierAlignmentState(long migrationId, int numberOfChannels) {
            this.migrationId = migrationId;
            this.numberOfChannels = numberOfChannels;
            this.channelReceived = new boolean[numberOfChannels];
            this.channelBlocked = new boolean[numberOfChannels];
            this.receivedCount = new AtomicInteger(0);
            this.startTime = System.currentTimeMillis();
        }

        /**
         * Mark a channel as having received the barrier.
         * Returns true if all channels have now received it (aligned).
         */
        public synchronized boolean markChannelReceived(int channelIndex) {
            if (channelIndex < 0 || channelIndex >= numberOfChannels) {
                throw new IllegalArgumentException("Invalid channel index: " + channelIndex);
            }

            if (!channelReceived[channelIndex]) {
                channelReceived[channelIndex] = true;
                int count = receivedCount.incrementAndGet();

                if (count == numberOfChannels) {
                    long duration = System.currentTimeMillis() - startTime;
                    LOG.info("Barrier alignment completed for migration {} in {}ms",
                            migrationId, duration);
                    return true;
                }
            }

            return false;
        }

        /**
         * Block a channel (stop processing records from this channel).
         */
        public synchronized void blockChannel(int channelIndex) {
            channelBlocked[channelIndex] = true;
        }

        /**
         * Unblock all channels.
         */
        public synchronized void unblockAllChannels() {
            for (int i = 0; i < numberOfChannels; i++) {
                channelBlocked[i] = false;
            }
        }

        /**
         * Check if a channel is blocked.
         */
        public synchronized boolean isChannelBlocked(int channelIndex) {
            return channelBlocked[channelIndex];
        }

        /**
         * Get number of channels that have received the barrier.
         */
        public int getReceivedCount() {
            return receivedCount.get();
        }

        /**
         * Check if aligned.
         */
        public boolean isAligned() {
            return receivedCount.get() == numberOfChannels;
        }

        /**
         * Get alignment duration so far.
         */
        public long getAlignmentDuration() {
            return System.currentTimeMillis() - startTime;
        }
    }
}