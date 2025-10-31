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

package org.apache.flink.fugue.common.rpc;

import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.fugue.common.state.DeltaLog;

import java.io.Serializable;

/**
 * RPC messages for migration coordination between JobManager and TaskManagers.
 */
public class MigrationMessages {

    /**
     * Base class for all migration RPC messages.
     */
    public abstract static class MigrationMessage implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final long migrationId;
        protected final long timestamp;

        public MigrationMessage(long migrationId) {
            this.migrationId = migrationId;
            this.timestamp = System.currentTimeMillis();
        }

        public long getMigrationId() {
            return migrationId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Request to prepare source operator for migration.
     */
    public static class PrepareMigrationSource extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final MigrationPlan migrationPlan;
        private final String targetAddress;
        private final int targetPort;

        public PrepareMigrationSource(
                long migrationId,
                MigrationPlan migrationPlan,
                String targetAddress,
                int targetPort) {
            super(migrationId);
            this.migrationPlan = migrationPlan;
            this.targetAddress = targetAddress;
            this.targetPort = targetPort;
        }

        public MigrationPlan getMigrationPlan() {
            return migrationPlan;
        }

        public String getTargetAddress() {
            return targetAddress;
        }

        public int getTargetPort() {
            return targetPort;
        }
    }

    /**
     * Request to prepare target operator for migration.
     */
    public static class PrepareMigrationTarget extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final MigrationPlan migrationPlan;
        private final int listenPort;

        public PrepareMigrationTarget(
                long migrationId,
                MigrationPlan migrationPlan,
                int listenPort) {
            super(migrationId);
            this.migrationPlan = migrationPlan;
            this.listenPort = listenPort;
        }

        public MigrationPlan getMigrationPlan() {
            return migrationPlan;
        }

        public int getListenPort() {
            return listenPort;
        }
    }

    /**
     * Request to start pre-copy phase.
     */
    public static class StartPreCopy extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;
        private final long rateLimitBytesPerSec;

        public StartPreCopy(long migrationId, int partitionId, long rateLimitBytesPerSec) {
            super(migrationId);
            this.partitionId = partitionId;
            this.rateLimitBytesPerSec = rateLimitBytesPerSec;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public long getRateLimitBytesPerSec() {
            return rateLimitBytesPerSec;
        }
    }

    /**
     * Notification that pre-copy round completed.
     */
    public static class PreCopyRoundComplete extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int roundNumber;
        private final long deltaSize;
        private final long bytesTransferred;
        private final boolean converged;

        public PreCopyRoundComplete(
                long migrationId,
                int roundNumber,
                long deltaSize,
                long bytesTransferred,
                boolean converged) {
            super(migrationId);
            this.roundNumber = roundNumber;
            this.deltaSize = deltaSize;
            this.bytesTransferred = bytesTransferred;
            this.converged = converged;
        }

        public int getRoundNumber() {
            return roundNumber;
        }

        public long getDeltaSize() {
            return deltaSize;
        }

        public long getBytesTransferred() {
            return bytesTransferred;
        }

        public boolean isConverged() {
            return converged;
        }
    }

    /**
     * Request to transfer final delta after barrier alignment.
     */
    public static class TransferFinalDelta extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;

        public TransferFinalDelta(long migrationId, int partitionId) {
            super(migrationId);
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }
    }

    /**
     * Notification that final delta transfer completed.
     */
    public static class FinalDeltaComplete extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;
        private final long finalDeltaSize;

        public FinalDeltaComplete(long migrationId, int partitionId, long finalDeltaSize) {
            super(migrationId);
            this.partitionId = partitionId;
            this.finalDeltaSize = finalDeltaSize;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public long getFinalDeltaSize() {
            return finalDeltaSize;
        }
    }

    /**
     * Request to abort migration.
     */
    public static class AbortMigration extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final String reason;

        public AbortMigration(long migrationId, String reason) {
            super(migrationId);
            this.reason = reason;
        }

        public String getReason() {
            return reason;
        }
    }

    /**
     * Request to commit migration.
     */
    public static class CommitMigration extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;

        public CommitMigration(long migrationId, int partitionId) {
            super(migrationId);
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }
    }

    /**
     * State update notification.
     */
    public static class StateUpdate extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final MigrationState newState;
        private final String details;

        public StateUpdate(long migrationId, MigrationState newState, String details) {
            super(migrationId);
            this.newState = newState;
            this.details = details;
        }

        public MigrationState getNewState() {
            return newState;
        }

        public String getDetails() {
            return details;
        }
    }

    /**
     * Response message for RPC calls.
     */
    public static class MigrationResponse extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final boolean success;
        private final String message;

        public MigrationResponse(long migrationId, boolean success, String message) {
            super(migrationId);
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * Update routing table request.
     */
    public static class UpdateRoutingTable extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;
        private final int newSubtaskIndex;

        public UpdateRoutingTable(long migrationId, int partitionId, int newSubtaskIndex) {
            super(migrationId);
            this.partitionId = partitionId;
            this.newSubtaskIndex = newSubtaskIndex;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public int getNewSubtaskIndex() {
            return newSubtaskIndex;
        }
    }

    /**
     * Migration statistics report.
     */
    public static class MigrationStats extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final long duration;
        private final long bytesTransferred;
        private final int preCopyRounds;
        private final double throughputRetention;

        public MigrationStats(
                long migrationId,
                long duration,
                long bytesTransferred,
                int preCopyRounds,
                double throughputRetention) {
            super(migrationId);
            this.duration = duration;
            this.bytesTransferred = bytesTransferred;
            this.preCopyRounds = preCopyRounds;
            this.throughputRetention = throughputRetention;
        }

        public long getDuration() {
            return duration;
        }

        public long getBytesTransferred() {
            return bytesTransferred;
        }

        public int getPreCopyRounds() {
            return preCopyRounds;
        }

        public double getThroughputRetention() {
            return throughputRetention;
        }
    }

    // Private constructor to prevent instantiation
    private MigrationMessages() {}
}