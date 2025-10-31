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

package org.apache.flink.fugue.common.core;

import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Migration barrier that flows in-band with data records through the dataflow graph.
 * Extends Flink's CheckpointBarrier to leverage existing barrier alignment mechanisms.
 */
public class MigrationBarrier extends CheckpointBarrier {
    private static final long serialVersionUID = 1L;

    /** List of migration plans carried by this barrier. */
    private List<MigrationPlan> migrationPlans;

    /** Unique identifier for this migration round. */
    private final long migrationId;

    /** Whether this is the final barrier for the migration. */
    private final boolean isFinalBarrier;

    public MigrationBarrier(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            long migrationId,
            List<MigrationPlan> migrationPlans,
            boolean isFinalBarrier) {
        super(checkpointId, timestamp, checkpointOptions);
        this.migrationId = migrationId;
        this.migrationPlans = new ArrayList<>(migrationPlans);
        this.isFinalBarrier = isFinalBarrier;
    }

    /**
     * Creates a migration barrier without checkpoint coordination.
     */
    public static MigrationBarrier createStandalone(
            long migrationId,
            List<MigrationPlan> migrationPlans) {
        // Use a special checkpoint ID to indicate this is a migration-only barrier
        long checkpointId = -migrationId;
        return new MigrationBarrier(
                checkpointId,
                System.currentTimeMillis(),
                CheckpointOptions.forCheckpointWithDefaultLocation(),
                migrationId,
                migrationPlans,
                false);
    }

    /**
     * Creates a final migration barrier that signals completion.
     */
    public static MigrationBarrier createFinalBarrier(long migrationId) {
        return new MigrationBarrier(
                -migrationId,
                System.currentTimeMillis(),
                CheckpointOptions.forCheckpointWithDefaultLocation(),
                migrationId,
                Collections.emptyList(),
                true);
    }

    public List<MigrationPlan> getMigrationPlans() {
        return Collections.unmodifiableList(migrationPlans);
    }

    public long getMigrationId() {
        return migrationId;
    }

    public boolean isFinalBarrier() {
        return isFinalBarrier;
    }

    /**
     * Check if this barrier affects a specific partition.
     */
    public boolean affectsPartition(int partitionId) {
        return migrationPlans.stream()
                .anyMatch(plan -> plan.getPartitionId() == partitionId);
    }

    /**
     * Get the migration plan for a specific partition, if any.
     */
    public MigrationPlan getMigrationPlanForPartition(int partitionId) {
        return migrationPlans.stream()
                .filter(plan -> plan.getPartitionId() == partitionId)
                .findFirst()
                .orElse(null);
    }

    /**
     * Check if this is a pure migration barrier (not associated with checkpointing).
     */
    public boolean isPureMigrationBarrier() {
        return getId() < 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MigrationBarrier that = (MigrationBarrier) o;
        return migrationId == that.migrationId &&
                isFinalBarrier == that.isFinalBarrier &&
                Objects.equals(migrationPlans, that.migrationPlans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), migrationId, migrationPlans, isFinalBarrier);
    }

    @Override
    public String toString() {
        return String.format("MigrationBarrier{migrationId=%d, checkpointId=%d, plans=%d, final=%b}",
                migrationId, getId(), migrationPlans.size(), isFinalBarrier);
    }

    /**
     * Custom serialization to handle the migration plans list.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(migrationPlans.size());
        for (MigrationPlan plan : migrationPlans) {
            out.writeObject(plan);
        }
    }

    /**
     * Custom deserialization to handle the migration plans list.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int size = in.readInt();
        migrationPlans = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            migrationPlans.add((MigrationPlan) in.readObject());
        }
    }
}