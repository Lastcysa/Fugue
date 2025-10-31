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

package org.apache.flink.fugue.integration.serialization;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for MigrationBarrier to integrate with Flink's network stack.
 */
public class MigrationBarrierSerializer extends EventSerializer {

    /** Event type ID for migration barrier. */
    public static final int MIGRATION_BARRIER_EVENT = 100;

    /**
     * Serialize migration barrier.
     */
    public static void serializeMigrationBarrier(
            MigrationBarrier barrier,
            DataOutputView target) throws IOException {

        // Write event type
        target.writeInt(MIGRATION_BARRIER_EVENT);

        // Write checkpoint info
        target.writeLong(barrier.getId());
        target.writeLong(barrier.getTimestamp());

        // Write migration-specific info
        target.writeLong(barrier.getMigrationId());
        target.writeBoolean(barrier.isFinalBarrier());

        // Write migration plans
        List<MigrationPlan> plans = barrier.getMigrationPlans();
        target.writeInt(plans.size());

        for (MigrationPlan plan : plans) {
            serializeMigrationPlan(plan, target);
        }
    }

    /**
     * Deserialize migration barrier.
     */
    public static MigrationBarrier deserializeMigrationBarrier(
            DataInputView source) throws IOException {

        // Read checkpoint info
        long checkpointId = source.readLong();
        long timestamp = source.readLong();

        // Read migration-specific info
        long migrationId = source.readLong();
        boolean isFinalBarrier = source.readBoolean();

        // Read migration plans
        int numPlans = source.readInt();
        List<MigrationPlan> plans = new ArrayList<>(numPlans);

        for (int i = 0; i < numPlans; i++) {
            plans.add(deserializeMigrationPlan(source));
        }

        // Create barrier
        return new MigrationBarrier(
                checkpointId,
                timestamp,
                CheckpointOptions.forCheckpointWithDefaultLocation(),
                migrationId,
                plans,
                isFinalBarrier);
    }

    /**
     * Serialize migration plan.
     */
    private static void serializeMigrationPlan(
            MigrationPlan plan,
            DataOutputView target) throws IOException {

        // Write partition ID
        target.writeInt(plan.getPartitionId());

        // Write operator ID
        byte[] operatorIdBytes = plan.getOperatorId().getBytes();
        target.writeInt(operatorIdBytes.length);
        target.write(operatorIdBytes);

        // Write source instance
        serializeOperatorInstance(plan.getSourceInstance(), target);

        // Write target instance
        serializeOperatorInstance(plan.getTargetInstance(), target);

        // Write job ID
        byte[] jobIdBytes = plan.getJobId().getBytes();
        target.writeInt(jobIdBytes.length);
        target.write(jobIdBytes);

        // Write estimated state size
        target.writeLong(plan.getEstimatedStateSize());
    }

    /**
     * Deserialize migration plan.
     */
    private static MigrationPlan deserializeMigrationPlan(
            DataInputView source) throws IOException {

        // Read partition ID
        int partitionId = source.readInt();

        // Read operator ID
        int operatorIdLength = source.readInt();
        byte[] operatorIdBytes = new byte[operatorIdLength];
        source.readFully(operatorIdBytes);
        org.apache.flink.runtime.jobgraph.OperatorID operatorId =
                new org.apache.flink.runtime.jobgraph.OperatorID(operatorIdBytes);

        // Read source instance
        MigrationPlan.OperatorInstance sourceInstance = deserializeOperatorInstance(source);

        // Read target instance
        MigrationPlan.OperatorInstance targetInstance = deserializeOperatorInstance(source);

        // Read job ID
        int jobIdLength = source.readInt();
        byte[] jobIdBytes = new byte[jobIdLength];
        source.readFully(jobIdBytes);
        org.apache.flink.api.common.JobID jobId =
                new org.apache.flink.api.common.JobID(jobIdBytes);

        // Read estimated state size
        long estimatedStateSize = source.readLong();

        return new MigrationPlan(
                partitionId,
                operatorId,
                sourceInstance,
                targetInstance,
                jobId,
                estimatedStateSize);
    }

    /**
     * Serialize operator instance.
     */
    private static void serializeOperatorInstance(
            MigrationPlan.OperatorInstance instance,
            DataOutputView target) throws IOException {

        // Write execution ID
        byte[] executionIdBytes = instance.getExecutionId().getBytes();
        target.writeInt(executionIdBytes.length);
        target.write(executionIdBytes);

        // Write subtask index
        target.writeInt(instance.getSubtaskIndex());

        // Write task manager location
        target.writeUTF(instance.getTaskManagerLocation());
    }

    /**
     * Deserialize operator instance.
     */
    private static MigrationPlan.OperatorInstance deserializeOperatorInstance(
            DataInputView source) throws IOException {

        // Read execution ID
        int executionIdLength = source.readInt();
        byte[] executionIdBytes = new byte[executionIdLength];
        source.readFully(executionIdBytes);
        org.apache.flink.runtime.executiongraph.ExecutionAttemptID executionId =
                new org.apache.flink.runtime.executiongraph.ExecutionAttemptID(executionIdBytes);

        // Read subtask index
        int subtaskIndex = source.readInt();

        // Read task manager location
        String taskManagerLocation = source.readUTF();

        return new MigrationPlan.OperatorInstance(
                executionId,
                subtaskIndex,
                taskManagerLocation);
    }

    /**
     * Get estimated serialized size of migration barrier.
     */
    public static int getSerializedSize(MigrationBarrier barrier) {
        // Rough estimate
        int size = 4 + 8 + 8 + 8 + 1; // event type + checkpoint ID + timestamp + migration ID + final flag
        size += 4; // num plans

        for (MigrationPlan plan : barrier.getMigrationPlans()) {
            size += 4; // partition ID
            size += 4 + 16; // operator ID
            size += 4 + 16 + 4 + 100; // source instance (rough estimate)
            size += 4 + 16 + 4 + 100; // target instance
            size += 4 + 16; // job ID
            size += 8; // estimated state size
        }

        return size;
    }
}