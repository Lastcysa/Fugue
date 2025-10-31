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

package org.apache.flink.fugue.runtime.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages buffering of post-barrier records on the target side during migration.
 * Ensures records that arrive after the barrier are buffered until state is ready.
 */
public class RecordBufferManager {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBufferManager.class);

    /** Buffers per migration. */
    private final Map<Long, PartitionBuffer> buffers;

    /** Map from partition ID to migration ID. */
    private final Map<Integer, Long> partitionToMigration;

    /** Maximum buffer size per partition (in records). */
    private final int maxBufferSize;

    public RecordBufferManager() {
        this(100000); // Default 100K records per buffer
    }

    public RecordBufferManager(int maxBufferSize) {
        this.buffers = new ConcurrentHashMap<>();
        this.partitionToMigration = new ConcurrentHashMap<>();
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Create buffer for a migration.
     */
    public void createBuffer(long migrationId, int partitionId) {
        LOG.info("Creating buffer for migration {}, partition {}", migrationId, partitionId);

        PartitionBuffer buffer = new PartitionBuffer(migrationId, partitionId, maxBufferSize);
        buffers.put(migrationId, buffer);
        partitionToMigration.put(partitionId, migrationId);

        LOG.info("Buffer created for migration {}", migrationId);
    }

    /**
     * Start buffering records for a partition.
     */
    public void startBuffering(long migrationId, int partitionId) {
        PartitionBuffer buffer = buffers.get(migrationId);
        if (buffer == null) {
            LOG.warn("No buffer found for migration {}", migrationId);
            return;
        }

        buffer.startBuffering();
        LOG.info("Started buffering for migration {}, partition {}", migrationId, partitionId);
    }

    /**
     * Buffer a record for a partition.
     * Returns true if buffered, false if buffering is not active.
     */
    public boolean bufferRecord(int partitionId, Object record) {
        Long migrationId = partitionToMigration.get(partitionId);
        if (migrationId == null) {
            return false; // No active migration
        }

        PartitionBuffer buffer = buffers.get(migrationId);
        if (buffer == null || !buffer.isBuffering()) {
            return false;
        }

        boolean success = buffer.add(record);
        if (!success) {
            LOG.warn("Buffer full for migration {}, partition {}", migrationId, partitionId);
        }

        return success;
    }

    /**
     * Check if buffering is active for a partition.
     */
    public boolean isBuffering(int partitionId) {
        Long migrationId = partitionToMigration.get(partitionId);
        if (migrationId == null) {
            return false;
        }

        PartitionBuffer buffer = buffers.get(migrationId);
        return buffer != null && buffer.isBuffering();
    }

    /**
     * Flush buffered records for processing.
     * Returns list of buffered records in order.
     */
    public List<Object> flushBuffer(long migrationId, int partitionId) {
        LOG.info("Flushing buffer for migration {}, partition {}", migrationId, partitionId);

        PartitionBuffer buffer = buffers.get(migrationId);
        if (buffer == null) {
            LOG.warn("No buffer found for migration {}", migrationId);
            return Collections.emptyList();
        }

        List<Object> records = buffer.flush();
        LOG.info("Flushed {} records for migration {}", records.size(), migrationId);

        // Cleanup
        buffers.remove(migrationId);
        partitionToMigration.remove(partitionId);

        return records;
    }

    /**
     * Clear buffer without processing (for abort scenarios).
     */
    public void clearBuffer(long migrationId) {
        LOG.info("Clearing buffer for migration {}", migrationId);

        PartitionBuffer buffer = buffers.remove(migrationId);
        if (buffer != null) {
            buffer.clear();
            partitionToMigration.remove(buffer.getPartitionId());
            LOG.info("Buffer cleared for migration {} ({} records discarded)",
                    migrationId, buffer.getRecordCount());
        }
    }

    /**
     * Get buffer statistics.
     */
    public BufferStats getStats(long migrationId) {
        PartitionBuffer buffer = buffers.get(migrationId);
        if (buffer == null) {
            return null;
        }

        return new BufferStats(
                migrationId,
                buffer.getPartitionId(),
                buffer.getRecordCount(),
                buffer.getBufferSize(),
                buffer.isBuffering());
    }

    /**
     * Shutdown manager and clear all buffers.
     */
    public void shutdown() {
        LOG.info("Shutting down RecordBufferManager");

        for (PartitionBuffer buffer : buffers.values()) {
            buffer.clear();
        }

        buffers.clear();
        partitionToMigration.clear();

        LOG.info("RecordBufferManager shut down");
    }

    /**
     * Buffer for a single partition.
     */
    private static class PartitionBuffer {
        private final long migrationId;
        private final int partitionId;
        private final int maxSize;
        private final LinkedBlockingQueue<Object> records;
        private final AtomicBoolean buffering;
        private final AtomicLong bytesBuffered;
        private final long creationTime;

        public PartitionBuffer(long migrationId, int partitionId, int maxSize) {
            this.migrationId = migrationId;
            this.partitionId = partitionId;
            this.maxSize = maxSize;
            this.records = new LinkedBlockingQueue<>(maxSize);
            this.buffering = new AtomicBoolean(false);
            this.bytesBuffered = new AtomicLong(0);
            this.creationTime = System.currentTimeMillis();
        }

        public void startBuffering() {
            buffering.set(true);
        }

        public boolean isBuffering() {
            return buffering.get();
        }

        public boolean add(Object record) {
            if (!buffering.get()) {
                return false;
            }

            boolean success = records.offer(record);
            if (success) {
                // Estimate size (actual size would require serialization)
                bytesBuffered.addAndGet(estimateRecordSize(record));
            }
            return success;
        }

        public List<Object> flush() {
            buffering.set(false);
            List<Object> result = new ArrayList<>(records.size());
            records.drainTo(result);
            return result;
        }

        public void clear() {
            buffering.set(false);
            records.clear();
            bytesBuffered.set(0);
        }

        public int getRecordCount() {
            return records.size();
        }

        public long getBufferSize() {
            return bytesBuffered.get();
        }

        public int getPartitionId() {
            return partitionId;
        }

        private int estimateRecordSize(Object record) {
            // Rough estimate - would need actual serializer in practice
            return 100; // Assume average 100 bytes per record
        }

        public long getBufferDuration() {
            return System.currentTimeMillis() - creationTime;
        }
    }

    /**
     * Buffer statistics.
     */
    public static class BufferStats {
        private final long migrationId;
        private final int partitionId;
        private final int recordCount;
        private final long bufferSize;
        private final boolean isBuffering;

        public BufferStats(long migrationId, int partitionId, int recordCount,
                          long bufferSize, boolean isBuffering) {
            this.migrationId = migrationId;
            this.partitionId = partitionId;
            this.recordCount = recordCount;
            this.bufferSize = bufferSize;
            this.isBuffering = isBuffering;
        }

        public long getMigrationId() {
            return migrationId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public int getRecordCount() {
            return recordCount;
        }

        public long getBufferSize() {
            return bufferSize;
        }

        public boolean isBuffering() {
            return isBuffering;
        }

        @Override
        public String toString() {
            return String.format("BufferStats{migration=%d, partition=%d, records=%d, size=%d, buffering=%b}",
                    migrationId, partitionId, recordCount, bufferSize, isBuffering);
        }
    }
}