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

package org.apache.flink.fugue.common.state;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.Set;

/**
 * In-memory structure for tracking state modifications during migration.
 * Captures all state changes that occur after the initial snapshot.
 */
public class DeltaLog implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Entry representing a state modification.
     */
    public static class DeltaEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        public enum OperationType {
            PUT,    // Key-value insertion or update
            DELETE  // Key deletion
        }

        private final OperationType operation;
        private final byte[] key;
        private final byte[] value;
        private final long timestamp;

        public DeltaEntry(OperationType operation, byte[] key, byte[] value) {
            this.operation = operation;
            this.key = key;
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }

        public OperationType getOperation() {
            return operation;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getSize() {
            int size = 1; // operation type
            size += 4 + (key != null ? key.length : 0); // key length + key
            size += 4 + (value != null ? value.length : 0); // value length + value
            size += 8; // timestamp
            return size;
        }
    }

    /** Map of key to latest delta entry. */
    private final ConcurrentHashMap<ByteBuffer, DeltaEntry> deltaMap;

    /** Total size of delta log in bytes. */
    private final AtomicLong totalSize;

    /** Round number for this delta log. */
    private final int roundNumber;

    /** Creation timestamp. */
    private final long creationTime;

    public DeltaLog(int roundNumber) {
        this.deltaMap = new ConcurrentHashMap<>();
        this.totalSize = new AtomicLong(0);
        this.roundNumber = roundNumber;
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Record a PUT operation.
     */
    public void recordPut(byte[] key, byte[] value) {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        DeltaEntry newEntry = new DeltaEntry(DeltaEntry.OperationType.PUT, key, value);

        DeltaEntry oldEntry = deltaMap.put(keyBuffer, newEntry);

        // Update size
        totalSize.addAndGet(newEntry.getSize());
        if (oldEntry != null) {
            totalSize.addAndGet(-oldEntry.getSize());
        }
    }

    /**
     * Record a DELETE operation.
     */
    public void recordDelete(byte[] key) {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        DeltaEntry newEntry = new DeltaEntry(DeltaEntry.OperationType.DELETE, key, null);

        DeltaEntry oldEntry = deltaMap.put(keyBuffer, newEntry);

        // Update size
        totalSize.addAndGet(newEntry.getSize());
        if (oldEntry != null) {
            totalSize.addAndGet(-oldEntry.getSize());
        }
    }

    /**
     * Get all delta entries.
     */
    public Map<ByteBuffer, DeltaEntry> getAllEntries() {
        return new ConcurrentHashMap<>(deltaMap);
    }

    /**
     * Get the set of modified keys.
     */
    public Set<ByteBuffer> getModifiedKeys() {
        return deltaMap.keySet();
    }

    /**
     * Clear all entries.
     */
    public void clear() {
        deltaMap.clear();
        totalSize.set(0);
    }

    /**
     * Merge another delta log into this one.
     * Later entries override earlier ones.
     */
    public void merge(DeltaLog other) {
        for (Map.Entry<ByteBuffer, DeltaEntry> entry : other.deltaMap.entrySet()) {
            ByteBuffer key = entry.getKey();
            DeltaEntry newEntry = entry.getValue();

            DeltaEntry oldEntry = deltaMap.put(key, newEntry);

            totalSize.addAndGet(newEntry.getSize());
            if (oldEntry != null) {
                totalSize.addAndGet(-oldEntry.getSize());
            }
        }
    }

    /**
     * Get the total size of the delta log in bytes.
     */
    public long getSize() {
        return totalSize.get();
    }

    /**
     * Get the number of entries in the delta log.
     */
    public int getEntryCount() {
        return deltaMap.size();
    }

    /**
     * Get the round number for this delta log.
     */
    public int getRoundNumber() {
        return roundNumber;
    }

    /**
     * Get creation timestamp.
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Check if the delta log is empty.
     */
    public boolean isEmpty() {
        return deltaMap.isEmpty();
    }

    /**
     * Serialize the delta log to a byte array for network transfer.
     */
    public byte[] serialize() {
        int estimatedSize = (int) Math.min(totalSize.get() + 1024, Integer.MAX_VALUE);
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        // Write header
        buffer.putInt(roundNumber);
        buffer.putLong(creationTime);
        buffer.putInt(deltaMap.size());

        // Write entries
        for (Map.Entry<ByteBuffer, DeltaEntry> entry : deltaMap.entrySet()) {
            DeltaEntry delta = entry.getValue();

            // Write operation type
            buffer.put((byte) delta.getOperation().ordinal());

            // Write key
            buffer.putInt(delta.getKey().length);
            buffer.put(delta.getKey());

            // Write value (if exists)
            if (delta.getValue() != null) {
                buffer.putInt(delta.getValue().length);
                buffer.put(delta.getValue());
            } else {
                buffer.putInt(0);
            }

            // Write timestamp
            buffer.putLong(delta.getTimestamp());
        }

        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Deserialize a delta log from a byte array.
     */
    public static DeltaLog deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Read header
        int roundNumber = buffer.getInt();
        long creationTime = buffer.getLong();
        int entryCount = buffer.getInt();

        DeltaLog deltaLog = new DeltaLog(roundNumber);

        // Read entries
        for (int i = 0; i < entryCount; i++) {
            // Read operation type
            byte opType = buffer.get();
            DeltaEntry.OperationType operation = DeltaEntry.OperationType.values()[opType];

            // Read key
            int keyLength = buffer.getInt();
            byte[] key = new byte[keyLength];
            buffer.get(key);

            // Read value
            int valueLength = buffer.getInt();
            byte[] value = null;
            if (valueLength > 0) {
                value = new byte[valueLength];
                buffer.get(value);
            }

            // Read timestamp (but we'll create new entry with current time)
            long timestamp = buffer.getLong();

            // Add entry to delta log
            if (operation == DeltaEntry.OperationType.PUT) {
                deltaLog.recordPut(key, value);
            } else {
                deltaLog.recordDelete(key);
            }
        }

        return deltaLog;
    }

    @Override
    public String toString() {
        return String.format("DeltaLog{round=%d, entries=%d, size=%d bytes}",
                roundNumber, deltaMap.size(), totalSize.get());
    }
}