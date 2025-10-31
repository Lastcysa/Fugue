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

package org.apache.flink.fugue.runtime.transfer;

import org.apache.flink.fugue.common.state.DeltaLog;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for RocksDB state operations including checkpoints, delta logging, and file ingestion.
 * Integrates with RocksDB's native checkpoint and SST file APIs.
 */
public class RocksDBStateHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateHandler.class);

    /** Base directory for RocksDB state. */
    private final Path baseDir;

    /** RocksDB instances per partition. */
    private final Map<Integer, RocksDB> partitionDatabases;

    /** Active delta logs per partition. */
    private final Map<Integer, DeltaLog> activeDeltaLogs;

    /** Write listeners for capturing modifications. */
    private final Map<Integer, DeltaLoggingListener> deltaListeners;

    /** RocksDB options. */
    private final Options options;

    /** Checkpoint object for creating snapshots. */
    private final Map<Integer, Checkpoint> checkpoints;

    public RocksDBStateHandler() {
        this.baseDir = Paths.get(System.getProperty("java.io.tmpdir"), "fugue-rocksdb");
        this.partitionDatabases = new ConcurrentHashMap<>();
        this.activeDeltaLogs = new ConcurrentHashMap<>();
        this.deltaListeners = new ConcurrentHashMap<>();
        this.checkpoints = new ConcurrentHashMap<>();

        // Initialize RocksDB options
        this.options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(64 * 1024 * 1024) // 64MB
                .setMaxWriteBufferNumber(3)
                .setMaxBackgroundJobs(4)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);

        try {
            Files.createDirectories(baseDir);
        } catch (Exception e) {
            LOG.error("Failed to create base directory", e);
        }

        LOG.info("RocksDB state handler initialized at {}", baseDir);
    }

    /**
     * Get or create RocksDB instance for partition.
     */
    private RocksDB getOrCreateDatabase(int partitionId) throws RocksDBException {
        return partitionDatabases.computeIfAbsent(partitionId, pid -> {
            try {
                Path dbPath = getPartitionPath(pid);
                Files.createDirectories(dbPath);

                RocksDB db = RocksDB.open(options, dbPath.toString());
                LOG.info("Opened RocksDB for partition {} at {}", pid, dbPath);

                // Create checkpoint object
                Checkpoint checkpoint = Checkpoint.create(db);
                checkpoints.put(pid, checkpoint);

                return db;
            } catch (Exception e) {
                throw new RuntimeException("Failed to open RocksDB for partition " + pid, e);
            }
        });
    }

    /**
     * Get partition directory path.
     */
    public Path getPartitionPath(int partitionId) {
        return baseDir.resolve("partition-" + partitionId);
    }

    /**
     * Create a checkpoint for a partition.
     * Returns the path to the checkpoint directory.
     */
    public Path createCheckpoint(int partitionId) throws Exception {
        LOG.info("Creating checkpoint for partition {}", partitionId);

        RocksDB db = getOrCreateDatabase(partitionId);
        Checkpoint checkpoint = checkpoints.get(partitionId);

        if (checkpoint == null) {
            throw new IllegalStateException("Checkpoint not initialized for partition " + partitionId);
        }

        // Create checkpoint directory
        Path checkpointPath = baseDir.resolve("checkpoint-" + partitionId + "-" + System.currentTimeMillis());
        Files.createDirectories(checkpointPath);

        // Create checkpoint
        checkpoint.createCheckpoint(checkpointPath.toString());

        LOG.info("Checkpoint created for partition {} at {}", partitionId, checkpointPath);
        return checkpointPath;
    }

    /**
     * Start delta logging for a partition.
     * Captures all writes to the delta log.
     */
    public void startDeltaLogging(int partitionId, DeltaLog deltaLog) {
        LOG.info("Starting delta logging for partition {}", partitionId);

        activeDeltaLogs.put(partitionId, deltaLog);

        // Create listener to capture writes
        DeltaLoggingListener listener = new DeltaLoggingListener(partitionId, deltaLog);
        deltaListeners.put(partitionId, listener);

        LOG.info("Delta logging started for partition {}", partitionId);
    }

    /**
     * Stop delta logging for a partition.
     */
    public void stopDeltaLogging(int partitionId) {
        LOG.info("Stopping delta logging for partition {}", partitionId);

        activeDeltaLogs.remove(partitionId);
        deltaListeners.remove(partitionId);

        LOG.info("Delta logging stopped for partition {}", partitionId);
    }

    /**
     * Put a key-value pair (intercepted for delta logging).
     */
    public void put(int partitionId, byte[] key, byte[] value) throws RocksDBException {
        RocksDB db = getOrCreateDatabase(partitionId);

        // Write to RocksDB
        db.put(key, value);

        // Record in delta log if active
        DeltaLog deltaLog = activeDeltaLogs.get(partitionId);
        if (deltaLog != null) {
            deltaLog.recordPut(key, value);
        }
    }

    /**
     * Delete a key (intercepted for delta logging).
     */
    public void delete(int partitionId, byte[] key) throws RocksDBException {
        RocksDB db = getOrCreateDatabase(partitionId);

        // Write to RocksDB
        db.delete(key);

        // Record in delta log if active
        DeltaLog deltaLog = activeDeltaLogs.get(partitionId);
        if (deltaLog != null) {
            deltaLog.recordDelete(key);
        }
    }

    /**
     * Get a value by key.
     */
    public byte[] get(int partitionId, byte[] key) throws RocksDBException {
        RocksDB db = getOrCreateDatabase(partitionId);
        return db.get(key);
    }

    /**
     * Ingest SST files into RocksDB (target-side operation).
     * Uses RocksDB's IngestExternalFileAPI for bulk loading.
     */
    public void ingestFiles(int partitionId, Path sstDirectory) throws Exception {
        LOG.info("Ingesting SST files for partition {} from {}", partitionId, sstDirectory);

        RocksDB db = getOrCreateDatabase(partitionId);

        // Find all SST files
        List<String> sstFiles = new ArrayList<>();
        Files.walk(sstDirectory)
                .filter(p -> p.toString().endsWith(".sst"))
                .forEach(p -> sstFiles.add(p.toString()));

        if (sstFiles.isEmpty()) {
            LOG.warn("No SST files found in {}", sstDirectory);
            return;
        }

        // Ingest files
        IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()
                .setMoveFiles(false)
                .setSnapshotConsistency(true)
                .setAllowGlobalSeqNo(true)
                .setAllowBlockingFlush(true);

        db.ingestExternalFile(sstFiles, ingestOptions);

        LOG.info("Ingested {} SST files for partition {}", sstFiles.size(), partitionId);

        ingestOptions.close();
    }

    /**
     * Apply delta log to partition state (target-side operation).
     */
    public void applyDeltaLog(int partitionId, DeltaLog deltaLog) throws Exception {
        LOG.info("Applying delta log to partition {}: {} entries",
                partitionId, deltaLog.getEntryCount());

        RocksDB db = getOrCreateDatabase(partitionId);

        // Apply all delta entries
        WriteBatch batch = new WriteBatch();
        WriteOptions writeOptions = new WriteOptions();

        try {
            for (Map.Entry<ByteBuffer, DeltaLog.DeltaEntry> entry : deltaLog.getAllEntries().entrySet()) {
                DeltaLog.DeltaEntry delta = entry.getValue();

                if (delta.getOperation() == DeltaLog.DeltaEntry.OperationType.PUT) {
                    batch.put(delta.getKey(), delta.getValue());
                } else if (delta.getOperation() == DeltaLog.DeltaEntry.OperationType.DELETE) {
                    batch.delete(delta.getKey());
                }
            }

            // Write batch atomically
            db.write(writeOptions, batch);

            LOG.info("Delta log applied to partition {}", partitionId);

        } finally {
            batch.close();
            writeOptions.close();
        }
    }

    /**
     * Delete partition state.
     */
    public void deletePartition(int partitionId) {
        LOG.info("Deleting partition {}", partitionId);

        try {
            // Close RocksDB instance
            RocksDB db = partitionDatabases.remove(partitionId);
            if (db != null) {
                db.close();
            }

            // Delete checkpoint
            Checkpoint checkpoint = checkpoints.remove(partitionId);
            if (checkpoint != null) {
                // Checkpoint doesn't need explicit close in newer RocksDB versions
            }

            // Stop delta logging
            stopDeltaLogging(partitionId);

            // Delete files
            Path partitionPath = getPartitionPath(partitionId);
            if (Files.exists(partitionPath)) {
                Files.walk(partitionPath)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception e) {
                                LOG.warn("Failed to delete {}", path, e);
                            }
                        });
            }

            LOG.info("Partition {} deleted", partitionId);

        } catch (Exception e) {
            LOG.error("Failed to delete partition {}", partitionId, e);
        }
    }

    /**
     * Get partition state size estimate.
     */
    public long getPartitionSize(int partitionId) {
        try {
            Path partitionPath = getPartitionPath(partitionId);
            if (!Files.exists(partitionPath)) {
                return 0;
            }

            return Files.walk(partitionPath)
                    .filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (Exception e) {
                            return 0;
                        }
                    })
                    .sum();
        } catch (Exception e) {
            LOG.error("Failed to get partition size for {}", partitionId, e);
            return 0;
        }
    }

    /**
     * Close all RocksDB instances.
     */
    public void close() {
        LOG.info("Closing RocksDB state handler");

        for (Map.Entry<Integer, RocksDB> entry : partitionDatabases.entrySet()) {
            try {
                entry.getValue().close();
                LOG.debug("Closed RocksDB for partition {}", entry.getKey());
            } catch (Exception e) {
                LOG.error("Failed to close RocksDB for partition {}", entry.getKey(), e);
            }
        }

        partitionDatabases.clear();
        checkpoints.clear();
        activeDeltaLogs.clear();
        deltaListeners.clear();

        options.close();

        LOG.info("RocksDB state handler closed");
    }

    /**
     * Listener for delta logging.
     * In a real implementation, this would hook into RocksDB's write-ahead log.
     */
    private static class DeltaLoggingListener {
        private final int partitionId;
        private final DeltaLog deltaLog;

        public DeltaLoggingListener(int partitionId, DeltaLog deltaLog) {
            this.partitionId = partitionId;
            this.deltaLog = deltaLog;
        }

        public void onPut(byte[] key, byte[] value) {
            deltaLog.recordPut(key, value);
        }

        public void onDelete(byte[] key) {
            deltaLog.recordDelete(key);
        }
    }
}