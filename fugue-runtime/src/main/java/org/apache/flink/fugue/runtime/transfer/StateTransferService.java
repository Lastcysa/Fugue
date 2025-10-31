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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * State Transfer Service handles the actual data movement between source and target.
 * Manages RocksDB snapshots, SST file transfer, and delta log maintenance.
 */
public class StateTransferService {
    private static final Logger LOG = LoggerFactory.getLogger(StateTransferService.class);

    /** Source-side contexts. */
    private final Map<Long, SourceContext> sourceContexts;

    /** Target-side contexts. */
    private final Map<Long, TargetContext> targetContexts;

    /** Executor for transfer operations. */
    private final ExecutorService transferExecutor;

    /** RocksDB state handler. */
    private final RocksDBStateHandler rocksDBHandler;

    public StateTransferService() {
        this.sourceContexts = new ConcurrentHashMap<>();
        this.targetContexts = new ConcurrentHashMap<>();
        this.transferExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("fugue-state-transfer");
            t.setDaemon(true);
            return t;
        });
        this.rocksDBHandler = new RocksDBStateHandler();
    }

    /**
     * Prepare source for migration.
     */
    public void prepareSource(long migrationId, int partitionId,
                              String targetAddress, int targetPort) throws Exception {
        LOG.info("Preparing source for migration {}, partition {}", migrationId, partitionId);

        SourceContext context = new SourceContext(
                migrationId,
                partitionId,
                targetAddress,
                targetPort);

        // Create initial snapshot checkpoint
        Path checkpointPath = rocksDBHandler.createCheckpoint(partitionId);
        context.setCheckpointPath(checkpointPath);

        // Initialize first delta log
        DeltaLog deltaLog = new DeltaLog(0);
        context.setCurrentDeltaLog(deltaLog);

        sourceContexts.put(migrationId, context);

        LOG.info("Source prepared for migration {}, checkpoint created at {}",
                migrationId, checkpointPath);
    }

    /**
     * Prepare target for migration.
     */
    public void prepareTarget(long migrationId, int partitionId, int listenPort) throws Exception {
        LOG.info("Preparing target for migration {}, partition {}", migrationId, partitionId);

        TargetContext context = new TargetContext(migrationId, partitionId, listenPort);

        // Start listening for incoming state transfer
        ServerSocket serverSocket = new ServerSocket(listenPort);
        context.setServerSocket(serverSocket);

        // Accept connection in background
        transferExecutor.submit(() -> acceptConnection(context));

        targetContexts.put(migrationId, context);

        LOG.info("Target prepared for migration {}, listening on port {}", migrationId, listenPort);
    }

    /**
     * Accept connection from source.
     */
    private void acceptConnection(TargetContext context) {
        try {
            LOG.info("Waiting for connection for migration {}", context.getMigrationId());
            Socket socket = context.getServerSocket().accept();
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(1024 * 1024); // 1MB buffer
            socket.setReceiveBufferSize(1024 * 1024);

            context.setSocket(socket);
            LOG.info("Connection established for migration {}", context.getMigrationId());

        } catch (IOException e) {
            LOG.error("Failed to accept connection for migration {}", context.getMigrationId(), e);
        }
    }

    /**
     * Transfer snapshot from source to target.
     * This is called iteratively for each pre-copy round.
     */
    public TransferResult transferSnapshot(long migrationId, int partitionId,
                                           int roundNumber, long rateLimitBytesPerSec)
            throws Exception {

        SourceContext context = sourceContexts.get(migrationId);
        if (context == null) {
            throw new IllegalStateException("Source context not found for migration " + migrationId);
        }

        LOG.info("Transferring snapshot for migration {}, round {}", migrationId, roundNumber);

        long startTime = System.currentTimeMillis();
        long bytesTransferred = 0;

        // Establish connection if first round
        if (roundNumber == 1) {
            Socket socket = new Socket(context.getTargetAddress(), context.getTargetPort());
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(1024 * 1024);
            socket.setReceiveBufferSize(1024 * 1024);
            context.setSocket(socket);
        }

        Socket socket = context.getSocket();
        if (socket == null || !socket.isConnected()) {
            throw new IllegalStateException("Socket not connected for migration " + migrationId);
        }

        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        if (roundNumber == 1) {
            // First round: transfer SST files from checkpoint
            bytesTransferred = transferSSTFiles(context, out, rateLimitBytesPerSec);
        } else {
            // Subsequent rounds: transfer delta log
            DeltaLog previousDelta = context.getCurrentDeltaLog();
            bytesTransferred = transferDeltaLog(previousDelta, out, rateLimitBytesPerSec);
        }

        // Create new delta log for next round
        DeltaLog newDeltaLog = new DeltaLog(roundNumber);
        context.setCurrentDeltaLog(newDeltaLog);

        // Start logging state modifications to new delta log
        rocksDBHandler.startDeltaLogging(partitionId, newDeltaLog);

        long duration = System.currentTimeMillis() - startTime;
        long deltaSize = newDeltaLog.getSize();

        LOG.info("Snapshot transfer complete for migration {}, round {}: {} bytes in {}ms, delta size: {}",
                migrationId, roundNumber, bytesTransferred, duration, deltaSize);

        return new TransferResult(bytesTransferred, deltaSize);
    }

    /**
     * Transfer SST files from checkpoint.
     */
    private long transferSSTFiles(SourceContext context, DataOutputStream out, long rateLimit)
            throws Exception {

        Path checkpointPath = context.getCheckpointPath();
        List<Path> sstFiles = Files.walk(checkpointPath)
                .filter(p -> p.toString().endsWith(".sst"))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        LOG.info("Transferring {} SST files for migration {}", sstFiles.size(), context.getMigrationId());

        long totalBytes = 0;
        RateLimiter rateLimiter = new RateLimiter(rateLimit);

        // Send number of files
        out.writeInt(sstFiles.size());

        for (Path sstFile : sstFiles) {
            long fileSize = Files.size(sstFile);

            // Send file metadata
            out.writeUTF(sstFile.getFileName().toString());
            out.writeLong(fileSize);

            // Send file content
            try (FileInputStream fis = new FileInputStream(sstFile.toFile());
                 FileChannel channel = fis.getChannel()) {

                ByteBuffer buffer = ByteBuffer.allocate(64 * 1024); // 64KB chunks

                while (channel.read(buffer) > 0) {
                    buffer.flip();
                    byte[] chunk = new byte[buffer.remaining()];
                    buffer.get(chunk);
                    out.write(chunk);

                    totalBytes += chunk.length;
                    rateLimiter.acquire(chunk.length);

                    buffer.clear();
                }
            }

            LOG.debug("Transferred SST file: {} ({} bytes)", sstFile.getFileName(), fileSize);
        }

        out.flush();
        return totalBytes;
    }

    /**
     * Transfer delta log.
     */
    private long transferDeltaLog(DeltaLog deltaLog, DataOutputStream out, long rateLimit)
            throws Exception {

        byte[] serialized = deltaLog.serialize();
        LOG.info("Transferring delta log: {} bytes, {} entries",
                serialized.length, deltaLog.getEntryCount());

        // Send delta log marker
        out.writeInt(-1); // Negative to indicate delta log

        // Send delta log data
        out.writeInt(serialized.length);
        out.write(serialized);
        out.flush();

        // Rate limiting
        RateLimiter rateLimiter = new RateLimiter(rateLimit);
        rateLimiter.acquire(serialized.length);

        return serialized.length;
    }

    /**
     * Transfer final delta after barrier alignment.
     */
    public TransferResult transferFinalDelta(long migrationId, int partitionId) throws Exception {
        SourceContext context = sourceContexts.get(migrationId);
        if (context == null) {
            throw new IllegalStateException("Source context not found for migration " + migrationId);
        }

        LOG.info("Transferring final delta for migration {}", migrationId);

        // Stop delta logging
        rocksDBHandler.stopDeltaLogging(partitionId);

        // Get final delta log
        DeltaLog finalDelta = context.getCurrentDeltaLog();

        Socket socket = context.getSocket();
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        // Transfer final delta
        long bytesTransferred = transferDeltaLog(finalDelta, out, Long.MAX_VALUE); // No rate limit for final

        // Send completion marker
        out.writeInt(-2); // Special marker for completion
        out.flush();

        LOG.info("Final delta transferred for migration {}: {} bytes", migrationId, bytesTransferred);

        return new TransferResult(bytesTransferred, 0);
    }

    /**
     * Receive state on target side.
     * Runs in background thread.
     */
    public void receiveState(long migrationId) {
        transferExecutor.submit(() -> {
            TargetContext context = targetContexts.get(migrationId);
            if (context == null) {
                LOG.error("Target context not found for migration {}", migrationId);
                return;
            }

            try {
                Socket socket = context.getSocket();
                while (socket == null || !socket.isConnected()) {
                    Thread.sleep(100);
                    socket = context.getSocket();
                }

                DataInputStream in = new DataInputStream(socket.getInputStream());

                while (true) {
                    int marker = in.readInt();

                    if (marker == -2) {
                        // Completion marker
                        LOG.info("Received completion marker for migration {}", migrationId);
                        break;
                    } else if (marker == -1) {
                        // Delta log
                        receiveDeltaLog(context, in);
                    } else {
                        // SST files
                        receiveSSTFiles(context, in, marker);
                    }
                }

            } catch (Exception e) {
                LOG.error("Failed to receive state for migration {}", migrationId, e);
            }
        });
    }

    /**
     * Receive SST files on target.
     */
    private void receiveSSTFiles(TargetContext context, DataInputStream in, int numFiles)
            throws Exception {

        LOG.info("Receiving {} SST files for migration {}", numFiles, context.getMigrationId());

        Path targetPath = rocksDBHandler.getPartitionPath(context.getPartitionId());
        Files.createDirectories(targetPath);

        for (int i = 0; i < numFiles; i++) {
            String fileName = in.readUTF();
            long fileSize = in.readLong();

            Path filePath = targetPath.resolve(fileName);

            // Receive file content
            try (FileOutputStream fos = new FileOutputStream(filePath.toFile());
                 FileChannel channel = fos.getChannel()) {

                byte[] buffer = new byte[64 * 1024];
                long remaining = fileSize;

                while (remaining > 0) {
                    int toRead = (int) Math.min(buffer.length, remaining);
                    int read = in.read(buffer, 0, toRead);

                    if (read < 0) {
                        throw new EOFException("Unexpected end of stream");
                    }

                    channel.write(ByteBuffer.wrap(buffer, 0, read));
                    remaining -= read;
                }
            }

            LOG.debug("Received SST file: {} ({} bytes)", fileName, fileSize);
        }

        // Ingest SST files into RocksDB
        rocksDBHandler.ingestFiles(context.getPartitionId(), targetPath);

        LOG.info("SST files received and ingested for migration {}", context.getMigrationId());
    }

    /**
     * Receive delta log on target.
     */
    private void receiveDeltaLog(TargetContext context, DataInputStream in) throws Exception {
        int size = in.readInt();
        byte[] data = new byte[size];
        in.readFully(data);

        DeltaLog deltaLog = DeltaLog.deserialize(data);

        LOG.info("Received delta log for migration {}: {} entries, {} bytes",
                context.getMigrationId(), deltaLog.getEntryCount(), deltaLog.getSize());

        // Apply delta log to state
        rocksDBHandler.applyDeltaLog(context.getPartitionId(), deltaLog);

        LOG.info("Delta log applied for migration {}", context.getMigrationId());
    }

    /**
     * Stop delta logging for a partition.
     */
    public void stopDeltaLogging(long migrationId) {
        SourceContext context = sourceContexts.get(migrationId);
        if (context != null) {
            rocksDBHandler.stopDeltaLogging(context.getPartitionId());
        }
    }

    /**
     * Delete partition state on source after migration.
     */
    public void deletePartitionState(long migrationId, int partitionId) {
        rocksDBHandler.deletePartition(partitionId);
        sourceContexts.remove(migrationId);
        LOG.info("Deleted partition {} state for migration {}", partitionId, migrationId);
    }

    /**
     * Abort source migration.
     */
    public void abortSource(long migrationId) {
        SourceContext context = sourceContexts.remove(migrationId);
        if (context != null) {
            try {
                if (context.getSocket() != null) {
                    context.getSocket().close();
                }
                rocksDBHandler.stopDeltaLogging(context.getPartitionId());
            } catch (IOException e) {
                LOG.error("Error aborting source for migration {}", migrationId, e);
            }
        }
    }

    /**
     * Abort target migration.
     */
    public void abortTarget(long migrationId) {
        TargetContext context = targetContexts.remove(migrationId);
        if (context != null) {
            try {
                if (context.getSocket() != null) {
                    context.getSocket().close();
                }
                if (context.getServerSocket() != null) {
                    context.getServerSocket().close();
                }
            } catch (IOException e) {
                LOG.error("Error aborting target for migration {}", migrationId, e);
            }
        }
    }

    /**
     * Shutdown the service.
     */
    public void shutdown() {
        transferExecutor.shutdown();
        try {
            if (!transferExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                transferExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            transferExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        rocksDBHandler.close();
    }

    /**
     * Transfer result.
     */
    public static class TransferResult {
        private final long bytesTransferred;
        private final long deltaSize;

        public TransferResult(long bytesTransferred, long deltaSize) {
            this.bytesTransferred = bytesTransferred;
            this.deltaSize = deltaSize;
        }

        public long getBytesTransferred() {
            return bytesTransferred;
        }

        public long getDeltaSize() {
            return deltaSize;
        }
    }

    /**
     * Source-side context.
     */
    private static class SourceContext {
        private final long migrationId;
        private final int partitionId;
        private final String targetAddress;
        private final int targetPort;
        private Path checkpointPath;
        private Socket socket;
        private DeltaLog currentDeltaLog;

        public SourceContext(long migrationId, int partitionId,
                           String targetAddress, int targetPort) {
            this.migrationId = migrationId;
            this.partitionId = partitionId;
            this.targetAddress = targetAddress;
            this.targetPort = targetPort;
        }

        public long getMigrationId() {
            return migrationId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public String getTargetAddress() {
            return targetAddress;
        }

        public int getTargetPort() {
            return targetPort;
        }

        public Path getCheckpointPath() {
            return checkpointPath;
        }

        public void setCheckpointPath(Path checkpointPath) {
            this.checkpointPath = checkpointPath;
        }

        public Socket getSocket() {
            return socket;
        }

        public void setSocket(Socket socket) {
            this.socket = socket;
        }

        public DeltaLog getCurrentDeltaLog() {
            return currentDeltaLog;
        }

        public void setCurrentDeltaLog(DeltaLog currentDeltaLog) {
            this.currentDeltaLog = currentDeltaLog;
        }
    }

    /**
     * Target-side context.
     */
    private static class TargetContext {
        private final long migrationId;
        private final int partitionId;
        private final int listenPort;
        private ServerSocket serverSocket;
        private Socket socket;

        public TargetContext(long migrationId, int partitionId, int listenPort) {
            this.migrationId = migrationId;
            this.partitionId = partitionId;
            this.listenPort = listenPort;
        }

        public long getMigrationId() {
            return migrationId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public ServerSocket getServerSocket() {
            return serverSocket;
        }

        public void setServerSocket(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        public Socket getSocket() {
            return socket;
        }

        public void setSocket(Socket socket) {
            this.socket = socket;
        }
    }

    /**
     * Simple rate limiter for bandwidth control.
     */
    private static class RateLimiter {
        private final long bytesPerSecond;
        private long lastTime;
        private long bytesConsumed;

        public RateLimiter(long bytesPerSecond) {
            this.bytesPerSecond = bytesPerSecond;
            this.lastTime = System.currentTimeMillis();
            this.bytesConsumed = 0;
        }

        public void acquire(long bytes) throws InterruptedException {
            if (bytesPerSecond == Long.MAX_VALUE) {
                return; // No limit
            }

            bytesConsumed += bytes;
            long currentTime = System.currentTimeMillis();
            long elapsedMs = currentTime - lastTime;

            if (elapsedMs >= 1000) {
                // Reset counter every second
                lastTime = currentTime;
                bytesConsumed = 0;
            } else {
                long allowedBytes = (bytesPerSecond * elapsedMs) / 1000;
                if (bytesConsumed > allowedBytes) {
                    long sleepTime = ((bytesConsumed - allowedBytes) * 1000) / bytesPerSecond;
                    Thread.sleep(sleepTime);
                }
            }
        }
    }
}