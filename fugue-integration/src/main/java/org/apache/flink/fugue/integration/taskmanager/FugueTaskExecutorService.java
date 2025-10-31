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

package org.apache.flink.fugue.integration.taskmanager;

import org.apache.flink.fugue.common.rpc.MigrationMessages;
import org.apache.flink.fugue.coordinator.rpc.FugueRpcGateway;
import org.apache.flink.fugue.runtime.controller.MigrationController;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Service that integrates Fugue into Flink's TaskExecutor.
 * Manages the lifecycle of MigrationController and handles RPC messages.
 */
public class FugueTaskExecutorService extends RpcEndpoint
        implements FugueRpcGateway.TaskManagerMigrationGateway {

    private static final Logger LOG = LoggerFactory.getLogger(FugueTaskExecutorService.class);

    /** Task manager location identifier. */
    private final String taskManagerLocation;

    /** Migration controller for this TaskManager. */
    private final MigrationController migrationController;

    /** RPC address for coordinator callbacks. */
    private volatile String coordinatorAddress;

    /**
     * Create Fugue service for a TaskExecutor.
     *
     * @param rpcService Flink's RPC service
     * @param taskManagerLocation Task manager location identifier
     */
    public FugueTaskExecutorService(
            RpcService rpcService,
            String taskManagerLocation) {
        super(rpcService);

        this.taskManagerLocation = taskManagerLocation;
        this.migrationController = new MigrationController(taskManagerLocation);

        // Set up coordinator callback
        this.migrationController.setCoordinatorCallback(this::sendToCoordinator);

        LOG.info("Fugue TaskExecutor service created for TM {}", taskManagerLocation);
    }

    @Override
    public void start() {
        super.start();
        LOG.info("Fugue TaskExecutor service started for TM {}", taskManagerLocation);
    }

    @Override
    public CompletableFuture<Void> onStop() {
        LOG.info("Stopping Fugue TaskExecutor service for TM {}", taskManagerLocation);

        // Shutdown migration controller
        migrationController.shutdown();

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<MigrationMessages.MigrationMessage> handleMigrationMessage(
            MigrationMessages.MigrationMessage message) {

        LOG.debug("Received migration message: {}", message.getClass().getSimpleName());

        // Dispatch to appropriate handler based on message type
        if (message instanceof MigrationMessages.PrepareMigrationSource) {
            return migrationController.handlePrepareMigrationSource(
                    (MigrationMessages.PrepareMigrationSource) message)
                    .thenApply(response -> (MigrationMessages.MigrationMessage) response);

        } else if (message instanceof MigrationMessages.PrepareMigrationTarget) {
            return migrationController.handlePrepareMigrationTarget(
                    (MigrationMessages.PrepareMigrationTarget) message)
                    .thenApply(response -> (MigrationMessages.MigrationMessage) response);

        } else if (message instanceof MigrationMessages.StartPreCopy) {
            return migrationController.handleStartPreCopy(
                    (MigrationMessages.StartPreCopy) message)
                    .thenApply(response -> (MigrationMessages.MigrationMessage) response);

        } else if (message instanceof MigrationMessages.TransferFinalDelta) {
            return migrationController.handleTransferFinalDelta(
                    (MigrationMessages.TransferFinalDelta) message)
                    .thenApply(response -> (MigrationMessages.MigrationMessage) response);

        } else if (message instanceof MigrationMessages.CommitMigration) {
            return migrationController.handleCommitMigration(
                    (MigrationMessages.CommitMigration) message)
                    .thenApply(response -> (MigrationMessages.MigrationMessage) response);

        } else if (message instanceof MigrationMessages.AbortMigration) {
            return migrationController.handleAbortMigration(
                    (MigrationMessages.AbortMigration) message)
                    .thenApply(response -> (MigrationMessages.MigrationMessage) response);

        } else {
            LOG.warn("Unknown migration message type: {}", message.getClass());
            return CompletableFuture.completedFuture(
                    new MigrationMessages.MigrationResponse(
                            message.getMigrationId(),
                            false,
                            "Unknown message type"));
        }
    }

    /**
     * Send message back to coordinator.
     */
    private void sendToCoordinator(MigrationMessages.MigrationMessage message) {
        if (coordinatorAddress == null) {
            LOG.warn("Coordinator address not set, cannot send message");
            return;
        }

        // In full implementation, this would use RPC to send back to coordinator
        LOG.debug("Sending {} to coordinator at {}",
                message.getClass().getSimpleName(), coordinatorAddress);

        // TODO: Implement actual RPC call to coordinator
        // getRpcService().connect(coordinatorAddress, CoordinatorGateway.class)
        //     .thenAccept(gateway -> gateway.handleTaskManagerMessage(message));
    }

    /**
     * Set coordinator address for callbacks.
     */
    public void setCoordinatorAddress(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
        LOG.info("Coordinator address set to {}", coordinatorAddress);
    }

    /**
     * Get the migration controller (for testing).
     */
    public MigrationController getMigrationController() {
        return migrationController;
    }

    /**
     * Get task manager location.
     */
    public String getTaskManagerLocation() {
        return taskManagerLocation;
    }
}