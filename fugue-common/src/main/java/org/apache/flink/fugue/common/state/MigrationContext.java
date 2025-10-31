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

import org.apache.flink.fugue.common.core.MigrationPlan;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Context for tracking the state and progress of an individual migration.
 */
public class MigrationContext implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The migration plan being executed. */
    private final MigrationPlan migrationPlan;

    /** Unique migration ID. */
    private final long migrationId;

    /** Current state of the migration. */
    private final AtomicReference<MigrationState> currentState;

    /** Timestamp when migration started. */
    private final long startTime;

    /** Timestamp when migration completed (if applicable). */
    private volatile long endTime;

    /** Number of pre-copy rounds completed. */
    private final AtomicInteger preCopyRounds;

    /** Current delta log size in bytes. */
    private final AtomicLong currentDeltaSize;

    /** Total bytes transferred so far. */
    private final AtomicLong bytesTransferred;

    /** Last error message if migration failed. */
    private volatile String lastError;

    /** Whether barrier has been injected. */
    private volatile boolean barrierInjected;

    /** Whether final delta has been sent. */
    private volatile boolean finalDeltaSent;

    /** Network bandwidth rate for transfer (bytes/sec). */
    private volatile long networkBandwidth;

    /** Rate of state modifications (bytes/sec). */
    private volatile long dirtyRate;

    public MigrationContext(MigrationPlan migrationPlan, long migrationId) {
        this.migrationPlan = migrationPlan;
        this.migrationId = migrationId;
        this.currentState = new AtomicReference<>(MigrationState.IDLE);
        this.startTime = System.currentTimeMillis();
        this.endTime = -1;
        this.preCopyRounds = new AtomicInteger(0);
        this.currentDeltaSize = new AtomicLong(0);
        this.bytesTransferred = new AtomicLong(0);
        this.barrierInjected = false;
        this.finalDeltaSent = false;
        this.networkBandwidth = 0;
        this.dirtyRate = 0;
    }

    /**
     * Attempt to transition to a new state.
     * @return true if transition was successful, false otherwise
     */
    public boolean transitionTo(MigrationState newState) {
        MigrationState current = currentState.get();
        if (current.canTransitionTo(newState)) {
            boolean success = currentState.compareAndSet(current, newState);
            if (success && newState.isTerminal()) {
                endTime = System.currentTimeMillis();
            }
            return success;
        }
        return false;
    }

    /**
     * Force transition to a state (used for abort scenarios).
     */
    public void forceTransitionTo(MigrationState newState) {
        currentState.set(newState);
        if (newState.isTerminal()) {
            endTime = System.currentTimeMillis();
        }
    }

    /**
     * Increment pre-copy round counter.
     */
    public int incrementPreCopyRound() {
        return preCopyRounds.incrementAndGet();
    }

    /**
     * Update delta log size.
     */
    public void updateDeltaSize(long deltaSize) {
        currentDeltaSize.set(deltaSize);
    }

    /**
     * Add to bytes transferred counter.
     */
    public void addBytesTransferred(long bytes) {
        bytesTransferred.addAndGet(bytes);
    }

    /**
     * Check if migration has converged (R_net > R_dirty).
     */
    public boolean hasConverged() {
        return networkBandwidth > dirtyRate && currentDeltaSize.get() < getConvergenceThreshold();
    }

    /**
     * Get convergence threshold in bytes.
     * Default is 5MB, but can be configured.
     */
    public long getConvergenceThreshold() {
        return 5 * 1024 * 1024; // 5MB default
    }

    /**
     * Calculate migration duration in milliseconds.
     */
    public long getDuration() {
        if (endTime > 0) {
            return endTime - startTime;
        } else {
            return System.currentTimeMillis() - startTime;
        }
    }

    // Getters and setters

    public MigrationPlan getMigrationPlan() {
        return migrationPlan;
    }

    public long getMigrationId() {
        return migrationId;
    }

    public MigrationState getCurrentState() {
        return currentState.get();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public int getPreCopyRounds() {
        return preCopyRounds.get();
    }

    public long getCurrentDeltaSize() {
        return currentDeltaSize.get();
    }

    public long getBytesTransferred() {
        return bytesTransferred.get();
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public boolean isBarrierInjected() {
        return barrierInjected;
    }

    public void setBarrierInjected(boolean barrierInjected) {
        this.barrierInjected = barrierInjected;
    }

    public boolean isFinalDeltaSent() {
        return finalDeltaSent;
    }

    public void setFinalDeltaSent(boolean finalDeltaSent) {
        this.finalDeltaSent = finalDeltaSent;
    }

    public long getNetworkBandwidth() {
        return networkBandwidth;
    }

    public void setNetworkBandwidth(long networkBandwidth) {
        this.networkBandwidth = networkBandwidth;
    }

    public long getDirtyRate() {
        return dirtyRate;
    }

    public void setDirtyRate(long dirtyRate) {
        this.dirtyRate = dirtyRate;
    }

    @Override
    public String toString() {
        return String.format("MigrationContext{id=%d, state=%s, rounds=%d, deltaSize=%d, transferred=%d, duration=%dms}",
                migrationId, currentState.get(), preCopyRounds.get(),
                currentDeltaSize.get(), bytesTransferred.get(), getDuration());
    }
}