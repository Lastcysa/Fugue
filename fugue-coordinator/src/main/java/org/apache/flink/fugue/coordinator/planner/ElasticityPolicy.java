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

package org.apache.flink.fugue.coordinator.planner;

import org.apache.flink.fugue.common.core.MigrationPlan;

import java.util.List;

/**
 * Interface for elasticity policies that determine when and how to scale.
 */
public interface ElasticityPolicy {

    /**
     * Decision returned by the policy.
     */
    enum ScalingDecision {
        NO_SCALING,      // No scaling needed
        SCALE_OUT,       // Add more resources
        SCALE_IN,        // Remove resources
        REBALANCE        // Rebalance load without changing resource count
    }

    /**
     * Scaling action details.
     */
    class ScalingAction {
        private final ScalingDecision decision;
        private final int targetParallelism;
        private final List<MigrationPlan> migrationPlans;
        private final String reason;

        public ScalingAction(
                ScalingDecision decision,
                int targetParallelism,
                List<MigrationPlan> migrationPlans,
                String reason) {
            this.decision = decision;
            this.targetParallelism = targetParallelism;
            this.migrationPlans = migrationPlans;
            this.reason = reason;
        }

        public ScalingDecision getDecision() {
            return decision;
        }

        public int getTargetParallelism() {
            return targetParallelism;
        }

        public List<MigrationPlan> getMigrationPlans() {
            return migrationPlans;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return String.format("ScalingAction{decision=%s, target=%d, plans=%d, reason=%s}",
                    decision, targetParallelism,
                    migrationPlans != null ? migrationPlans.size() : 0,
                    reason);
        }
    }

    /**
     * Evaluate current metrics and decide if scaling is needed.
     *
     * @param metrics Current system metrics
     * @return Scaling action to take, or null if no action needed
     */
    ScalingAction evaluate(SystemMetrics metrics);

    /**
     * Update policy configuration.
     *
     * @param config New configuration parameters
     */
    void updateConfiguration(PolicyConfiguration config);

    /**
     * Get current policy configuration.
     */
    PolicyConfiguration getConfiguration();

    /**
     * Reset policy state (e.g., after scaling action).
     */
    void reset();
}