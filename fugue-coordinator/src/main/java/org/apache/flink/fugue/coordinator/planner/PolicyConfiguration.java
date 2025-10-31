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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for elasticity policies.
 */
public class PolicyConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    // Threshold-based policy parameters
    public static final String CPU_SCALE_OUT_THRESHOLD = "cpu.scaleout.threshold";
    public static final String CPU_SCALE_IN_THRESHOLD = "cpu.scalein.threshold";
    public static final String MEMORY_SCALE_OUT_THRESHOLD = "memory.scaleout.threshold";
    public static final String MEMORY_SCALE_IN_THRESHOLD = "memory.scalein.threshold";
    public static final String LATENCY_SCALE_OUT_THRESHOLD = "latency.scaleout.threshold";
    public static final String BACKPRESSURE_THRESHOLD = "backpressure.threshold";
    public static final String THROUGHPUT_DROP_THRESHOLD = "throughput.drop.threshold";

    // Scaling parameters
    public static final String MIN_PARALLELISM = "min.parallelism";
    public static final String MAX_PARALLELISM = "max.parallelism";
    public static final String SCALE_OUT_FACTOR = "scaleout.factor";
    public static final String SCALE_IN_FACTOR = "scalein.factor";
    public static final String COOLDOWN_PERIOD = "cooldown.period.ms";

    // Migration parameters
    public static final String MAX_CONCURRENT_MIGRATIONS = "max.concurrent.migrations";
    public static final String CONVERGENCE_THRESHOLD_BYTES = "convergence.threshold.bytes";
    public static final String MAX_PRECOPY_ROUNDS = "max.precopy.rounds";
    public static final String NETWORK_BANDWIDTH_LIMIT = "network.bandwidth.limit.mbps";

    // Monitoring parameters
    public static final String METRICS_WINDOW_SIZE = "metrics.window.size.ms";
    public static final String METRICS_SAMPLE_INTERVAL = "metrics.sample.interval.ms";
    public static final String EVALUATION_INTERVAL = "evaluation.interval.ms";

    private final Map<String, Object> parameters;

    public PolicyConfiguration() {
        this.parameters = new HashMap<>();
        setDefaults();
    }

    /**
     * Set default configuration values.
     */
    private void setDefaults() {
        // Threshold defaults
        parameters.put(CPU_SCALE_OUT_THRESHOLD, 0.8);           // 80% CPU
        parameters.put(CPU_SCALE_IN_THRESHOLD, 0.3);            // 30% CPU
        parameters.put(MEMORY_SCALE_OUT_THRESHOLD, 0.85);       // 85% memory
        parameters.put(MEMORY_SCALE_IN_THRESHOLD, 0.4);         // 40% memory
        parameters.put(LATENCY_SCALE_OUT_THRESHOLD, 1000.0);    // 1000ms
        parameters.put(BACKPRESSURE_THRESHOLD, 0.5);            // 50% backpressure
        parameters.put(THROUGHPUT_DROP_THRESHOLD, 0.2);         // 20% drop

        // Scaling defaults
        parameters.put(MIN_PARALLELISM, 1);
        parameters.put(MAX_PARALLELISM, 128);
        parameters.put(SCALE_OUT_FACTOR, 1.5);                  // Scale out by 50%
        parameters.put(SCALE_IN_FACTOR, 0.75);                  // Scale in by 25%
        parameters.put(COOLDOWN_PERIOD, 60000L);                // 1 minute

        // Migration defaults
        parameters.put(MAX_CONCURRENT_MIGRATIONS, 10);
        parameters.put(CONVERGENCE_THRESHOLD_BYTES, 5L * 1024 * 1024);  // 5MB
        parameters.put(MAX_PRECOPY_ROUNDS, 10);
        parameters.put(NETWORK_BANDWIDTH_LIMIT, 100.0);         // 100 Mbps

        // Monitoring defaults
        parameters.put(METRICS_WINDOW_SIZE, 300000L);           // 5 minutes
        parameters.put(METRICS_SAMPLE_INTERVAL, 5000L);         // 5 seconds
        parameters.put(EVALUATION_INTERVAL, 30000L);            // 30 seconds
    }

    /**
     * Get a configuration parameter.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) parameters.get(key);
    }

    /**
     * Set a configuration parameter.
     */
    public void set(String key, Object value) {
        parameters.put(key, value);
    }

    /**
     * Get a double parameter with default value.
     */
    public double getDouble(String key, double defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return defaultValue;
    }

    /**
     * Get an integer parameter with default value.
     */
    public int getInt(String key, int defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }

    /**
     * Get a long parameter with default value.
     */
    public long getLong(String key, long defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return defaultValue;
    }

    /**
     * Get a boolean parameter with default value.
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Object value = parameters.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return defaultValue;
    }

    /**
     * Check if a key exists.
     */
    public boolean contains(String key) {
        return parameters.containsKey(key);
    }

    /**
     * Get all parameters.
     */
    public Map<String, Object> getAllParameters() {
        return new HashMap<>(parameters);
    }

    /**
     * Create a copy of this configuration.
     */
    public PolicyConfiguration copy() {
        PolicyConfiguration copy = new PolicyConfiguration();
        copy.parameters.clear();
        copy.parameters.putAll(this.parameters);
        return copy;
    }

    @Override
    public String toString() {
        return "PolicyConfiguration{" + parameters + "}";
    }
}