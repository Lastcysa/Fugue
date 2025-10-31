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

package org.apache.flink.fugue.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Stateful Word Count example with Fugue elasticity.
 * Demonstrates online migration of state during scaling operations.
 */
public class WordCountWithFugue {

    public static void main(String[] args) throws Exception {
        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure for Fugue
        Configuration config = new Configuration();

        // Enable RocksDB state backend (required for Fugue)
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // Configure checkpointing
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // Set parallelism
        env.setParallelism(4);

        // Create data stream from source
        DataStream<String> text = env.addSource(new WordSource())
                .name("Word Source");

        // Tokenize and count words with stateful processing
        DataStream<Tuple2<String, Long>> counts = text
                .flatMap(new Tokenizer())
                .name("Tokenizer")
                .keyBy(value -> value.f0)
                .map(new StatefulCounter())
                .name("Stateful Counter");

        // Print results
        counts.print().name("Print Sink");

        // Execute program
        System.out.println("Starting Fugue-enabled Word Count example...");
        System.out.println("State backend: RocksDB");
        System.out.println("Initial parallelism: " + env.getParallelism());
        System.out.println("\nFugue will automatically handle state migration during scaling.");
        System.out.println("Monitor logs for migration events.\n");

        env.execute("Fugue Word Count Example");
    }

    /**
     * Source that generates random words.
     */
    public static class WordSource implements SourceFunction<String> {
        private static final String[] WORDS = {
                "apache", "flink", "streaming", "fugue", "migration",
                "elasticity", "state", "scalability", "performance",
                "distributed", "system", "data", "processing", "real-time"
        };

        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                // Generate random word
                String word = WORDS[random.nextInt(WORDS.length)];

                // Emit word
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(word);
                }

                // Control rate: 1000 words/sec
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Tokenizer that splits lines into words.
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Emit word with count 1
            out.collect(new Tuple2<>(value, 1));
        }
    }

    /**
     * Stateful counter that maintains running count per key.
     * This state will be migrated when Fugue scales the job.
     */
    public static class StatefulCounter extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>> {
        private transient ValueState<Long> countState;
        private transient long processedRecords;
        private transient long lastLogTime;

        @Override
        public void open(Configuration parameters) {
            // Initialize state
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "word-count",
                    TypeInformation.of(new TypeHint<Long>() {}));

            countState = getRuntimeContext().getState(descriptor);
            processedRecords = 0;
            lastLogTime = System.currentTimeMillis();

            System.out.println("Initialized StatefulCounter on subtask: " +
                    getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Tuple2<String, Long> map(Tuple2<String, Integer> value) throws Exception {
            // Get current count
            Long currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0L;
            }

            // Increment count
            currentCount += value.f1;

            // Update state (this state will be migrated by Fugue if needed)
            countState.update(currentCount);

            // Track processed records
            processedRecords++;

            // Log periodically
            long now = System.currentTimeMillis();
            if (now - lastLogTime > 10000) { // Every 10 seconds
                System.out.println(String.format(
                        "Subtask %d: Processed %d records, current state size estimate: %d words",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        processedRecords,
                        estimateStateSize()));
                lastLogTime = now;
            }

            return new Tuple2<>(value.f0, currentCount);
        }

        /**
         * Estimate state size for monitoring.
         */
        private long estimateStateSize() {
            // In a real implementation, this would query actual state size
            // For demo purposes, we estimate based on processed records
            return processedRecords / 100; // Rough estimate
        }
    }

    /**
     * Configuration builder for Fugue-specific settings.
     */
    public static class FugueConfiguration {
        private final Configuration config;

        public FugueConfiguration() {
            this.config = new Configuration();
        }

        /**
         * Enable Fugue elasticity with threshold-based policy.
         */
        public FugueConfiguration enableElasticity() {
            config.setString("fugue.elasticity.enabled", "true");
            config.setString("fugue.elasticity.policy", "threshold");
            return this;
        }

        /**
         * Set CPU threshold for scale-out.
         */
        public FugueConfiguration setCpuScaleOutThreshold(double threshold) {
            config.setDouble("fugue.policy.cpu.scaleout.threshold", threshold);
            return this;
        }

        /**
         * Set maximum parallelism for scaling.
         */
        public FugueConfiguration setMaxParallelism(int maxParallelism) {
            config.setInteger("fugue.policy.max.parallelism", maxParallelism);
            return this;
        }

        /**
         * Set migration convergence threshold in bytes.
         */
        public FugueConfiguration setConvergenceThreshold(long bytes) {
            config.setLong("fugue.migration.convergence.threshold.bytes", bytes);
            return this;
        }

        /**
         * Set evaluation interval for elasticity policy.
         */
        public FugueConfiguration setEvaluationInterval(long milliseconds) {
            config.setLong("fugue.policy.evaluation.interval.ms", milliseconds);
            return this;
        }

        public Configuration build() {
            return config;
        }
    }
}