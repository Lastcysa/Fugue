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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;

/**
 * Windowed aggregation example with Fugue.
 * Demonstrates migration of large windowed state.
 */
public class WindowedAggregationExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Fugue
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(30000); // 30 second checkpoints
        env.setParallelism(4);

        // Create event stream with watermarks
        DataStream<Event> events = env
                .addSource(new EventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp));

        // Window aggregation with large state
        DataStream<Tuple3<String, Long, Double>> results = events
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new EventAggregator());

        // Print results
        results.print();

        System.out.println("Starting Fugue Windowed Aggregation Example");
        System.out.println("Window size: 1 minute");
        System.out.println("Parallelism: " + env.getParallelism());
        System.out.println("\nThis example maintains large windowed state that Fugue can migrate.");

        env.execute("Fugue Windowed Aggregation");
    }

    /**
     * Event data structure.
     */
    public static class Event {
        public String key;
        public double value;
        public long timestamp;

        public Event() {}

        public Event(String key, double value, long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Event{key=%s, value=%.2f, ts=%d}", key, value, timestamp);
        }
    }

    /**
     * Event source generating synthetic data.
     */
    public static class EventSource implements SourceFunction<Event> {
        private static final String[] KEYS = {
                "sensor-1", "sensor-2", "sensor-3", "sensor-4", "sensor-5",
                "sensor-6", "sensor-7", "sensor-8", "sensor-9", "sensor-10"
        };

        private volatile boolean running = true;
        private final Random random = new Random();
        private long eventTime;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            eventTime = System.currentTimeMillis();

            while (running) {
                // Generate event
                String key = KEYS[random.nextInt(KEYS.length)];
                double value = random.nextDouble() * 100;

                Event event = new Event(key, value, eventTime);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                }

                // Advance time
                eventTime += random.nextInt(100);

                // Control rate
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * Aggregator that computes statistics over window.
     * Maintains large state per key-window combination.
     */
    public static class EventAggregator
            implements AggregateFunction<Event, EventAggregator.Accumulator, Tuple3<String, Long, Double>> {

        /**
         * Accumulator holding aggregation state.
         */
        public static class Accumulator {
            public String key;
            public long count = 0;
            public double sum = 0.0;
            public double min = Double.MAX_VALUE;
            public double max = Double.MIN_VALUE;

            // Large state to simulate real-world scenarios
            public double[] values = new double[1000]; // Keep last 1000 values
            public int valueIndex = 0;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(Event event, Accumulator acc) {
            acc.key = event.key;
            acc.count++;
            acc.sum += event.value;
            acc.min = Math.min(acc.min, event.value);
            acc.max = Math.max(acc.max, event.value);

            // Store value in array (circular buffer)
            acc.values[acc.valueIndex] = event.value;
            acc.valueIndex = (acc.valueIndex + 1) % acc.values.length;

            return acc;
        }

        @Override
        public Tuple3<String, Long, Double> getResult(Accumulator acc) {
            double average = acc.count > 0 ? acc.sum / acc.count : 0.0;
            return new Tuple3<>(acc.key, acc.count, average);
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            a.min = Math.min(a.min, b.min);
            a.max = Math.max(a.max, b.max);

            // Merge value arrays
            for (int i = 0; i < b.valueIndex; i++) {
                a.values[a.valueIndex] = b.values[i];
                a.valueIndex = (a.valueIndex + 1) % a.values.length;
            }

            return a;
        }
    }
}