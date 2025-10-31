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

package org.apache.flink.fugue.common;

import org.apache.flink.fugue.common.state.DeltaLog;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DeltaLog.
 */
public class DeltaLogTest {

    @Test
    public void testRecordPut() {
        DeltaLog log = new DeltaLog(1);

        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();

        log.recordPut(key, value);

        assertEquals(1, log.getEntryCount());
        assertTrue(log.getSize() > 0);
    }

    @Test
    public void testRecordDelete() {
        DeltaLog log = new DeltaLog(1);

        byte[] key = "testKey".getBytes();

        log.recordDelete(key);

        assertEquals(1, log.getEntryCount());
        assertTrue(log.getSize() > 0);
    }

    @Test
    public void testMultipleOperations() {
        DeltaLog log = new DeltaLog(1);

        log.recordPut("key1".getBytes(), "value1".getBytes());
        log.recordPut("key2".getBytes(), "value2".getBytes());
        log.recordDelete("key3".getBytes());

        assertEquals(3, log.getEntryCount());
    }

    @Test
    public void testOverwriteKey() {
        DeltaLog log = new DeltaLog(1);

        byte[] key = "key1".getBytes();

        log.recordPut(key, "value1".getBytes());
        assertEquals(1, log.getEntryCount());

        long sizeAfterFirst = log.getSize();

        log.recordPut(key, "value2".getBytes());
        assertEquals(1, log.getEntryCount()); // Still 1 entry

        // Size should have changed
        assertTrue(log.getSize() != sizeAfterFirst);
    }

    @Test
    public void testClear() {
        DeltaLog log = new DeltaLog(1);

        log.recordPut("key1".getBytes(), "value1".getBytes());
        log.recordPut("key2".getBytes(), "value2".getBytes());

        assertFalse(log.isEmpty());
        assertEquals(2, log.getEntryCount());

        log.clear();

        assertTrue(log.isEmpty());
        assertEquals(0, log.getEntryCount());
        assertEquals(0, log.getSize());
    }

    @Test
    public void testMerge() {
        DeltaLog log1 = new DeltaLog(1);
        DeltaLog log2 = new DeltaLog(2);

        log1.recordPut("key1".getBytes(), "value1".getBytes());
        log2.recordPut("key2".getBytes(), "value2".getBytes());

        log1.merge(log2);

        assertEquals(2, log1.getEntryCount());
    }

    @Test
    public void testSerialization() {
        DeltaLog log = new DeltaLog(1);

        log.recordPut("key1".getBytes(), "value1".getBytes());
        log.recordPut("key2".getBytes(), "value2".getBytes());
        log.recordDelete("key3".getBytes());

        // Serialize
        byte[] serialized = log.serialize();
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        // Deserialize
        DeltaLog deserialized = DeltaLog.deserialize(serialized);
        assertNotNull(deserialized);
        assertEquals(log.getEntryCount(), deserialized.getEntryCount());
        assertEquals(log.getRoundNumber(), deserialized.getRoundNumber());
    }

    @Test
    public void testEmptyLog() {
        DeltaLog log = new DeltaLog(0);

        assertTrue(log.isEmpty());
        assertEquals(0, log.getEntryCount());
        assertEquals(0, log.getSize());

        // Serialize empty log
        byte[] serialized = log.serialize();
        DeltaLog deserialized = DeltaLog.deserialize(serialized);

        assertTrue(deserialized.isEmpty());
    }
}