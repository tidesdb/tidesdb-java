/**
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Alex Gaetano Padula
 *
 * Licensed under the Mozilla Public License, v. 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.mozilla.org/en-US/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tidesdb;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for TidesDB Java bindings.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TidesDBTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    @Order(1)
    void testOpenClose() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            assertNotNull(db);
        }
    }
    
    @Test
    @Order(2)
    void testCreateDropColumnFamily() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb2").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            assertNotNull(cf);
            assertEquals("test_cf", cf.getName());
            
            String[] families = db.listColumnFamilies();
            assertTrue(families.length > 0);
            
            db.dropColumnFamily("test_cf");
        }
    }
    
    @Test
    @Order(3)
    void testTransactionPutGetDelete() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb3").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            byte[] key = "key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);
            
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, key, value);
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, key);
                assertNotNull(result);
                assertArrayEquals(value, result);
            }
            
            try (Transaction txn = db.beginTransaction()) {
                txn.delete(cf, key);
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                assertThrows(TidesDBException.class, () -> txn.get(cf, key));
            }
        }
    }
    
    @Test
    @Order(4)
    void testTransactionWithTTL() throws TidesDBException, InterruptedException {
        Config config = Config.builder(tempDir.resolve("testdb4").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            byte[] key = "temp_key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "temp_value".getBytes(StandardCharsets.UTF_8);
            
            // Set TTL to 2 seconds from now
            long ttl = Instant.now().getEpochSecond() + 2;
            
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, key, value, ttl);
                txn.commit();
            }
            
            // Verify key exists before expiration
            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, key);
                assertNotNull(result);
                assertArrayEquals(value, result);
            }
            
            Thread.sleep(3000);
            
            // Verify key is expired
            try (Transaction txn = db.beginTransaction()) {
                assertThrows(TidesDBException.class, () -> txn.get(cf, key));
            }
        }
    }
    
    @Test
    @Order(5)
    void testMultiOperationTransaction() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb5").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Multiple operations in one transaction
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.put(cf, "key2".getBytes(), "value2".getBytes());
                txn.put(cf, "key3".getBytes(), "value3".getBytes());
                txn.commit();
            }
            
            // Verify all keys exist
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 1; i <= 3; i++) {
                    byte[] key = ("key" + i).getBytes();
                    byte[] expectedValue = ("value" + i).getBytes();
                    byte[] result = txn.get(cf, key);
                    assertArrayEquals(expectedValue, result);
                }
            }
        }
    }
    
    @Test
    @Order(6)
    void testTransactionRollback() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb6").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            byte[] key = "rollback_key".getBytes();
            byte[] value = "rollback_value".getBytes();
            
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, key, value);
                txn.rollback();
            }
            
            // Verify key does not exist
            try (Transaction txn = db.beginTransaction()) {
                assertThrows(TidesDBException.class, () -> txn.get(cf, key));
            }
        }
    }
    
    @Test
    @Order(7)
    void testSavepoints() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb7").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                
                txn.savepoint("sp1");
                txn.put(cf, "key2".getBytes(), "value2".getBytes());
                
                // Rollback to savepoint -- key2 is discarded, key1 remains
                txn.rollbackToSavepoint("sp1");
                
                // Add different operation after rollback
                txn.put(cf, "key3".getBytes(), "value3".getBytes());
                
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                // key1 should exist
                assertNotNull(txn.get(cf, "key1".getBytes()));
                
                // key2 should not exist (rolled back)
                assertThrows(TidesDBException.class, () -> txn.get(cf, "key2".getBytes()));
                
                // key3 should exist
                assertNotNull(txn.get(cf, "key3".getBytes()));
            }
        }
    }
    
    @Test
    @Order(8)
    void testIterator() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb8").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 10; i++) {
                    String key = String.format("key%02d", i);
                    String value = "value" + i;
                    txn.put(cf, key.getBytes(), value.getBytes());
                }
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                try (TidesDBIterator iter = txn.newIterator(cf)) {
                    iter.seekToFirst();
                    
                    int count = 0;
                    while (iter.isValid()) {
                        byte[] key = iter.key();
                        byte[] value = iter.value();
                        assertNotNull(key);
                        assertNotNull(value);
                        count++;
                        iter.next();
                    }
                    assertEquals(10, count);
                }
            }
            
            try (Transaction txn = db.beginTransaction()) {
                try (TidesDBIterator iter = txn.newIterator(cf)) {
                    iter.seekToLast();
                    
                    int count = 0;
                    while (iter.isValid()) {
                        byte[] key = iter.key();
                        byte[] value = iter.value();
                        assertNotNull(key);
                        assertNotNull(value);
                        count++;
                        iter.prev();
                    }
                    assertEquals(10, count);
                }
            }
        }
    }
    
    @Test
    @Order(9)
    void testIsolationLevels() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb9").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            for (IsolationLevel level : IsolationLevel.values()) {
                try (Transaction txn = db.beginTransaction(level)) {
                    assertNotNull(txn);
                }
            }
        }
    }
    
    @Test
    @Order(10)
    void testColumnFamilyStats() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb10").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 100; i++) {
                    txn.put(cf, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            Stats stats = cf.getStats();
            assertNotNull(stats);
            assertTrue(stats.getNumLevels() >= 0);
            assertTrue(stats.getTotalKeys() >= 0);
            assertTrue(stats.getTotalDataSize() >= 0);
            assertTrue(stats.getAvgKeySize() >= 0);
            assertTrue(stats.getAvgValueSize() >= 0);
            assertTrue(stats.getReadAmp() >= 0);
            assertTrue(stats.getHitRate() >= 0.0 && stats.getHitRate() <= 1.0);
            assertFalse(stats.isUseBtree());
        }
    }
    
    @Test
    @Order(11)
    void testCacheStats() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb11").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            CacheStats stats = db.getCacheStats();
            assertNotNull(stats);
        }
    }
    
    @Test
    @Order(12)
    void testCustomColumnFamilyConfig() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb12").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .writeBufferSize(128 * 1024 * 1024)
                .levelSizeRatio(10)
                .minLevels(5)
                .compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)
                .enableBloomFilter(true)
                .bloomFPR(0.01)
                .enableBlockIndexes(true)
                .syncMode(SyncMode.SYNC_INTERVAL)
                .syncIntervalUs(128000)
                .defaultIsolationLevel(IsolationLevel.READ_COMMITTED)
                .build();
            
            db.createColumnFamily("custom_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("custom_cf");
            assertNotNull(cf);
            assertEquals("custom_cf", cf.getName());
        }
    }

    @Test
    @Order(13)
    void testBtreeColumnFamily() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb13").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .writeBufferSize(128 * 1024 * 1024)
                .levelSizeRatio(10)
                .minLevels(5)
                .compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)
                .enableBloomFilter(true)
                .bloomFPR(0.01)
                .enableBlockIndexes(true)
                .syncMode(SyncMode.SYNC_FULL)
                .useBtree(true)
                .build();
            
            db.createColumnFamily("btree_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("btree_cf");
            assertNotNull(cf);
            assertEquals("btree_cf", cf.getName());
            
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 100; i++) {
                    txn.put(cf, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, "key50".getBytes());
                assertNotNull(result);
                assertArrayEquals("value50".getBytes(), result);
            }
            
            Stats stats = cf.getStats();
            assertNotNull(stats);
            assertTrue(stats.isUseBtree());
            assertTrue(stats.getBtreeTotalNodes() >= 0);
            assertTrue(stats.getBtreeMaxHeight() >= 0);
            assertTrue(stats.getBtreeAvgHeight() >= 0.0);
        }
    }
    
    @Test
    @Order(14)
    void testBtreeIterator() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb14").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .writeBufferSize(128 * 1024 * 1024)
                .compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)
                .enableBloomFilter(true)
                .useBtree(true)
                .build();
            
            db.createColumnFamily("btree_iter_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("btree_iter_cf");
            
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 10; i++) {
                    String key = String.format("key%02d", i);
                    String value = "value" + i;
                    txn.put(cf, key.getBytes(), value.getBytes());
                }
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                try (TidesDBIterator iter = txn.newIterator(cf)) {
                    iter.seekToFirst();
                    
                    int count = 0;
                    while (iter.isValid()) {
                        byte[] key = iter.key();
                        byte[] value = iter.value();
                        assertNotNull(key);
                        assertNotNull(value);
                        count++;
                        iter.next();
                    }
                    assertEquals(10, count);
                }
            }
        }
    }
    
    @Test
    @Order(15)
    void testCloneColumnFamily() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb15").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("source_cf", cfConfig);
            
            ColumnFamily sourceCf = db.getColumnFamily("source_cf");
            
            // Insert data into source
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 10; i++) {
                    txn.put(sourceCf, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            // Clone the column family
            db.cloneColumnFamily("source_cf", "cloned_cf");
            
            // Verify clone exists
            ColumnFamily clonedCf = db.getColumnFamily("cloned_cf");
            assertNotNull(clonedCf);
            assertEquals("cloned_cf", clonedCf.getName());
            
            // Verify both column families are listed
            String[] families = db.listColumnFamilies();
            assertTrue(families.length >= 2);
            
            // Verify data exists in clone
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 10; i++) {
                    byte[] result = txn.get(clonedCf, ("key" + i).getBytes());
                    assertNotNull(result);
                    assertArrayEquals(("value" + i).getBytes(), result);
                }
            }
            
            // Verify independence: insert into clone, should not appear in source
            try (Transaction txn = db.beginTransaction()) {
                txn.put(clonedCf, "clone_only_key".getBytes(), "clone_only_value".getBytes());
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                assertThrows(TidesDBException.class, () -> txn.get(sourceCf, "clone_only_key".getBytes()));
            }
        }
    }
    
    @Test
    @Order(16)
    void testCheckpoint() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb16").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Insert some data
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 10; i++) {
                    txn.put(cf, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            // Create checkpoint
            String checkpointDir = tempDir.resolve("testdb16_checkpoint").toString();
            db.checkpoint(checkpointDir);
            
            // Open the checkpoint as a separate database and verify data
            Config checkpointConfig = Config.builder(checkpointDir)
                .numFlushThreads(2)
                .numCompactionThreads(2)
                .logLevel(LogLevel.INFO)
                .blockCacheSize(64 * 1024 * 1024)
                .maxOpenSSTables(256)
                .build();
            
            try (TidesDB checkpointDb = TidesDB.open(checkpointConfig)) {
                ColumnFamily checkpointCf = checkpointDb.getColumnFamily("test_cf");
                assertNotNull(checkpointCf);
                
                try (Transaction txn = checkpointDb.beginTransaction()) {
                    for (int i = 0; i < 10; i++) {
                        byte[] result = txn.get(checkpointCf, ("key" + i).getBytes());
                        assertNotNull(result);
                        assertArrayEquals(("value" + i).getBytes(), result);
                    }
                }
            }
        }
    }
    
    @Test
    @Order(17)
    void testCheckpointNullDir() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb16b").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            assertThrows(IllegalArgumentException.class, () -> db.checkpoint(null));
            assertThrows(IllegalArgumentException.class, () -> db.checkpoint(""));
        }
    }
    
    @Test
    @Order(18)
    void testTransactionPutGetDeleteBadKey() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb3").toString())
                .numFlushThreads(2)
                .numCompactionThreads(2)
                .logLevel(LogLevel.INFO)
                .blockCacheSize(64 * 1024 * 1024)
                .maxOpenSSTables(256)
                .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            ColumnFamily cf = db.getColumnFamily("test_cf");

            byte[] key = new byte[0]; // Bad key (empty)
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);

            assertThrows(IllegalArgumentException.class, () -> {
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, key, value);
                }
            });

            assertThrows(IllegalArgumentException.class, () -> {
                try (Transaction txn = db.beginTransaction()) {
                    byte[] result = txn.get(cf, key);
                    assertNotNull(result);
                    assertArrayEquals(value, result);
                }
            });

            assertThrows(IllegalArgumentException.class, () -> {
                try (Transaction txn = db.beginTransaction()) {
                    txn.delete(cf, key);
                    txn.commit();
                }
            });
        }
    }
    
    @Test
    @Order(19)
    void testTransactionReset() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb17").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Begin transaction and do first batch of work
            Transaction txn = db.beginTransaction();
            txn.put(cf, "key1".getBytes(), "value1".getBytes());
            txn.commit();
            
            // Reset instead of free + begin
            txn.reset(IsolationLevel.READ_COMMITTED);
            
            // Second batch of work using the same transaction
            txn.put(cf, "key2".getBytes(), "value2".getBytes());
            txn.commit();
            
            // Free once when done
            txn.free();
            
            // Verify both keys exist
            try (Transaction readTxn = db.beginTransaction()) {
                byte[] result1 = readTxn.get(cf, "key1".getBytes());
                assertNotNull(result1);
                assertArrayEquals("value1".getBytes(), result1);
                
                byte[] result2 = readTxn.get(cf, "key2".getBytes());
                assertNotNull(result2);
                assertArrayEquals("value2".getBytes(), result2);
            }
        }
    }
    
    @Test
    @Order(20)
    void testTransactionResetWithDifferentIsolation() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb18").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Begin with READ_COMMITTED
            Transaction txn = db.beginTransaction(IsolationLevel.READ_COMMITTED);
            txn.put(cf, "key1".getBytes(), "value1".getBytes());
            txn.commit();
            
            // Reset with different isolation level (REPEATABLE_READ)
            txn.reset(IsolationLevel.REPEATABLE_READ);
            txn.put(cf, "key2".getBytes(), "value2".getBytes());
            txn.commit();
            
            // Reset again with SERIALIZABLE
            txn.reset(IsolationLevel.SERIALIZABLE);
            txn.put(cf, "key3".getBytes(), "value3".getBytes());
            txn.commit();
            
            txn.free();
            
            // Verify all keys exist
            try (Transaction readTxn = db.beginTransaction()) {
                for (int i = 1; i <= 3; i++) {
                    byte[] result = readTxn.get(cf, ("key" + i).getBytes());
                    assertNotNull(result);
                    assertArrayEquals(("value" + i).getBytes(), result);
                }
            }
        }
    }
    
    @Test
    @Order(22)
    void testRangeCost() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb20").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Insert data
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 100; i++) {
                    String key = String.format("key%04d", i);
                    txn.put(cf, key.getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            // Estimate cost for a range
            double cost = cf.rangeCost("key0000".getBytes(), "key0099".getBytes());
            assertTrue(cost >= 0.0, "Range cost should be non-negative");
        }
    }
    
    @Test
    @Order(23)
    void testRangeCostComparison() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb21").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Insert data
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 1000; i++) {
                    String key = String.format("key%04d", i);
                    txn.put(cf, key.getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            // Both costs should be non-negative
            double costSmall = cf.rangeCost("key0000".getBytes(), "key0010".getBytes());
            double costLarge = cf.rangeCost("key0000".getBytes(), "key0999".getBytes());
            assertTrue(costSmall >= 0.0, "Small range cost should be non-negative");
            assertTrue(costLarge >= 0.0, "Large range cost should be non-negative");
        }
    }
    
    @Test
    @Order(24)
    void testRangeCostNullKeys() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb22").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            assertThrows(IllegalArgumentException.class,
                () -> cf.rangeCost(null, "key".getBytes()));
            assertThrows(IllegalArgumentException.class,
                () -> cf.rangeCost("key".getBytes(), null));
            assertThrows(IllegalArgumentException.class,
                () -> cf.rangeCost(new byte[0], "key".getBytes()));
            assertThrows(IllegalArgumentException.class,
                () -> cf.rangeCost("key".getBytes(), new byte[0]));
        }
    }
    
    @Test
    @Order(25)
    void testCommitHookBasic() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb23").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            List<CommitOp[]> received = new ArrayList<>();
            AtomicLong lastSeq = new AtomicLong();
            
            cf.setCommitHook((ops, commitSeq) -> {
                received.add(ops);
                lastSeq.set(commitSeq);
                return 0;
            });
            
            // Commit a put operation
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }
            
            // Hook fires synchronously, so data is available immediately
            assertEquals(1, received.size());
            assertEquals(1, received.get(0).length);
            assertArrayEquals("key1".getBytes(), received.get(0)[0].getKey());
            assertArrayEquals("value1".getBytes(), received.get(0)[0].getValue());
            assertFalse(received.get(0)[0].isDelete());
            assertTrue(lastSeq.get() > 0);
            
            cf.clearCommitHook();
        }
    }
    
    @Test
    @Order(26)
    void testCommitHookMultipleOps() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb24").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            List<CommitOp[]> received = new ArrayList<>();
            
            cf.setCommitHook((ops, commitSeq) -> {
                received.add(ops);
                return 0;
            });
            
            // Commit multiple operations in one transaction
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.put(cf, "key2".getBytes(), "value2".getBytes());
                txn.delete(cf, "key1".getBytes());
                txn.commit();
            }
            
            // Should fire once with all operations
            assertEquals(1, received.size());
            assertEquals(3, received.get(0).length);
            
            // Last op should be a delete
            assertTrue(received.get(0)[2].isDelete());
            assertArrayEquals("key1".getBytes(), received.get(0)[2].getKey());
            
            cf.clearCommitHook();
        }
    }
    
    @Test
    @Order(27)
    void testCommitHookClear() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb25").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            List<CommitOp[]> received = new ArrayList<>();
            
            cf.setCommitHook((ops, commitSeq) -> {
                received.add(ops);
                return 0;
            });
            
            // First commit - hook should fire
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }
            assertEquals(1, received.size());
            
            // Clear the hook
            cf.clearCommitHook();
            
            // Second commit - hook should NOT fire
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key2".getBytes(), "value2".getBytes());
                txn.commit();
            }
            assertEquals(1, received.size(), "Hook should not fire after clearing");
        }
    }
    
    @Test
    @Order(28)
    void testCommitHookNullThrows() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb26").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            assertThrows(IllegalArgumentException.class, () -> cf.setCommitHook(null));
        }
    }
    
    @Test
    @Order(29)
    void testMaxMemoryUsageConfig() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb27").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .maxMemoryUsage(0)
            .build();
        
        assertEquals(0, config.getMaxMemoryUsage());
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }
            
            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, "key1".getBytes());
                assertNotNull(result);
                assertArrayEquals("value1".getBytes(), result);
            }
        }
    }
    
    @Test
    @Order(30)
    void testMultiColumnFamilyTransaction() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb28").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("users", cfConfig);
            db.createColumnFamily("orders", cfConfig);
            
            ColumnFamily usersCf = db.getColumnFamily("users");
            ColumnFamily ordersCf = db.getColumnFamily("orders");
            
            // Atomic transaction across multiple column families
            try (Transaction txn = db.beginTransaction()) {
                txn.put(usersCf, "user:1000".getBytes(), "John Doe".getBytes());
                txn.put(ordersCf, "order:5000".getBytes(), "user:1000|product:A".getBytes());
                txn.commit();
            }
            
            // Verify data in both column families
            try (Transaction txn = db.beginTransaction()) {
                byte[] user = txn.get(usersCf, "user:1000".getBytes());
                assertNotNull(user);
                assertArrayEquals("John Doe".getBytes(), user);
                
                byte[] order = txn.get(ordersCf, "order:5000".getBytes());
                assertNotNull(order);
                assertArrayEquals("user:1000|product:A".getBytes(), order);
            }
        }
    }
    
    @Test
    @Order(31)
    void testPurgeCf() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb29").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Insert data
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 100; i++) {
                    txn.put(cf, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            // Purge the column family (synchronous flush + compaction)
            cf.purge();
            
            // Verify data still accessible after purge
            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, "key50".getBytes());
                assertNotNull(result);
                assertArrayEquals("value50".getBytes(), result);
            }
        }
    }
    
    @Test
    @Order(32)
    void testPurgeDb() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb30").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("cf1", cfConfig);
            db.createColumnFamily("cf2", cfConfig);
            
            ColumnFamily cf1 = db.getColumnFamily("cf1");
            ColumnFamily cf2 = db.getColumnFamily("cf2");
            
            // Insert data into both column families
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 50; i++) {
                    txn.put(cf1, ("key" + i).getBytes(), ("value" + i).getBytes());
                    txn.put(cf2, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            // Purge entire database
            db.purge();
            
            // Verify data still accessible after purge
            try (Transaction txn = db.beginTransaction()) {
                byte[] result1 = txn.get(cf1, "key25".getBytes());
                assertNotNull(result1);
                assertArrayEquals("value25".getBytes(), result1);
                
                byte[] result2 = txn.get(cf2, "key25".getBytes());
                assertNotNull(result2);
                assertArrayEquals("value25".getBytes(), result2);
            }
        }
    }
    
    @Test
    @Order(33)
    void testSyncWal() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb31").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .syncMode(SyncMode.SYNC_NONE)
                .build();
            db.createColumnFamily("test_cf", cfConfig);
            
            ColumnFamily cf = db.getColumnFamily("test_cf");
            
            // Write some data
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }
            
            // Force WAL sync
            cf.syncWal();
            
            // Verify data accessible
            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, "key1".getBytes());
                assertNotNull(result);
                assertArrayEquals("value1".getBytes(), result);
            }
        }
    }
    
    @Test
    @Order(34)
    void testGetDbStats() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb32").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("cf1", cfConfig);
            db.createColumnFamily("cf2", cfConfig);
            
            ColumnFamily cf1 = db.getColumnFamily("cf1");
            
            // Insert some data
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 100; i++) {
                    txn.put(cf1, ("key" + i).getBytes(), ("value" + i).getBytes());
                }
                txn.commit();
            }
            
            DbStats dbStats = db.getDbStats();
            assertNotNull(dbStats);
            assertEquals(2, dbStats.getNumColumnFamilies());
            assertTrue(dbStats.getTotalMemory() > 0);
            assertTrue(dbStats.getResolvedMemoryLimit() > 0);
            assertTrue(dbStats.getMemoryPressureLevel() >= 0);
            assertTrue(dbStats.getGlobalSeq() > 0);
            assertTrue(dbStats.getTotalMemtableBytes() >= 0);
            assertTrue(dbStats.getTotalSstableCount() >= 0);
            assertTrue(dbStats.getTotalDataSizeBytes() >= 0);
        }
    }
    
    @Test
    @Order(35)
    void testGetDbStatsToString() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb33").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);
            
            DbStats dbStats = db.getDbStats();
            assertNotNull(dbStats);
            String str = dbStats.toString();
            assertTrue(str.contains("numColumnFamilies="));
            assertTrue(str.contains("totalMemory="));
        }
    }
    
    @Test
    @Order(36)
    void testUnifiedMemtableConfig() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_unified").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .unifiedMemtable(true)
            .unifiedMemtableWriteBufferSize(0)
            .unifiedMemtableSkipListMaxLevel(0)
            .unifiedMemtableSkipListProbability(0)
            .unifiedMemtableSyncMode(0)
            .unifiedMemtableSyncIntervalUs(0)
            .build();

        assertTrue(config.isUnifiedMemtable());

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            ColumnFamily cf = db.getColumnFamily("test_cf");

            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }

            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, "key1".getBytes());
                assertNotNull(result);
                assertArrayEquals("value1".getBytes(), result);
            }

            DbStats dbStats = db.getDbStats();
            assertNotNull(dbStats);
            assertTrue(dbStats.isUnifiedMemtableEnabled());
        }
    }

    @Test
    @Order(37)
    void testDeleteColumnFamily() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_delcf").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("delete_me", cfConfig);

            ColumnFamily cf = db.getColumnFamily("delete_me");
            assertNotNull(cf);

            // Insert some data first
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }

            // Delete the column family via handle
            db.deleteColumnFamily(cf);

            // Verify it's gone
            assertThrows(TidesDBException.class, () -> db.getColumnFamily("delete_me"));
        }
    }

    @Test
    @Order(38)
    void testDeleteColumnFamilyNull() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_delcf_null").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            assertThrows(IllegalArgumentException.class, () -> db.deleteColumnFamily(null));
        }
    }

    @Test
    @Order(39)
    void testIteratorKeyValue() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_kv").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            ColumnFamily cf = db.getColumnFamily("test_cf");

            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < 10; i++) {
                    String key = String.format("key%02d", i);
                    String value = "value" + i;
                    txn.put(cf, key.getBytes(), value.getBytes());
                }
                txn.commit();
            }

            // Test combined keyValue() method
            try (Transaction txn = db.beginTransaction()) {
                try (TidesDBIterator iter = txn.newIterator(cf)) {
                    iter.seekToFirst();

                    int count = 0;
                    while (iter.isValid()) {
                        KeyValue kv = iter.keyValue();
                        assertNotNull(kv);
                        assertNotNull(kv.getKey());
                        assertNotNull(kv.getValue());
                        count++;
                        iter.next();
                    }
                    assertEquals(10, count);
                }
            }
        }
    }

    @Test
    @Order(40)
    void testDbStatsUnifiedFields() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_stats_unified").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            DbStats dbStats = db.getDbStats();
            assertNotNull(dbStats);

            // With default config, unified memtable should be disabled
            assertFalse(dbStats.isUnifiedMemtableEnabled());
            assertFalse(dbStats.isObjectStoreEnabled());
            assertFalse(dbStats.isReplicaMode());
            assertTrue(dbStats.getUnifiedMemtableBytes() >= 0);
            assertTrue(dbStats.getUnifiedImmutableCount() >= 0);
            assertTrue(dbStats.getLocalCacheBytesUsed() >= 0);
            assertTrue(dbStats.getTotalUploads() >= 0);
            assertTrue(dbStats.getTotalUploadFailures() >= 0);

            // Verify toString includes new fields
            String str = dbStats.toString();
            assertTrue(str.contains("unifiedMemtableEnabled="));
            assertTrue(str.contains("objectStoreEnabled="));
            assertTrue(str.contains("replicaMode="));
        }
    }

    @Test
    @Order(41)
    void testLogLevelNoneValue() {
        assertEquals(99, LogLevel.NONE.getValue());
        assertEquals(LogLevel.NONE, LogLevel.fromValue(99));
    }

    @Test
    @Order(21)
    void testTransactionResetNullIsolation() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb19").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            ColumnFamily cf = db.getColumnFamily("test_cf");

            Transaction txn = db.beginTransaction();
            txn.put(cf, "key1".getBytes(), "value1".getBytes());
            txn.commit();

            // Null isolation level should throw IllegalArgumentException
            assertThrows(IllegalArgumentException.class, () -> txn.reset(null));

            txn.free();
        }
    }

    @Test
    @Order(42)
    void testTransactionSingleDelete() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_single_delete").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            ColumnFamily cf = db.getColumnFamily("test_cf");

            byte[] key = "single_key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "single_value".getBytes(StandardCharsets.UTF_8);

            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, key, value);
                txn.commit();
            }

            try (Transaction txn = db.beginTransaction()) {
                byte[] result = txn.get(cf, key);
                assertNotNull(result);
                assertArrayEquals(value, result);
            }

            try (Transaction txn = db.beginTransaction()) {
                txn.singleDelete(cf, key);
                txn.commit();
            }

            try (Transaction txn = db.beginTransaction()) {
                assertThrows(TidesDBException.class, () -> txn.get(cf, key));
            }
        }
    }

    @Test
    @Order(44)
    void testTombstoneCfConfigRoundTrip() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_tombstone_cfg").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .tombstoneDensityTrigger(0.5)
                .tombstoneDensityMinEntries(256)
                .build();

            db.createColumnFamily("ts_cf", cfConfig);
            ColumnFamily cf = db.getColumnFamily("ts_cf");

            ColumnFamilyConfig readback = cf.getStats().getConfig();
            assertNotNull(readback);
            assertEquals(0.5, readback.getTombstoneDensityTrigger(), 0.0);
            assertEquals(256L, readback.getTombstoneDensityMinEntries());

            // Defaults from the C library should be sensible (min entries ~= 1024)
            ColumnFamilyConfig defaults = ColumnFamilyConfig.defaultConfig();
            assertTrue(defaults.getTombstoneDensityMinEntries() > 0,
                "default tombstoneDensityMinEntries should be non-zero (sourced from C library)");
            assertTrue(defaults.getTombstoneDensityTrigger() >= 0.0
                       && defaults.getTombstoneDensityTrigger() <= 1.0,
                "default tombstoneDensityTrigger should be in [0.0, 1.0]");
        }
    }

    @Test
    @Order(45)
    void testTombstoneStatsPopulated() throws TidesDBException, InterruptedException {
        Config config = Config.builder(tempDir.resolve("testdb_tombstone_stats").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            db.createColumnFamily("ts_stats_cf", ColumnFamilyConfig.defaultConfig());
            ColumnFamily cf = db.getColumnFamily("ts_stats_cf");

            final int n = 200;
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < n; i++) {
                    txn.put(cf, ("key" + i).getBytes(StandardCharsets.UTF_8),
                                ("value" + i).getBytes(StandardCharsets.UTF_8));
                }
                txn.commit();
            }
            cf.flushMemtable();

            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < n / 2; i++) {
                    txn.delete(cf, ("key" + i).getBytes(StandardCharsets.UTF_8));
                }
                txn.commit();
            }
            cf.flushMemtable();

            // Wait for the flush to land so the stats include the tombstones
            Thread.sleep(500);

            Stats stats = cf.getStats();
            assertNotNull(stats);
            assertTrue(stats.getTotalTombstones() > 0,
                "expected total_tombstones > 0 after deletes + flush");
            assertTrue(stats.getTombstoneRatio() >= 0.0 && stats.getTombstoneRatio() <= 1.0,
                "tombstone_ratio must be within [0.0, 1.0]");
            assertTrue(stats.getMaxSstDensity() >= 0.0 && stats.getMaxSstDensity() <= 1.0,
                "max_sst_density must be within [0.0, 1.0]");
            assertTrue(stats.getMaxSstDensityLevel() >= 0,
                "max_sst_density_level must be non-negative");

            long[] perLevel = stats.getLevelTombstoneCounts();
            assertNotNull(perLevel, "level_tombstone_counts must be populated");
            assertEquals(stats.getNumLevels(), perLevel.length,
                "level_tombstone_counts length must match num_levels");
        }
    }

    @Test
    @Order(46)
    void testCompactRange() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_compact_range").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            // Small write buffer keeps each batch falling into its own SSTable
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .writeBufferSize(64 * 1024)
                .build();
            db.createColumnFamily("range_cf", cfConfig);
            ColumnFamily cf = db.getColumnFamily("range_cf");

            // Multi-batch insert + flush to spread keys across several SSTables
            for (int batch = 0; batch < 4; batch++) {
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 50; i++) {
                        int n = batch * 50 + i;
                        byte[] key = String.format("k%05d", n).getBytes(StandardCharsets.UTF_8);
                        byte[] value = ("value" + n).getBytes(StandardCharsets.UTF_8);
                        txn.put(cf, key, value);
                    }
                    txn.commit();
                }
                cf.flushMemtable();
            }

            // Narrow range compaction over a slice of the keyspace
            byte[] start = "k00050".getBytes(StandardCharsets.UTF_8);
            byte[] end   = "k00100".getBytes(StandardCharsets.UTF_8);
            cf.compactRange(start, end);

            // Both endpoints null should be rejected with INVALID_ARGS
            TidesDBException ex = assertThrows(TidesDBException.class,
                () -> cf.compactRange(null, null));
            assertEquals(-2, ex.getErrorCode(), "expected TDB_ERR_INVALID_ARGS for both-null range");

            // Both empty should also be rejected
            assertThrows(TidesDBException.class,
                () -> cf.compactRange(new byte[0], new byte[0]));

            // A key outside the compacted range must still read back unchanged
            try (Transaction txn = db.beginTransaction()) {
                byte[] outside = txn.get(cf, "k00150".getBytes(StandardCharsets.UTF_8));
                assertNotNull(outside);
                assertArrayEquals("value150".getBytes(StandardCharsets.UTF_8), outside);
            }
        }
    }

    @Test
    @Order(47)
    void testMaxConcurrentFlushes() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_max_flushes").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .maxConcurrentFlushes(1)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            db.createColumnFamily("flush_cf", ColumnFamilyConfig.defaultConfig());
            ColumnFamily cf = db.getColumnFamily("flush_cf");

            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "k".getBytes(StandardCharsets.UTF_8),
                            "v".getBytes(StandardCharsets.UTF_8));
                txn.commit();
            }
            cf.flushMemtable();
        }

        // defaultConfig() should source maxConcurrentFlushes from the C library
        Config defaults = Config.defaultConfig();
        assertTrue(defaults.getMaxConcurrentFlushes() > 0,
            "default maxConcurrentFlushes should be non-zero (sourced from tidesdb_default_config())");
    }

    @Test
    @Order(43)
    void testTransactionSingleDeleteNullArgs() throws TidesDBException {
        Config config = Config.builder(tempDir.resolve("testdb_single_delete_null").toString())
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();

        try (TidesDB db = TidesDB.open(config)) {
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
            db.createColumnFamily("test_cf", cfConfig);

            ColumnFamily cf = db.getColumnFamily("test_cf");

            try (Transaction txn = db.beginTransaction()) {
                assertThrows(IllegalArgumentException.class,
                    () -> txn.singleDelete(null, "k".getBytes()));
                assertThrows(IllegalArgumentException.class,
                    () -> txn.singleDelete(cf, null));
                assertThrows(IllegalArgumentException.class,
                    () -> txn.singleDelete(cf, new byte[0]));
            }
        }
    }
}
