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
            
            // First commit — hook should fire
            try (Transaction txn = db.beginTransaction()) {
                txn.put(cf, "key1".getBytes(), "value1".getBytes());
                txn.commit();
            }
            assertEquals(1, received.size());
            
            // Clear the hook
            cf.clearCommitHook();
            
            // Second commit — hook should NOT fire
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
}
