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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the value types and enums, which need no open database.
 */
public class PojoAndEnumTest {

    @Nested
    class Enums {

        @Test
        void logLevelMapsToTheNativeOrdering() {
            assertEquals(0, LogLevel.NONE.getValue());
            assertEquals(1, LogLevel.TRACE.getValue());
            assertEquals(2, LogLevel.INFO.getValue());
            assertEquals(3, LogLevel.WARN.getValue());
            assertEquals(4, LogLevel.ERROR.getValue());

            for (LogLevel level : LogLevel.values()) {
                assertEquals(level, LogLevel.fromValue(level.getValue()));
            }
            assertThrows(IllegalArgumentException.class, () -> LogLevel.fromValue(99));
        }

        @Test
        void isolationLevelRunsWeakestToStrongest() {
            assertEquals(0, IsolationLevel.READ_UNCOMMITTED.getValue());
            assertEquals(1, IsolationLevel.READ_COMMITTED.getValue());
            assertEquals(2, IsolationLevel.REPEATABLE_READ.getValue());
            assertEquals(3, IsolationLevel.SNAPSHOT.getValue());
            assertEquals(4, IsolationLevel.SERIALIZABLE.getValue());

            for (IsolationLevel level : IsolationLevel.values()) {
                assertEquals(level, IsolationLevel.fromValue(level.getValue()));
            }
            assertThrows(IllegalArgumentException.class, () -> IsolationLevel.fromValue(5));
        }

        @Test
        void syncModeMapsToTheNativeValues() {
            assertEquals(0, SyncMode.SYNC_NONE.getValue());
            assertEquals(1, SyncMode.SYNC_FULL.getValue());
            assertEquals(2, SyncMode.SYNC_INTERVAL.getValue());

            for (SyncMode mode : SyncMode.values()) {
                assertEquals(mode, SyncMode.fromValue(mode.getValue()));
            }
            assertThrows(IllegalArgumentException.class, () -> SyncMode.fromValue(3));
        }

        @Test
        void compressionAlgorithmValuesAreTheEncodingIds() {
            assertEquals(0, CompressionAlgorithm.NONE.getValue());
            assertEquals(1, CompressionAlgorithm.SNAPPY.getValue());
            assertEquals(2, CompressionAlgorithm.LZ4.getValue());
            assertEquals(3, CompressionAlgorithm.ZSTD.getValue());
            assertEquals(4, CompressionAlgorithm.LZ4_FAST.getValue());

            for (CompressionAlgorithm algo : CompressionAlgorithm.values()) {
                assertEquals(algo, CompressionAlgorithm.fromValue(algo.getValue()));
            }
            assertThrows(IllegalArgumentException.class, () -> CompressionAlgorithm.fromValue(5));
        }

        @Test
        void transactionStateCoversTheLifecycle() {
            assertEquals(0, TransactionState.ACTIVE.getValue());
            assertEquals(1, TransactionState.PREPARED.getValue());
            assertEquals(2, TransactionState.COMMITTED.getValue());
            assertEquals(3, TransactionState.ABORTED.getValue());

            for (TransactionState state : TransactionState.values()) {
                assertEquals(state, TransactionState.fromValue(state.getValue()));
            }
            assertThrows(IllegalArgumentException.class, () -> TransactionState.fromValue(4));
        }

        @Test
        void stallReasonValuesAreContiguousIndices() {
            StallReason[] reasons = StallReason.values();
            for (int i = 0; i < reasons.length; i++) {
                assertEquals(i, reasons[i].getValue(), "the value is the index into StallStats");
                assertEquals(reasons[i], StallReason.fromValue(i));
            }
            assertThrows(IllegalArgumentException.class,
                () -> StallReason.fromValue(reasons.length));
        }

        @Test
        void ioClassValuesAreContiguousIndices() {
            IoClass[] classes = IoClass.values();
            for (int i = 0; i < classes.length; i++) {
                assertEquals(i, classes[i].getValue(), "the value is the index into IoStats");
                assertEquals(classes[i], IoClass.fromValue(i));
            }
            assertThrows(IllegalArgumentException.class, () -> IoClass.fromValue(classes.length));
        }

        @Test
        void stallReasonAndIoClassCarryNativeNames() {
            for (StallReason reason : StallReason.values()) {
                assertNotNull(reason.getNativeName());
                assertFalse(reason.getNativeName().isEmpty());
            }
            for (IoClass cls : IoClass.values()) {
                assertNotNull(cls.getNativeName());
                assertFalse(cls.getNativeName().isEmpty());
            }
        }
    }

    @Nested
    class DatabaseConfig {

        @Test
        void carriesTheNativeDefaults() {
            Config config = Config.defaultConfig("/tmp/tidesdb-config-test");

            assertEquals("/tmp/tidesdb-config-test", config.getDbPath());
            assertTrue(config.getNumFlushThreads() > 0);
            assertTrue(config.getNumCompactionThreads() > 0);
            assertTrue(config.getBlockCacheSize() > 0);
            assertTrue(config.getMaxOpenSSTables() > 0);
            assertTrue(config.getMemtableWriteBufferSize() > 0);
            assertTrue(config.getValueSeparationThreshold() > 0);
            assertTrue(config.getVlogSegmentSize() > 0);
            assertNotNull(config.getLogLevel());
            assertNotNull(config.getMemtableSyncMode());
            assertNotNull(config.toString());
        }

        @Test
        void roundTripsThroughItsBuilder() {
            Config config = Config.builder("/tmp/db")
                .numFlushThreads(3)
                .numCompactionThreads(5)
                .logLevel(LogLevel.WARN)
                .blockCacheSize(1024)
                .maxOpenSSTables(64)
                .logToFile(true)
                .logTruncationAt(2048)
                .memtableWriteBufferSize(4096)
                .memtableSkipListMaxLevel(16)
                .memtableSkipListProbability(0.5f)
                .memtableSyncMode(SyncMode.SYNC_FULL)
                .memtableSyncIntervalUs(1000)
                .valueSeparationThreshold(512)
                .vlogSegmentSize(8192)
                .memtableL0QueueStallThreshold(8)
                .memtableIdleFlushSeconds(30)
                .txnTimeoutSeconds(60)
                .build();

            assertEquals("/tmp/db", config.getDbPath());
            assertEquals(3, config.getNumFlushThreads());
            assertEquals(5, config.getNumCompactionThreads());
            assertEquals(LogLevel.WARN, config.getLogLevel());
            assertEquals(1024, config.getBlockCacheSize());
            assertEquals(64, config.getMaxOpenSSTables());
            assertTrue(config.isLogToFile());
            assertEquals(2048, config.getLogTruncationAt());
            assertEquals(4096, config.getMemtableWriteBufferSize());
            assertEquals(16, config.getMemtableSkipListMaxLevel());
            assertEquals(0.5f, config.getMemtableSkipListProbability());
            assertEquals(SyncMode.SYNC_FULL, config.getMemtableSyncMode());
            assertEquals(1000, config.getMemtableSyncIntervalUs());
            assertEquals(512, config.getValueSeparationThreshold());
            assertEquals(8192, config.getVlogSegmentSize());
            assertEquals(8, config.getMemtableL0QueueStallThreshold());
            assertEquals(30, config.getMemtableIdleFlushSeconds());
            assertEquals(60, config.getTxnTimeoutSeconds());

            Config copy = config.toBuilder().build();
            assertEquals(config.toString(), copy.toString());
        }

        @Test
        void rejectsNegativeAndNullFields() {
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder(null).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").numFlushThreads(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").numCompactionThreads(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").blockCacheSize(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").maxOpenSSTables(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").logTruncationAt(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableWriteBufferSize(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").valueSeparationThreshold(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").vlogSegmentSize(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableL0QueueStallThreshold(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableIdleFlushSeconds(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").txnTimeoutSeconds(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").logLevel(null).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableSyncMode(null).build());
        }

        @Test
        void boundsSkipListProbability() {
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableSkipListProbability(-0.1f).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableSkipListProbability(1.1f).build());
            assertThrows(IllegalArgumentException.class,
                () -> Config.builder("/tmp/db").memtableSkipListProbability(Float.NaN).build());
            assertDoesNotThrow(
                () -> Config.builder("/tmp/db").memtableSkipListProbability(0.25f).build());
        }
    }

    @Nested
    class FamilyConfig {

        @Test
        void carriesTheNativeDefaults() {
            ColumnFamilyConfig config = ColumnFamilyConfig.defaultConfig();

            assertTrue(config.getLevelSizeRatio() > 0);
            assertTrue(config.getBtreeKlogBlockSize() > 0);
            assertNotNull(config.getEncodingPipeline());
            assertNotNull(config.getDefaultIsolationLevel());
            assertNotNull(config.getName());
            assertNotNull(config.toString());
        }

        @Test
        void roundTripsThroughItsBuilder() {
            ColumnFamilyConfig config = ColumnFamilyConfig.builder()
                .levelSizeRatio(12)
                .minLevels(2)
                .dividingLevelOffset(1)
                .keepValuesInline(true)
                .btreeKlogBlockSize(8192)
                .encodingPipeline(CompressionAlgorithm.LZ4, CompressionAlgorithm.ZSTD)
                .enableBloomFilter(true)
                .bloomFpr(0.02)
                .defaultIsolationLevel(IsolationLevel.SERIALIZABLE)
                .l1FileCountTrigger(6)
                .tombstoneDensityTrigger(0.4)
                .tombstoneDensityMinEntries(1000)
                .build();

            assertEquals(12, config.getLevelSizeRatio());
            assertEquals(2, config.getMinLevels());
            assertEquals(1, config.getDividingLevelOffset());
            assertTrue(config.isKeepValuesInline());
            assertEquals(8192, config.getBtreeKlogBlockSize());
            assertArrayEquals(new int[]{2, 3}, config.getEncodingPipeline());
            assertTrue(config.isEnableBloomFilter());
            assertEquals(0.02, config.getBloomFpr(), 1e-9);
            assertEquals(IsolationLevel.SERIALIZABLE, config.getDefaultIsolationLevel());
            assertEquals(6, config.getL1FileCountTrigger());
            assertEquals(0.4, config.getTombstoneDensityTrigger(), 1e-9);
            assertEquals(1000, config.getTombstoneDensityMinEntries());

            ColumnFamilyConfig copy = config.toBuilder().build();
            assertEquals(config.toString(), copy.toString());
        }

        @Test
        void treatsNoCompressionAsAnEmptyPipeline() {
            assertArrayEquals(new int[0], ColumnFamilyConfig.builder()
                .compression(CompressionAlgorithm.NONE).build().getEncodingPipeline());
            assertArrayEquals(new int[]{CompressionAlgorithm.SNAPPY.getValue()},
                ColumnFamilyConfig.builder()
                    .compression(CompressionAlgorithm.SNAPPY).build().getEncodingPipeline());
        }

        @Test
        void acceptsRawEncodingIds() {
            ColumnFamilyConfig config =
                ColumnFamilyConfig.builder().encodingPipelineIds(2, 3).build();
            assertArrayEquals(new int[]{2, 3}, config.getEncodingPipeline());

            assertArrayEquals(new int[0],
                ColumnFamilyConfig.builder().encodingPipelineIds().build().getEncodingPipeline());
        }

        @Test
        void copiesThePipelineOnTheWayInAndOut() {
            int[] source = {2, 3};
            ColumnFamilyConfig config =
                ColumnFamilyConfig.builder().encodingPipelineIds(source).build();

            source[0] = 99;
            assertArrayEquals(new int[]{2, 3}, config.getEncodingPipeline(),
                "the builder took a copy");

            int[] returned = config.getEncodingPipeline();
            returned[0] = 99;
            assertArrayEquals(new int[]{2, 3}, config.getEncodingPipeline(),
                "the getter returned a copy");
        }

        @Test
        void boundsThePipelineLength() {
            int[] tooMany = new int[ColumnFamilyConfig.MAX_ENCODING_PIPELINE + 1];
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().encodingPipelineIds(tooMany).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().encodingPipelineIds(256).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().encodingPipelineIds(-1).build());
        }

        @Test
        void rejectsInvalidFields() {
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().bloomFpr(-0.1).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().bloomFpr(1.0).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().bloomFpr(Double.NaN).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().tombstoneDensityTrigger(1.5).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().minLevels(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().l1FileCountTrigger(-1).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().defaultIsolationLevel(null).build());
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().compression(null).build());
        }

        @Test
        void boundsTheName() {
            StringBuilder tooLong = new StringBuilder();
            for (int i = 0; i < ColumnFamilyConfig.MAX_NAME_LENGTH; i++) {
                tooLong.append('x');
            }
            assertThrows(IllegalArgumentException.class,
                () -> ColumnFamilyConfig.builder().name(tooLong.toString()).build());
            assertEquals("", ColumnFamilyConfig.builder().name(null).build().getName());
        }
    }

    @Nested
    class ValueTypes {

        @Test
        void commitOpCarriesItsFields() {
            byte[] key = "k".getBytes(StandardCharsets.UTF_8);
            byte[] value = "v".getBytes(StandardCharsets.UTF_8);

            CommitOp put = new CommitOp(key, value, 12345L, false);
            assertArrayEquals(key, put.getKey());
            assertArrayEquals(value, put.getValue());
            assertEquals(12345L, put.getTtl());
            assertFalse(put.isDelete());

            CommitOp delete = new CommitOp(key, null, -1L, true);
            assertNull(delete.getValue());
            assertTrue(delete.isDelete());
        }

        @Test
        void keyValueCarriesBothHalves() {
            byte[] key = {1, 2};
            byte[] value = {3, 4};
            KeyValue kv = new KeyValue(key, value);
            assertArrayEquals(key, kv.getKey());
            assertArrayEquals(value, kv.getValue());
        }

        @Test
        void cacheStatsCarriesItsFields() {
            CacheStats stats = new CacheStats(true, 10, 2048, 7, 3, 0.7, 8);
            assertTrue(stats.isEnabled());
            assertEquals(10, stats.getTotalEntries());
            assertEquals(2048, stats.getTotalBytes());
            assertEquals(7, stats.getHits());
            assertEquals(3, stats.getMisses());
            assertEquals(0.7, stats.getHitRate(), 1e-9);
            assertEquals(8, stats.getNumPartitions());
            assertNotNull(stats.toString());
        }

        @Test
        void stallStatsIsIndexedByReason() {
            StallStat[] reasons = new StallStat[StallReason.values().length];
            for (int i = 0; i < reasons.length; i++) {
                reasons[i] = new StallStat(i, i * 10L, i * 100L);
            }
            StallStats stats = new StallStats(reasons);

            assertEquals(0, stats.get(StallReason.WAL_APPEND).getCount());
            assertEquals(StallReason.MANIFEST_COMMIT.getValue(),
                stats.get(StallReason.MANIFEST_COMMIT).getCount());
            assertEquals(reasons.length, stats.getReasons().length);
            assertNotNull(stats.toString());

            long expectedTotal = 0;
            for (StallStat r : reasons) {
                expectedTotal += r.getTotalUs();
            }
            assertEquals(expectedTotal, stats.getTotalUs());
        }

        @Test
        void stallStatsRejectsAMissizedArray() {
            assertThrows(IllegalArgumentException.class, () -> new StallStats(null));
            assertThrows(IllegalArgumentException.class,
                () -> new StallStats(new StallStat[]{new StallStat(0, 0, 0)}));
        }

        @Test
        void ioStatsIsIndexedByClass() {
            IoStat[] classes = new IoStat[IoClass.values().length];
            for (int i = 0; i < classes.length; i++) {
                classes[i] = new IoStat(i, i * 1000L, i * 10L, i);
            }
            IoStats stats = new IoStats(classes);

            assertEquals(0, stats.get(IoClass.SSTABLE).getOps());
            assertEquals(IoClass.VLOG.getValue(), stats.get(IoClass.VLOG).getOps());
            assertEquals(classes.length, stats.getClasses().length);
            assertNotNull(stats.toString());
        }

        @Test
        void ioStatComputesThroughput() {
            assertEquals(0.0, new IoStat(0, 0, 0, 0).getBytesPerSecond(), 1e-9);
            assertEquals(1_000_000.0, new IoStat(1, 1_000_000, 1_000_000, 1).getBytesPerSecond(),
                1e-6);
        }

        @Test
        void ioStatsRejectsAMissizedArray() {
            assertThrows(IllegalArgumentException.class, () -> new IoStats(null));
            assertThrows(IllegalArgumentException.class,
                () -> new IoStats(new IoStat[]{new IoStat(0, 0, 0, 0)}));
        }

        @Test
        void encodingStatsComputesItsRatio() {
            EncodingStats stats = new EncodingStats(new int[]{2}, 1000, 250, 4);
            assertArrayEquals(new int[]{2}, stats.getIds());
            assertEquals(1000, stats.getLogicalBytes());
            assertEquals(250, stats.getStoredBytes());
            assertEquals(4, stats.getItemCount());
            assertEquals(4.0, stats.getRatio(), 1e-9);
            assertNotNull(stats.toString());

            assertEquals(0.0, new EncodingStats(null, 100, 0, 0).getRatio(), 1e-9,
                "nothing stored means no ratio to report");
            assertArrayEquals(new int[0], new EncodingStats(null, 0, 0, 0).getIds());
        }

        @Test
        void rangeStatsCarriesItsFields() {
            RangeStats exact = new RangeStats(3, 500, true);
            assertEquals(3, exact.getSstablesOverlapping());
            assertEquals(500, exact.getEstimatedKeys());
            assertTrue(exact.isKeysExact());
            assertNotNull(exact.toString());

            assertFalse(new RangeStats(9, 100_000, false).isKeysExact());
        }

        @Test
        void preparedTransactionCopiesItsXid() {
            byte[] xid = {1, 2, 3};
            PreparedTransaction prepared = new PreparedTransaction(null, xid);

            xid[0] = 99;
            assertArrayEquals(new byte[]{1, 2, 3}, prepared.getXid(),
                "the constructor holds the array it was given, and the getter copies it");

            byte[] returned = prepared.getXid();
            returned[0] = 99;
            assertArrayEquals(new byte[]{1, 2, 3}, prepared.getXid());

            assertNull(prepared.getTransaction());
            assertArrayEquals(new byte[0], new PreparedTransaction(null, null).getXid());
            assertNotNull(prepared.toString());
        }

        @Test
        void cfStatsCopiesItsLevelArrays() {
            long[] sizes = new long[CfStats.MAX_LEVELS];
            sizes[0] = 100;
            CfStats stats = new CfStats(1, null, sizes, new int[CfStats.MAX_LEVELS],
                new long[CfStats.MAX_LEVELS], new long[CfStats.MAX_LEVELS], 10, 100, 1.0, 2.0, 1.0,
                5, 2, 2.0, 0, 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

            sizes[0] = 999;
            assertEquals(100, stats.getLevelSizes()[0], "the constructor took a copy");

            long[] returned = stats.getLevelSizes();
            returned[0] = 999;
            assertEquals(100, stats.getLevelSizes()[0], "the getter returned a copy");

            assertEquals(1, stats.getNumLevels());
            assertEquals(10, stats.getTotalKeys());
            assertNotNull(stats.toString());
        }
    }

    @Nested
    class Exceptions {

        @Test
        void carriesACodeAndADescription() {
            TidesDBException e = new TidesDBException("boom", TidesDBException.ERR_IO);
            assertEquals(TidesDBException.ERR_IO, e.getErrorCode());
            assertEquals("boom", e.getMessage());
            assertEquals("I/O error", e.getErrorMessage());
        }

        @Test
        void defaultsToUnknown() {
            assertEquals(TidesDBException.ERR_UNKNOWN,
                new TidesDBException("boom").getErrorCode());
            assertEquals(TidesDBException.ERR_UNKNOWN,
                new TidesDBException("boom", new RuntimeException()).getErrorCode());
        }

        @Test
        void keepsItsCause() {
            RuntimeException cause = new RuntimeException("root");
            TidesDBException e = new TidesDBException("boom", TidesDBException.ERR_IO, cause);
            assertSame(cause, e.getCause());
            assertEquals(TidesDBException.ERR_IO, e.getErrorCode());
        }

        @Test
        void describesEveryCode() {
            int[] codes = {
                TidesDBException.ERR_SUCCESS, TidesDBException.ERR_MEMORY,
                TidesDBException.ERR_INVALID_ARGS, TidesDBException.ERR_NOT_FOUND,
                TidesDBException.ERR_IO, TidesDBException.ERR_CORRUPTION,
                TidesDBException.ERR_EXISTS, TidesDBException.ERR_CONFLICT,
                TidesDBException.ERR_TOO_LARGE, TidesDBException.ERR_MEMORY_LIMIT,
                TidesDBException.ERR_INVALID_DB, TidesDBException.ERR_UNKNOWN,
                TidesDBException.ERR_LOCKED, TidesDBException.ERR_READONLY,
                TidesDBException.ERR_TXN_EXPIRED, TidesDBException.ERR_NO_SPACE,
                TidesDBException.ERR_TXN_ABORTED, TidesDBException.ERR_TOO_OLD};

            for (int code : codes) {
                String message = new TidesDBException("x", code).getErrorMessage();
                assertNotNull(message);
                assertFalse(message.isEmpty());
            }
            assertEquals("unknown error", new TidesDBException("x", -999).getErrorMessage());
        }

        @Test
        void identifiesTheRetryableCode() {
            assertTrue(new TidesDBException("x", TidesDBException.ERR_LOCKED).isRetryable());
            assertFalse(new TidesDBException("x", TidesDBException.ERR_CONFLICT).isRetryable());
            assertFalse(new TidesDBException("x", TidesDBException.ERR_IO).isRetryable());
        }

        @Test
        void errorCodesMatchTheNativeNumbering() {
            assertEquals(0, TidesDBException.ERR_SUCCESS);
            assertEquals(-1, TidesDBException.ERR_MEMORY);
            assertEquals(-2, TidesDBException.ERR_INVALID_ARGS);
            assertEquals(-3, TidesDBException.ERR_NOT_FOUND);
            assertEquals(-4, TidesDBException.ERR_IO);
            assertEquals(-5, TidesDBException.ERR_CORRUPTION);
            assertEquals(-6, TidesDBException.ERR_EXISTS);
            assertEquals(-7, TidesDBException.ERR_CONFLICT);
            assertEquals(-8, TidesDBException.ERR_TOO_LARGE);
            assertEquals(-9, TidesDBException.ERR_MEMORY_LIMIT);
            assertEquals(-10, TidesDBException.ERR_INVALID_DB);
            assertEquals(-11, TidesDBException.ERR_UNKNOWN);
            assertEquals(-12, TidesDBException.ERR_LOCKED);
            assertEquals(-13, TidesDBException.ERR_READONLY);
            assertEquals(-14, TidesDBException.ERR_TXN_EXPIRED);
            assertEquals(-15, TidesDBException.ERR_NO_SPACE);
            assertEquals(-16, TidesDBException.ERR_TXN_ABORTED);
            assertEquals(-17, TidesDBException.ERR_TOO_OLD);
        }
    }

    @Nested
    class NativeStatics {

        @Test
        void reportsWhichCompressionBackendsAreLinkedIn() {
            assertTrue(TidesDB.isCompressionAvailable(CompressionAlgorithm.NONE),
                "no-compression is always available");
            for (CompressionAlgorithm algo : CompressionAlgorithm.values()) {
                assertDoesNotThrow(() -> TidesDB.isCompressionAvailable(algo));
            }
            assertThrows(IllegalArgumentException.class,
                () -> TidesDB.isCompressionAvailable(null));
        }

        @Test
        void describesResultCodes() {
            assertNotNull(TidesDB.strerror(TidesDBException.ERR_SUCCESS));
            assertNotNull(TidesDB.strerror(TidesDBException.ERR_NOT_FOUND));
            assertNotNull(TidesDB.strerror(-999), "an unrecognised code still describes itself");
        }

        @Test
        void reportsTheOpenFileCeiling() {
            assertTrue(TidesDB.raiseOpenFileLimit(0) > 0);
        }

        @Test
        void loadsTheNativeLibrary() {
            NativeLibrary.load();
            assertTrue(NativeLibrary.isLoaded());
        }
    }
}
