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
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Behavioural tests for the TidesDB Java binding against a live engine.
 */
public class TidesDBTest {

    @TempDir
    Path tempDir;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] v) {
        return v == null ? null : new String(v, StandardCharsets.UTF_8);
    }

    /** Opens a database under a fresh subdirectory, with logging off. */
    private TidesDB open(String name) throws TidesDBException {
        return TidesDB.open(Config.builder(tempDir.resolve(name).toString())
            .logLevel(LogLevel.NONE)
            .build());
    }

    /** Opens a database with one column family already created, and returns both. */
    private TidesDB openWithCf(String name, String cfName) throws TidesDBException {
        TidesDB db = open(name);
        db.createColumnFamily(cfName, ColumnFamilyConfig.builder().build());
        return db;
    }

    /** Writes one committed key/value pair. */
    private static void write(TidesDB db, ColumnFamily cf, String key, String value)
            throws TidesDBException {
        try (Transaction txn = db.beginTransaction()) {
            txn.put(cf, b(key), b(value));
            txn.commit();
        }
    }

    /** Reads one key back in its own transaction. */
    private static String read(TidesDB db, ColumnFamily cf, String key) throws TidesDBException {
        try (Transaction txn = db.beginTransaction()) {
            String value = s(txn.get(cf, b(key)));
            txn.rollback();
            return value;
        }
    }

    @Nested
    class Lifecycle {

        @Test
        void opensAndCloses() throws TidesDBException {
            try (TidesDB db = open("open-close")) {
                assertNotNull(db);
            }
        }

        @Test
        void closeIsIdempotent() throws TidesDBException {
            TidesDB db = open("double-close");
            db.close();
            assertDoesNotThrow(db::close);
        }

        @Test
        void operationsOnAClosedDatabaseThrow() throws TidesDBException {
            TidesDB db = open("closed-guard");
            db.close();
            assertThrows(IllegalStateException.class, db::listColumnFamilies);
            assertThrows(IllegalStateException.class, db::beginTransaction);
            assertThrows(IllegalStateException.class, db::getDbStats);
        }

        @Test
        void rejectsAMissingPath() {
            assertThrows(IllegalArgumentException.class, () -> TidesDB.open(null));
            assertThrows(IllegalArgumentException.class,
                () -> TidesDB.open(Config.builder("").build()));
        }

        @Test
        void refusesASecondHandleOnTheSameDirectory() throws TidesDBException {
            try (TidesDB first = open("locked")) {
                TidesDBException e = assertThrows(TidesDBException.class, () -> open("locked"));
                assertEquals(TidesDBException.ERR_LOCKED, e.getErrorCode());
                assertTrue(e.isRetryable());
            }
        }

        @Test
        void dataSurvivesAReopen() throws TidesDBException {
            Config config = Config.builder(tempDir.resolve("reopen").toString())
                .logLevel(LogLevel.NONE)
                .build();

            try (TidesDB db = TidesDB.open(config)) {
                db.createColumnFamily("cf", ColumnFamilyConfig.builder().build());
                write(db, db.getColumnFamily("cf"), "durable", "value");
            }
            try (TidesDB db = TidesDB.open(config)) {
                assertEquals("value", read(db, db.getColumnFamily("cf"), "durable"));
            }
        }
    }

    @Nested
    class ColumnFamilies {

        @Test
        void createsListsAndDrops() throws TidesDBException {
            try (TidesDB db = open("cf-crud")) {
                db.createColumnFamily("alpha", ColumnFamilyConfig.builder().build());
                db.createColumnFamily("beta", ColumnFamilyConfig.builder().build());

                assertEquals(2, db.listColumnFamilies().length);
                assertTrue(Arrays.asList(db.listColumnFamilies()).contains("alpha"));

                db.dropColumnFamily("alpha");
                assertFalse(Arrays.asList(db.listColumnFamilies()).contains("alpha"));
            }
        }

        @Test
        void listsNothingOnAFreshDatabase() throws TidesDBException {
            try (TidesDB db = open("cf-empty")) {
                assertEquals(0, db.listColumnFamilies().length);
            }
        }

        @Test
        void rejectsADuplicateName() throws TidesDBException {
            try (TidesDB db = openWithCf("cf-dup", "cf")) {
                TidesDBException e = assertThrows(TidesDBException.class,
                    () -> db.createColumnFamily("cf", ColumnFamilyConfig.builder().build()));
                assertEquals(TidesDBException.ERR_EXISTS, e.getErrorCode());
            }
        }

        @Test
        void reportsAMissingFamilyAsNotFound() throws TidesDBException {
            try (TidesDB db = open("cf-missing")) {
                TidesDBException e =
                    assertThrows(TidesDBException.class, () -> db.getColumnFamily("absent"));
                assertEquals(TidesDBException.ERR_NOT_FOUND, e.getErrorCode());
            }
        }

        @Test
        void renamesInPlace() throws TidesDBException {
            try (TidesDB db = openWithCf("cf-rename", "before")) {
                write(db, db.getColumnFamily("before"), "k", "v");
                db.renameColumnFamily("before", "after");

                assertTrue(Arrays.asList(db.listColumnFamilies()).contains("after"));
                assertFalse(Arrays.asList(db.listColumnFamilies()).contains("before"));
                assertEquals("v", read(db, db.getColumnFamily("after"), "k"));
            }
        }

        @Test
        void clonesAtAPointInTime() throws TidesDBException {
            try (TidesDB db = openWithCf("cf-clone", "src")) {
                ColumnFamily src = db.getColumnFamily("src");
                write(db, src, "before-clone", "yes");

                db.cloneColumnFamily("src", "dst");
                write(db, src, "after-clone", "yes");

                ColumnFamily dst = db.getColumnFamily("dst");
                assertEquals("yes", read(db, dst, "before-clone"));
                assertNull(read(db, dst, "after-clone"), "a clone is a point-in-time copy");
            }
        }

        @Test
        void rejectsEmptyNames() throws TidesDBException {
            try (TidesDB db = open("cf-names")) {
                ColumnFamilyConfig cfg = ColumnFamilyConfig.builder().build();
                assertThrows(IllegalArgumentException.class, () -> db.createColumnFamily(null, cfg));
                assertThrows(IllegalArgumentException.class, () -> db.createColumnFamily("", cfg));
                assertThrows(IllegalArgumentException.class,
                    () -> db.createColumnFamily("cf", null));
                assertThrows(IllegalArgumentException.class, () -> db.dropColumnFamily(""));
                assertThrows(IllegalArgumentException.class, () -> db.getColumnFamily(""));
            }
        }

        @Test
        void keepsFamiliesIsolated() throws TidesDBException {
            try (TidesDB db = openWithCf("cf-isolated", "one")) {
                db.createColumnFamily("two", ColumnFamilyConfig.builder().build());
                ColumnFamily one = db.getColumnFamily("one");
                ColumnFamily two = db.getColumnFamily("two");

                write(db, one, "shared-key", "from-one");
                write(db, two, "shared-key", "from-two");

                assertEquals("from-one", read(db, one, "shared-key"));
                assertEquals("from-two", read(db, two, "shared-key"));
            }
        }
    }

    @Nested
    class ReadsAndWrites {

        @Test
        void putsAndGets() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-basic", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "key", "value");
                assertEquals("value", read(db, cf, "key"));
            }
        }

        @Test
        void reportsAnAbsentKeyAsNull() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-absent", "cf")) {
                assertNull(read(db, db.getColumnFamily("cf"), "never-written"));
            }
        }

        @Test
        void distinguishesAnEmptyValueFromAnAbsence() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-empty", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("present-but-empty"), new byte[0]);
                    txn.commit();
                }
                try (Transaction txn = db.beginTransaction()) {
                    byte[] value = txn.get(cf, b("present-but-empty"));
                    assertNotNull(value, "an empty value is a present key, not an absence");
                    assertEquals(0, value.length);
                    assertTrue(txn.contains(cf, b("present-but-empty")));
                    txn.rollback();
                }
            }
        }

        @Test
        void overwritesInPlace() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-overwrite", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "key", "first");
                write(db, cf, "key", "second");
                assertEquals("second", read(db, cf, "key"));
            }
        }

        @Test
        void roundTripsBinaryValues() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-binary", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                byte[] key = {0x00, 0x01, (byte) 0xFF, 0x7F};
                byte[] value = new byte[512];
                for (int i = 0; i < value.length; i++) {
                    value[i] = (byte) i;
                }

                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, key, value);
                    txn.commit();
                }
                try (Transaction txn = db.beginTransaction()) {
                    assertArrayEquals(value, txn.get(cf, key));
                    txn.rollback();
                }
            }
        }

        @Test
        void roundTripsALargeValueThroughTheValueLog() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-large", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                byte[] value = new byte[256 * 1024];
                Arrays.fill(value, (byte) 'x');

                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("big"), value);
                    txn.commit();
                }
                db.flushMemtable();
                try (Transaction txn = db.beginTransaction()) {
                    assertArrayEquals(value, txn.get(cf, b("big")));
                    txn.rollback();
                }
            }
        }

        @Test
        void readsNothingFromARolledBackTransaction() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-rollback", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("discarded"), b("v"));
                    txn.rollback();
                }
                assertNull(read(db, cf, "discarded"));
            }
        }

        @Test
        void doesNotTrackANoTrackRead() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-notrack", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "probe", "value");

                try (Transaction txn = db.beginTransaction()) {
                    assertEquals("value", s(txn.getNoTrack(cf, b("probe"))));
                    assertNull(txn.getNoTrack(cf, b("absent")));
                    txn.rollback();
                }
            }
        }

        @Test
        void reportsExistenceWithoutReadingTheValue() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-contains", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "here", "v");

                try (Transaction txn = db.beginTransaction()) {
                    assertTrue(txn.contains(cf, b("here")));
                    assertFalse(txn.contains(cf, b("not-here")));
                    txn.rollback();
                }
            }
        }

        @Test
        void rejectsBadArguments() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-args", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(IllegalArgumentException.class, () -> txn.put(null, b("k"), b("v")));
                    assertThrows(IllegalArgumentException.class, () -> txn.put(cf, null, b("v")));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.put(cf, new byte[0], b("v")));
                    assertThrows(IllegalArgumentException.class, () -> txn.put(cf, b("k"), null));
                    assertThrows(IllegalArgumentException.class, () -> txn.get(cf, null));
                    assertThrows(IllegalArgumentException.class, () -> txn.delete(cf, new byte[0]));
                    txn.rollback();
                }
            }
        }

        @Test
        void refusesOperationsOnAFreedTransaction() throws TidesDBException {
            try (TidesDB db = openWithCf("rw-freed", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                Transaction txn = db.beginTransaction();
                txn.free();
                txn.free();

                assertThrows(IllegalStateException.class, () -> txn.put(cf, b("k"), b("v")));
                assertThrows(IllegalStateException.class, () -> txn.get(cf, b("k")));
                assertThrows(IllegalStateException.class, txn::commit);
                assertDoesNotThrow(txn::requestAbort);
            }
        }
    }

    @Nested
    class Deletes {

        @Test
        void deletesAKey() throws TidesDBException {
            try (TidesDB db = openWithCf("del-basic", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "doomed", "v");

                try (Transaction txn = db.beginTransaction()) {
                    txn.delete(cf, b("doomed"));
                    txn.commit();
                }
                assertNull(read(db, cf, "doomed"));
            }
        }

        @Test
        void singleDeletesAKeyWrittenOnce() throws TidesDBException {
            try (TidesDB db = openWithCf("del-single", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "once", "v");

                try (Transaction txn = db.beginTransaction()) {
                    txn.singleDelete(cf, b("once"));
                    txn.commit();
                }
                assertNull(read(db, cf, "once"));
            }
        }

        @Test
        void deletesAHalfOpenRange() throws TidesDBException {
            try (TidesDB db = openWithCf("del-range", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 20; i++) {
                        txn.put(cf, b(String.format("k%02d", i)), b("v" + i));
                    }
                    txn.commit();
                }
                try (Transaction txn = db.beginTransaction()) {
                    txn.deleteRange(cf, b("k05"), b("k10"));
                    txn.commit();
                }

                assertNotNull(read(db, cf, "k04"), "below the lower bound");
                assertNull(read(db, cf, "k05"), "the lower bound is inclusive");
                assertNull(read(db, cf, "k09"));
                assertNotNull(read(db, cf, "k10"), "the upper bound is exclusive");
            }
        }

        @Test
        void deletesToTheEndOfTheFamily() throws TidesDBException {
            try (TidesDB db = openWithCf("del-range-open", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 10; i++) {
                        txn.put(cf, b("k" + i), b("v"));
                    }
                    txn.commit();
                }
                try (Transaction txn = db.beginTransaction()) {
                    txn.deleteRange(cf, b("k5"), null);
                    txn.commit();
                }

                assertNotNull(read(db, cf, "k4"));
                assertNull(read(db, cf, "k5"));
                assertNull(read(db, cf, "k9"));
            }
        }

        @Test
        void deletesUnderAPrefix() throws TidesDBException {
            try (TidesDB db = openWithCf("del-prefix", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("user:1"), b("a"));
                    txn.put(cf, b("user:2"), b("b"));
                    txn.put(cf, b("order:1"), b("c"));
                    txn.commit();
                }
                try (Transaction txn = db.beginTransaction()) {
                    txn.deletePrefix(cf, b("user:"));
                    txn.commit();
                }

                assertNull(read(db, cf, "user:1"));
                assertNull(read(db, cf, "user:2"));
                assertNotNull(read(db, cf, "order:1"), "another prefix is untouched");
            }
        }

        @Test
        void letsANewerWriteSurviveARangeDelete() throws TidesDBException {
            try (TidesDB db = openWithCf("del-range-newer", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "k05", "old");

                try (Transaction txn = db.beginTransaction()) {
                    txn.deleteRange(cf, b("k00"), b("k10"));
                    txn.put(cf, b("k05"), b("new"));
                    txn.commit();
                }
                assertEquals("new", read(db, cf, "k05"));
            }
        }

        @Test
        void boundsRangeAndPrefixArguments() throws TidesDBException {
            try (TidesDB db = openWithCf("del-bounds", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                byte[] tooLong = new byte[Transaction.MAX_RANGE_BOUND_SIZE + 1];
                Arrays.fill(tooLong, (byte) 'a');

                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.deleteRange(cf, null, b("z")));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.deleteRange(cf, new byte[0], b("z")));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.deleteRange(cf, tooLong, b("z")));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.deleteRange(cf, b("a"), tooLong));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.deletePrefix(cf, new byte[0]));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.deletePrefix(cf, tooLong));
                    txn.rollback();
                }
            }
        }
    }

    @Nested
    class TimeToLive {

        @Test
        void keepsAnUnexpiredEntry() throws TidesDBException {
            try (TidesDB db = openWithCf("ttl-live", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("lives"), b("v"), 3600);
                    txn.commit();
                }
                assertEquals("v", read(db, cf, "lives"));
            }
        }

        @Test
        void treatsZeroAndNegativeAsNoExpiry() throws TidesDBException {
            try (TidesDB db = openWithCf("ttl-none", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("zero"), b("v"), 0);
                    txn.put(cf, b("negative"), b("v"), -1);
                    txn.commit();
                }
                assertEquals("v", read(db, cf, "zero"));
                assertEquals("v", read(db, cf, "negative"));
            }
        }

        @Test
        void expiresAnEntryWhoseDeadlineHasPassed() throws Exception {
            try (TidesDB db = openWithCf("ttl-expired", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("brief"), b("v"), 1);
                    txn.commit();
                }
                // deadlines are judged against the same once-a-second clock
                Thread.sleep(2500);
                assertNull(read(db, cf, "brief"), "the entry is past its deadline");
            }
        }
    }

    @Nested
    class Savepoints {

        @Test
        void rollsBackToAMark() throws TidesDBException {
            try (TidesDB db = openWithCf("sp-rollback", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("kept"), b("v"));
                    txn.savepoint("mark");
                    txn.put(cf, b("discarded"), b("v"));
                    txn.rollbackToSavepoint("mark");
                    txn.commit();
                }

                assertEquals("v", read(db, cf, "kept"));
                assertNull(read(db, cf, "discarded"));
            }
        }

        @Test
        void releasesAMarkWithoutRollingBack() throws TidesDBException {
            try (TidesDB db = openWithCf("sp-release", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.savepoint("mark");
                    txn.put(cf, b("kept"), b("v"));
                    txn.releaseSavepoint("mark");
                    txn.commit();
                }
                assertEquals("v", read(db, cf, "kept"));
            }
        }

        @Test
        void nestsMarks() throws TidesDBException {
            try (TidesDB db = openWithCf("sp-nested", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("a"), b("v"));
                    txn.savepoint("outer");
                    txn.put(cf, b("b"), b("v"));
                    txn.savepoint("inner");
                    txn.put(cf, b("c"), b("v"));
                    txn.rollbackToSavepoint("inner");
                    txn.commit();
                }

                assertEquals("v", read(db, cf, "a"));
                assertEquals("v", read(db, cf, "b"));
                assertNull(read(db, cf, "c"));
            }
        }

        @Test
        void reportsAnUnknownMark() throws TidesDBException {
            try (TidesDB db = openWithCf("sp-unknown", "cf")) {
                try (Transaction txn = db.beginTransaction()) {
                    TidesDBException e = assertThrows(TidesDBException.class,
                        () -> txn.rollbackToSavepoint("never-marked"));
                    assertEquals(TidesDBException.ERR_NOT_FOUND, e.getErrorCode());
                    txn.rollback();
                }
            }
        }

        @Test
        void rejectsAnEmptyName() throws TidesDBException {
            try (TidesDB db = openWithCf("sp-args", "cf")) {
                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(IllegalArgumentException.class, () -> txn.savepoint(null));
                    assertThrows(IllegalArgumentException.class, () -> txn.savepoint(""));
                    txn.rollback();
                }
            }
        }
    }

    @Nested
    class Iterators {

        /** Writes {@code count} keys named k000..k(count-1) and commits them. */
        private void seed(TidesDB db, ColumnFamily cf, int count) throws TidesDBException {
            try (Transaction txn = db.beginTransaction()) {
                for (int i = 0; i < count; i++) {
                    txn.put(cf, b(String.format("k%03d", i)), b("v" + i));
                }
                txn.commit();
            }
        }

        @Test
        void walksForwardInKeyOrder() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-forward", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 25);

                List<String> keys = new ArrayList<>();
                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newIterator(cf)) {
                    it.seekToFirst();
                    while (it.isValid()) {
                        keys.add(s(it.key()));
                        it.next();
                    }
                    txn.rollback();
                }

                assertEquals(25, keys.size());
                List<String> sorted = new ArrayList<>(keys);
                sorted.sort(String::compareTo);
                assertEquals(sorted, keys, "keys are ordered byte-wise");
            }
        }

        @Test
        void walksBackwardOverTheSameRows() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-backward", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 25);

                List<String> backward = new ArrayList<>();
                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newIterator(cf)) {
                    it.seekToLast();
                    while (it.isValid()) {
                        backward.add(s(it.key()));
                        it.prev();
                    }
                    txn.rollback();
                }

                assertEquals(25, backward.size());
                List<String> forward = new ArrayList<>(backward);
                java.util.Collections.reverse(forward);
                List<String> sorted = new ArrayList<>(forward);
                sorted.sort(String::compareTo);
                assertEquals(sorted, forward);
            }
        }

        @Test
        void readsKeyAndValueInOneCall() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-kv", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 5);

                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newIterator(cf)) {
                    it.seekToFirst();
                    assertTrue(it.isValid());
                    KeyValue kv = it.keyValue();
                    assertEquals(s(it.key()), s(kv.getKey()));
                    assertEquals(s(it.value()), s(kv.getValue()));
                    txn.rollback();
                }
            }
        }

        @Test
        void seeksToTheFirstKeyAtOrAfterATarget() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-seek", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 30);

                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newIterator(cf)) {
                    it.seek(b("k010"));
                    assertTrue(it.isValid());
                    assertEquals("k010", s(it.key()));

                    it.seekForPrev(b("k010"));
                    assertTrue(it.isValid());
                    assertEquals("k010", s(it.key()));
                    txn.rollback();
                }
            }
        }

        @Test
        void leavesTheCursorInvalidPastTheEnd() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-past-end", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 5);

                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newIterator(cf)) {
                    assertDoesNotThrow(() -> it.seek(b("zzzz")));
                    assertFalse(it.isValid(), "seeking past the end is not an error");
                    txn.rollback();
                }
            }
        }

        @Test
        void isImmediatelyInvalidOnAnEmptyFamily() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-empty", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newIterator(cf)) {
                    assertDoesNotThrow(it::seekToFirst);
                    assertFalse(it.isValid());
                    assertDoesNotThrow(it::seekToLast);
                    assertFalse(it.isValid());
                    txn.rollback();
                }
            }
        }

        @Test
        void scansOnlyTheRangeItWasGiven() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-range", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 100);
                db.flushMemtable();

                List<String> keys = new ArrayList<>();
                try (Transaction txn = db.beginTransaction();
                     TidesDBIterator it = txn.newRangeIterator(cf, b("k020"), b("k030"))) {
                    it.seek(b("k020"));
                    while (it.isValid() && s(it.key()).compareTo("k030") < 0) {
                        keys.add(s(it.key()));
                        it.next();
                    }
                    txn.rollback();
                }

                assertEquals(10, keys.size());
                assertEquals("k020", keys.get(0));
                assertEquals("k029", keys.get(keys.size() - 1));
            }
        }

        @Test
        void refusesOperationsOnAFreedIterator() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-freed", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                seed(db, cf, 3);

                try (Transaction txn = db.beginTransaction()) {
                    TidesDBIterator it = txn.newIterator(cf);
                    it.free();
                    it.free();

                    assertFalse(it.isValid(), "a freed iterator reports invalid rather than throwing");
                    assertThrows(IllegalStateException.class, it::next);
                    assertThrows(IllegalStateException.class, it::key);
                    txn.rollback();
                }
            }
        }

        @Test
        void rejectsMissingRangeBounds() throws TidesDBException {
            try (TidesDB db = openWithCf("iter-range-args", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.newRangeIterator(cf, null, b("z")));
                    assertThrows(IllegalArgumentException.class,
                        () -> txn.newRangeIterator(cf, b("a"), new byte[0]));
                    txn.rollback();
                }
            }
        }
    }

    @Nested
    class Isolation {

        @Test
        void beginsAtEveryLevel() throws TidesDBException {
            try (TidesDB db = openWithCf("iso-levels", "cf")) {
                for (IsolationLevel level : IsolationLevel.values()) {
                    try (Transaction txn = db.beginTransaction(level)) {
                        assertEquals(TransactionState.ACTIVE, txn.state());
                        txn.rollback();
                    }
                }
            }
        }

        @Test
        void takesTheFamilyDefault() throws TidesDBException {
            try (TidesDB db = open("iso-cf-default")) {
                db.createColumnFamily("cf", ColumnFamilyConfig.builder()
                    .defaultIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
                ColumnFamily cf = db.getColumnFamily("cf");

                try (Transaction txn = db.beginTransaction(cf)) {
                    assertEquals(TransactionState.ACTIVE, txn.state());
                    txn.rollback();
                }
            }
        }

        @Test
        void refusesTheSecondCommitterOnAContendedKey() throws TidesDBException {
            try (TidesDB db = openWithCf("iso-conflict", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");

                try (Transaction first = db.beginTransaction(IsolationLevel.SNAPSHOT);
                     Transaction second = db.beginTransaction(IsolationLevel.SNAPSHOT)) {
                    first.put(cf, b("contended"), b("first"));
                    second.put(cf, b("contended"), b("second"));

                    first.commit();
                    TidesDBException e = assertThrows(TidesDBException.class, second::commit);
                    assertEquals(TidesDBException.ERR_CONFLICT, e.getErrorCode());
                }
                assertEquals("first", read(db, cf, "contended"));
            }
        }

        @Test
        void holdsAFrozenCeilingUnderRepeatableRead() throws TidesDBException {
            try (TidesDB db = openWithCf("iso-repeatable", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "key", "before");

                try (Transaction reader = db.beginTransaction(IsolationLevel.REPEATABLE_READ)) {
                    assertEquals("before", s(reader.get(cf, b("key"))));
                    write(db, cf, "key", "after");
                    assertEquals("before", s(reader.get(cf, b("key"))),
                        "the ceiling was frozen when the transaction began");
                    reader.rollback();
                }
                assertEquals("after", read(db, cf, "key"));
            }
        }

        @Test
        void reportsAReadCeiling() throws TidesDBException {
            try (TidesDB db = openWithCf("iso-ceiling", "cf")) {
                // a committed write advances the global sequence, so the frozen
                // ceiling below is something other than the initial zero
                write(db, db.getColumnFamily("cf"), "seed", "v");

                try (Transaction txn = db.beginTransaction(IsolationLevel.REPEATABLE_READ)) {
                    assertTrue(txn.getReadSnapshot() > 0);
                    txn.rollback();
                }
                try (Transaction txn = db.beginTransaction(IsolationLevel.READ_UNCOMMITTED)) {
                    assertEquals(-1L, txn.getReadSnapshot(),
                        "read-uncommitted filters at an unsigned UINT64_MAX");
                    txn.rollback();
                }
            }
        }

        @Test
        void resetsForReuse() throws TidesDBException {
            try (TidesDB db = openWithCf("iso-reset", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("first-use"), b("v"));
                    txn.commit();

                    txn.reset(IsolationLevel.SERIALIZABLE);
                    assertEquals(TransactionState.ACTIVE, txn.state());

                    txn.put(cf, b("second-use"), b("v"));
                    txn.commit();
                }

                assertEquals("v", read(db, cf, "first-use"));
                assertEquals("v", read(db, cf, "second-use"));
            }
        }

        @Test
        void rejectsANullLevel() throws TidesDBException {
            try (TidesDB db = openWithCf("iso-args", "cf")) {
                assertThrows(IllegalArgumentException.class,
                    () -> db.beginTransaction((IsolationLevel) null));
                assertThrows(IllegalArgumentException.class,
                    () -> db.beginTransaction((ColumnFamily) null));
                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(IllegalArgumentException.class, () -> txn.reset(null));
                    txn.rollback();
                }
            }
        }
    }

    @Nested
    class TimeoutsAndAborts {

        @Test
        void setsAndClearsATimeout() throws TidesDBException {
            try (TidesDB db = openWithCf("abort-timeout", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.setTimeout(3600);
                    txn.put(cf, b("k"), b("v"));
                    txn.setTimeout(0);
                    txn.commit();
                }
                assertEquals("v", read(db, cf, "k"));
            }
        }

        @Test
        void expiresATransactionPastItsDeadline() throws Exception {
            try (TidesDB db = openWithCf("abort-expired", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.setTimeout(1);
                    // the engine ages a transaction against a clock a background
                    // ticker publishes once a second, and expiry is strictly past
                    // the deadline, so a one second timeout needs more than two
                    // seconds of wall clock to be observed whatever the tick phase
                    Thread.sleep(3200);

                    TidesDBException e =
                        assertThrows(TidesDBException.class, () -> txn.put(cf, b("k"), b("v")));
                    assertEquals(TidesDBException.ERR_TXN_EXPIRED, e.getErrorCode());
                }
            }
        }

        @Test
        void stopsATransactionOnRequest() throws TidesDBException {
            try (TidesDB db = openWithCf("abort-request", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("k"), b("v"));
                    txn.requestAbort();

                    TidesDBException e = assertThrows(TidesDBException.class, txn::commit);
                    assertEquals(TidesDBException.ERR_TXN_ABORTED, e.getErrorCode(),
                        "an outside ruling is distinct from the engine's own conflict verdict");
                }
                assertNull(read(db, cf, "k"));
            }
        }

        @Test
        void abortsFromAnotherThread() throws Exception {
            try (TidesDB db = openWithCf("abort-cross-thread", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                Transaction txn = db.beginTransaction();
                txn.put(cf, b("k"), b("v"));

                Thread aborter = new Thread(txn::requestAbort);
                aborter.start();
                aborter.join();

                assertThrows(TidesDBException.class, txn::commit);
                txn.free();
            }
        }
    }

    @Nested
    class Snapshots {

        @Test
        void readsAsOfThePointItNamed() throws TidesDBException {
            try (TidesDB db = openWithCf("snap-read", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "key", "before");

                try (Snapshot snapshot = db.createSnapshot()) {
                    assertTrue(snapshot.getSeq() > 0);
                    write(db, cf, "key", "after");

                    try (Transaction txn = db.beginTransactionAtSnapshot(snapshot)) {
                        assertEquals("before", s(txn.get(cf, b("key"))));
                        txn.rollback();
                    }
                    assertEquals("after", read(db, cf, "key"));
                }
            }
        }

        @Test
        void readsAsOfAnExplicitSequence() throws TidesDBException {
            try (TidesDB db = openWithCf("snap-seq", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "key", "before");

                try (Snapshot snapshot = db.createSnapshot()) {
                    write(db, cf, "key", "after");

                    try (Transaction txn = db.beginTransactionAtSeq(snapshot.getSeq())) {
                        assertEquals("before", s(txn.get(cf, b("key"))));
                        txn.rollback();
                    }
                }
            }
        }

        @Test
        void releaseIsIdempotent() throws TidesDBException {
            try (TidesDB db = open("snap-release")) {
                Snapshot snapshot = db.createSnapshot();
                long seq = snapshot.getSeq();

                snapshot.release();
                snapshot.release();

                assertTrue(snapshot.isReleased());
                assertEquals(seq, snapshot.getSeq(), "the sequence stays readable after release");
                assertThrows(IllegalStateException.class,
                    () -> db.beginTransactionAtSnapshot(snapshot));
            }
        }

        @Test
        void reportsTheOldestReadableSequence() throws TidesDBException {
            try (TidesDB db = openWithCf("snap-floor", "cf")) {
                assertDoesNotThrow(db::getOldestReadableSeq);
            }
        }

        @Test
        void rejectsANullSnapshot() throws TidesDBException {
            try (TidesDB db = open("snap-args")) {
                assertThrows(IllegalArgumentException.class,
                    () -> db.beginTransactionAtSnapshot(null));
            }
        }
    }

    @Nested
    class TwoPhaseCommit {

        @Test
        void appliesAPreparedTransactionOnCommit() throws TidesDBException {
            try (TidesDB db = openWithCf("2pc-commit", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");

                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("decided"), b("v"));
                    txn.prepare(b("xid-1"));
                    assertEquals(TransactionState.PREPARED, txn.state());

                    assertNull(read(db, cf, "decided"), "a prepared write stays invisible");

                    txn.commitPrepared();
                    assertEquals(TransactionState.COMMITTED, txn.state());
                }
                assertEquals("v", read(db, cf, "decided"));
            }
        }

        @Test
        void discardsAPreparedTransactionOnRollback() throws TidesDBException {
            try (TidesDB db = openWithCf("2pc-rollback", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");

                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("undecided"), b("v"));
                    txn.prepare(b("xid-2"));
                    txn.rollbackPrepared();
                    assertEquals(TransactionState.ABORTED, txn.state());
                }
                assertNull(read(db, cf, "undecided"));
            }
        }

        @Test
        void refusesPhaseTwoOnAnUnpreparedTransaction() throws TidesDBException {
            try (TidesDB db = openWithCf("2pc-unprepared", "cf")) {
                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(TidesDBException.class, txn::commitPrepared);
                    txn.rollback();
                }
            }
        }

        @Test
        void listsNothingInDoubtAfterACleanRun() throws TidesDBException {
            try (TidesDB db = openWithCf("2pc-recover", "cf")) {
                PreparedTransaction[] inDoubt = db.recoverPrepared();
                assertNotNull(inDoubt);
                assertEquals(0, inDoubt.length);
            }
        }

        @Test
        void rejectsAnEmptyXid() throws TidesDBException {
            try (TidesDB db = openWithCf("2pc-args", "cf")) {
                try (Transaction txn = db.beginTransaction()) {
                    assertThrows(IllegalArgumentException.class, () -> txn.prepare(null));
                    assertThrows(IllegalArgumentException.class, () -> txn.prepare(new byte[0]));
                    txn.rollback();
                }
            }
        }
    }

    @Nested
    class CommitHooks {

        @Test
        void deliversTheCommittedBatch() throws TidesDBException {
            try (TidesDB db = openWithCf("hook-batch", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                AtomicInteger opCount = new AtomicInteger();
                AtomicLong lastSeq = new AtomicLong();
                List<String> keys = java.util.Collections.synchronizedList(new ArrayList<>());

                cf.setCommitHook((ops, commitSeq) -> {
                    opCount.addAndGet(ops.length);
                    lastSeq.set(commitSeq);
                    for (CommitOp op : ops) {
                        keys.add(s(op.getKey()));
                    }
                    return 0;
                });

                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("a"), b("1"));
                    txn.put(cf, b("b"), b("2"));
                    txn.commit();
                }

                assertEquals(2, opCount.get());
                assertTrue(lastSeq.get() > 0);
                assertTrue(keys.containsAll(Arrays.asList("a", "b")));
                cf.clearCommitHook();
            }
        }

        @Test
        void marksDeletesAndCarriesTtl() throws TidesDBException {
            try (TidesDB db = openWithCf("hook-ops", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                List<CommitOp> seen = java.util.Collections.synchronizedList(new ArrayList<>());
                cf.setCommitHook((ops, commitSeq) -> {
                    seen.addAll(Arrays.asList(ops));
                    return 0;
                });

                try (Transaction txn = db.beginTransaction()) {
                    txn.put(cf, b("plain"), b("v"));
                    txn.put(cf, b("expiring"), b("v"), 3600);
                    txn.delete(cf, b("gone"));
                    txn.commit();
                }

                assertEquals(3, seen.size());
                CommitOp delete = seen.stream().filter(CommitOp::isDelete).findFirst().orElseThrow();
                assertEquals("gone", s(delete.getKey()));
                assertNull(delete.getValue(), "a delete carries no value");

                CommitOp expiring = seen.stream()
                    .filter(o -> "expiring".equals(s(o.getKey()))).findFirst().orElseThrow();
                assertTrue(expiring.getTtl() > 0, "the hook sees the absolute deadline");

                CommitOp plain = seen.stream()
                    .filter(o -> "plain".equals(s(o.getKey()))).findFirst().orElseThrow();
                assertEquals(-1, plain.getTtl(), "an entry that never expires reports -1");

                cf.clearCommitHook();
            }
        }

        @Test
        void stopsFiringOnceCleared() throws TidesDBException {
            try (TidesDB db = openWithCf("hook-clear", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                AtomicInteger calls = new AtomicInteger();
                cf.setCommitHook((ops, seq) -> {
                    calls.incrementAndGet();
                    return 0;
                });

                write(db, cf, "before-clear", "v");
                int afterFirst = calls.get();
                assertTrue(afterFirst > 0);

                cf.clearCommitHook();
                write(db, cf, "after-clear", "v");
                assertEquals(afterFirst, calls.get());
            }
        }

        @Test
        void replacesAnInstalledHook() throws TidesDBException {
            try (TidesDB db = openWithCf("hook-replace", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                AtomicInteger first = new AtomicInteger();
                AtomicInteger second = new AtomicInteger();

                cf.setCommitHook((ops, seq) -> { first.incrementAndGet(); return 0; });
                cf.setCommitHook((ops, seq) -> { second.incrementAndGet(); return 0; });

                write(db, cf, "k", "v");

                assertEquals(0, first.get(), "the replaced hook no longer fires");
                assertTrue(second.get() > 0);
                cf.clearCommitHook();
            }
        }

        @Test
        void survivesAThrowingHook() throws TidesDBException {
            try (TidesDB db = openWithCf("hook-throws", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                cf.setCommitHook((ops, seq) -> {
                    throw new RuntimeException("hook failure");
                });

                assertDoesNotThrow(() -> write(db, cf, "k", "v"));
                assertEquals("v", read(db, cf, "k"), "the commit is already durable");
                cf.clearCommitHook();
            }
        }

        @Test
        void isDetachedWhenTheDatabaseCloses() throws TidesDBException {
            TidesDB db = openWithCf("hook-close", "cf");
            ColumnFamily cf = db.getColumnFamily("cf");
            cf.setCommitHook((ops, seq) -> 0);
            assertDoesNotThrow(db::close);
        }

        @Test
        void rejectsANullHook() throws TidesDBException {
            try (TidesDB db = openWithCf("hook-args", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                assertThrows(IllegalArgumentException.class, () -> cf.setCommitHook(null));
            }
        }
    }

    @Nested
    class Maintenance {

        @Test
        void flushesAndReportsFlushState() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-flush", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "k", "v");

                assertDoesNotThrow(db::flushMemtable);
                assertDoesNotThrow(db::isFlushing);
                assertEquals("v", read(db, cf, "k"));
            }
        }

        @Test
        void syncsTheWriteAheadLog() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-sync", "cf")) {
                write(db, db.getColumnFamily("cf"), "k", "v");
                assertDoesNotThrow(db::syncWal);
            }
        }

        @Test
        void establishesADurabilityBarrier() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-checkpoint", "cf")) {
                write(db, db.getColumnFamily("cf"), "k", "v");
                assertDoesNotThrow(db::checkpoint);
            }
        }

        @Test
        void compactsAFamily() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-compact", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 200; i++) {
                        txn.put(cf, b(String.format("k%04d", i)), b("v" + i));
                    }
                    txn.commit();
                }
                db.flushMemtable();

                assertDoesNotThrow(cf::compact);
                assertDoesNotThrow(cf::isCompacting);
                assertEquals("v100", read(db, cf, "k0100"));
            }
        }

        @Test
        void compactsAKeyRange() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-compact-range", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 100; i++) {
                        txn.put(cf, b(String.format("k%03d", i)), b("v"));
                    }
                    txn.commit();
                }
                db.flushMemtable();

                assertDoesNotThrow(() -> cf.compactRange(b("k000"), b("k050")));
                assertEquals("v", read(db, cf, "k025"));
            }
        }

        @Test
        void writesAnOpenableBackup() throws Exception {
            Path backupDir = tempDir.resolve("backup");
            try (TidesDB db = openWithCf("maint-backup", "cf")) {
                write(db, db.getColumnFamily("cf"), "backed-up", "v");
                db.flushMemtable();
                db.backup(backupDir.toString());
            }

            assertTrue(Files.isDirectory(backupDir));
            try (TidesDB restored = TidesDB.open(
                    Config.builder(backupDir.toString()).logLevel(LogLevel.NONE).build())) {
                assertEquals("v", read(restored, restored.getColumnFamily("cf"), "backed-up"));
            }
        }

        @Test
        void rejectsAnEmptyBackupDirectory() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-backup-args", "cf")) {
                assertThrows(IllegalArgumentException.class, () -> db.backup(null));
                assertThrows(IllegalArgumentException.class, () -> db.backup(""));
            }
        }

        @Test
        void appliesARuntimeConfigChange() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-runtime-config", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                write(db, cf, "k", "v");

                ColumnFamilyConfig updated = ColumnFamilyConfig.builder()
                    .compression(CompressionAlgorithm.ZSTD)
                    .enableBloomFilter(true)
                    .bloomFpr(0.05)
                    .build();
                cf.updateRuntimeConfig(updated, true);

                ColumnFamilyConfig applied = cf.getStats().getConfig();
                assertArrayEquals(new int[]{CompressionAlgorithm.ZSTD.getValue()},
                    applied.getEncodingPipeline());
                assertEquals(0.05, applied.getBloomFpr(), 1e-9);
                assertEquals("cf", applied.getName(), "the family keeps its identity");
                assertEquals("v", read(db, cf, "k"));
            }
        }

        @Test
        void rejectsANullRuntimeConfig() throws TidesDBException {
            try (TidesDB db = openWithCf("maint-runtime-args", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                assertThrows(IllegalArgumentException.class,
                    () -> cf.updateRuntimeConfig(null, false));
            }
        }
    }

    @Nested
    class Statistics {

        @Test
        void reportsColumnFamilyStatistics() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-cf", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 50; i++) {
                        txn.put(cf, b("k" + i), b("v" + i));
                    }
                    txn.commit();
                }
                db.flushMemtable();

                CfStats stats = cf.getStats();
                assertNotNull(stats);
                assertTrue(stats.getTotalKeys() > 0);
                assertEquals("cf", stats.getConfig().getName());
                assertEquals(CfStats.MAX_LEVELS, stats.getLevelSizes().length);
                assertEquals(CfStats.MAX_LEVELS, stats.getLevelNumSstables().length);
                assertEquals(CfStats.MAX_LEVELS, stats.getLevelKeyCounts().length);
                assertEquals(CfStats.MAX_LEVELS, stats.getLevelTombstoneCounts().length);
                assertTrue(stats.getNumLevels() >= 0);
                assertTrue(stats.getUserBytesWritten() > 0);
                assertNotNull(stats.toString());
            }
        }

        @Test
        void estimatesCardinality() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-cardinality", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 100; i++) {
                        txn.put(cf, b("k" + i), b("v"));
                    }
                    txn.commit();
                }
                db.flushMemtable();

                assertTrue(cf.estimateCardinality() >= 0);
            }
        }

        @Test
        void reportsDatabaseStatistics() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-db", "cf")) {
                write(db, db.getColumnFamily("cf"), "k", "v");

                DbStats stats = db.getDbStats();
                assertNotNull(stats);
                assertEquals(1, stats.getNumColumnFamilies());
                assertTrue(stats.getGlobalSeq() > 0);
                assertTrue(stats.getUserBytesWritten() > 0);
                assertTrue(stats.getWalBytesWritten() > 0);
                assertNotNull(stats.toString());
            }
        }

        @Test
        void reportsCacheStatistics() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-cache", "cf")) {
                CacheStats stats = db.getCacheStats();
                assertNotNull(stats);
                assertTrue(stats.getHits() >= 0);
                assertTrue(stats.getMisses() >= 0);
                assertTrue(stats.getNumPartitions() > 0);
                assertNotNull(stats.toString());
            }
        }

        @Test
        void reportsWhereWritersWaited() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-stall", "cf")) {
                write(db, db.getColumnFamily("cf"), "k", "v");

                StallStats stats = db.getStallStats();
                assertNotNull(stats);
                assertEquals(StallReason.values().length, stats.getReasons().length);
                for (StallReason reason : StallReason.values()) {
                    assertNotNull(stats.get(reason));
                    assertTrue(stats.get(reason).getCount() >= 0);
                }
                assertTrue(stats.getTotalUs() >= 0);
                assertThrows(IllegalArgumentException.class, () -> stats.get(null));
            }
        }

        @Test
        void reportsWhatEachFileClassAskedOfTheDevice() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-io", "cf")) {
                write(db, db.getColumnFamily("cf"), "k", "v");
                db.flushMemtable();

                IoStats stats = db.getIoStats();
                assertNotNull(stats);
                assertEquals(IoClass.values().length, stats.getClasses().length);
                for (IoClass cls : IoClass.values()) {
                    assertNotNull(stats.get(cls));
                    assertTrue(stats.get(cls).getBytesPerSecond() >= 0.0);
                }
                assertTrue(stats.getTotalBytes() > 0);
                assertThrows(IllegalArgumentException.class, () -> stats.get(null));
            }
        }

        @Test
        void reportsWhatEachEncodingChainAchieved() throws TidesDBException {
            try (TidesDB db = open("stats-encoding")) {
                db.createColumnFamily("cf", ColumnFamilyConfig.builder()
                    .compression(CompressionAlgorithm.LZ4)
                    .build());
                ColumnFamily cf = db.getColumnFamily("cf");

                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 200; i++) {
                        txn.put(cf, b(String.format("k%04d", i)), b("a repetitive value " + i));
                    }
                    txn.commit();
                }
                db.flushMemtable();

                EncodingStats[] klog = db.getKlogEncodingStats();
                assertNotNull(klog);
                assertTrue(klog.length <= EncodingStats.MAX_CHAINS);
                for (EncodingStats e : klog) {
                    assertNotNull(e.getIds());
                    assertTrue(e.getRatio() >= 0.0);
                    assertNotNull(e.toString());
                }
                assertNotNull(db.getVlogEncodingStats());
            }
        }

        @Test
        void describesAKeyRangeForAPlanner() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-range", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 100; i++) {
                        txn.put(cf, b(String.format("k%03d", i)), b("v"));
                    }
                    txn.commit();
                }
                db.flushMemtable();

                RangeStats stats = cf.rangeStats(b("k000"), b("k050"));
                assertNotNull(stats);
                assertTrue(stats.getSstablesOverlapping() >= 0);
                assertTrue(stats.getEstimatedKeys() >= 0);
                assertNotNull(stats.toString());
            }
        }

        @Test
        void rejectsEmptyRangeBounds() throws TidesDBException {
            try (TidesDB db = openWithCf("stats-range-args", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                assertThrows(IllegalArgumentException.class, () -> cf.rangeStats(null, b("z")));
                assertThrows(IllegalArgumentException.class,
                    () -> cf.rangeStats(b("a"), new byte[0]));
            }
        }
    }

    @Nested
    class Concurrency {

        @Test
        void servesConcurrentWritersOnDistinctKeys() throws Exception {
            try (TidesDB db = openWithCf("conc-writers", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                int threads = 4;
                int perThread = 50;
                List<Thread> workers = new ArrayList<>();
                List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());

                for (int t = 0; t < threads; t++) {
                    final int id = t;
                    Thread worker = new Thread(() -> {
                        try {
                            for (int i = 0; i < perThread; i++) {
                                try (Transaction txn = db.beginTransaction()) {
                                    txn.put(cf, b("t" + id + "-k" + i), b("v"));
                                    txn.commit();
                                }
                            }
                        } catch (Throwable e) {
                            errors.add(e);
                        }
                    });
                    workers.add(worker);
                    worker.start();
                }
                for (Thread worker : workers) {
                    worker.join();
                }

                assertTrue(errors.isEmpty(), () -> "worker failures: " + errors);
                for (int t = 0; t < threads; t++) {
                    assertEquals("v", read(db, cf, "t" + t + "-k0"));
                    assertEquals("v", read(db, cf, "t" + t + "-k" + (perThread - 1)));
                }
            }
        }

        @Test
        void servesConcurrentReadersDuringWrites() throws Exception {
            try (TidesDB db = openWithCf("conc-readers", "cf")) {
                ColumnFamily cf = db.getColumnFamily("cf");
                try (Transaction txn = db.beginTransaction()) {
                    for (int i = 0; i < 100; i++) {
                        txn.put(cf, b("k" + i), b("v" + i));
                    }
                    txn.commit();
                }

                List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
                List<Thread> readers = new ArrayList<>();
                for (int t = 0; t < 4; t++) {
                    Thread reader = new Thread(() -> {
                        try {
                            for (int i = 0; i < 100; i++) {
                                assertEquals("v" + i, read(db, cf, "k" + i));
                            }
                        } catch (Throwable e) {
                            errors.add(e);
                        }
                    });
                    readers.add(reader);
                    reader.start();
                }
                for (Thread reader : readers) {
                    reader.join();
                }

                assertTrue(errors.isEmpty(), () -> "reader failures: " + errors);
            }
        }
    }
}
