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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Main entry point for a TidesDB database. This class wraps the native TidesDB
 * library through JNI and owns the underlying database handle.
 *
 * <p>{@code TidesDB} implements {@link java.io.Closeable} and should be used with
 * try-with-resources. Callers must close any {@link TidesDBIterator},
 * {@link Transaction}, and {@link Snapshot} derived from it before closing the
 * database. Closing an already-closed instance is a no-op.
 *
 * <p>Operations on a closed instance throw {@link IllegalStateException}.
 *
 * <p>The memtable, write-ahead log, block cache, and value log are
 * database-level and shared by every column family, so the operations that act
 * on them — {@link #flushMemtable()}, {@link #syncWal()}, {@link #checkpoint()} —
 * live here rather than on {@link ColumnFamily}.
 *
 * <p>This class is not guaranteed to be thread-safe.
 */
public class TidesDB implements Closeable {

    static {
        NativeLibrary.load();
    }

    private long nativeHandle;
    private boolean closed = false;
    private final Set<ColumnFamily> hookBearingCFs = new HashSet<>();

    private TidesDB(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /**
     * Opens or creates a database at the configured path, recovering the
     * manifest, SSTables, and write-ahead log, and starting the worker pool.
     *
     * @param config the database configuration; must not be {@code null}
     * @return a new TidesDB instance
     * @throws IllegalArgumentException if {@code config} is {@code null} or its
     *         {@code dbPath} is {@code null} or empty
     * @throws TidesDBException if the native database cannot be opened, which
     *         includes {@link TidesDBException#ERR_LOCKED} when another handle
     *         already holds the directory, from this process or any other
     */
    public static TidesDB open(Config config) throws TidesDBException {
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }
        if (config.getDbPath() == null || config.getDbPath().isEmpty()) {
            throw new IllegalArgumentException("Database path cannot be null or empty");
        }

        long handle = nativeOpen(
            config.getDbPath(),
            config.getNumFlushThreads(),
            config.getNumCompactionThreads(),
            config.getLogLevel().getValue(),
            config.getBlockCacheSize(),
            config.getMaxOpenSSTables(),
            config.isLogToFile(),
            config.getLogTruncationAt(),
            config.getMemtableWriteBufferSize(),
            config.getMemtableSkipListMaxLevel(),
            config.getMemtableSkipListProbability(),
            config.getMemtableSyncMode().getValue(),
            config.getMemtableSyncIntervalUs(),
            config.getValueSeparationThreshold(),
            config.getVlogSegmentSize(),
            config.getMemtableL0QueueStallThreshold(),
            config.getMemtableIdleFlushSeconds(),
            config.getTxnTimeoutSeconds()
        );

        return new TidesDB(handle);
    }

    /**
     * Reports whether this build of the native library can actually use a
     * compression algorithm. Every algorithm is always named by the enum, but a
     * backend is only linked in when its build option was set, so a caller
     * choosing one for a column family's encoding pipeline asks here first
     * rather than discovering it when a node fails to decode.
     *
     * <p>{@link CompressionAlgorithm#NONE} is always available.
     *
     * @param algorithm the algorithm to query; must not be {@code null}
     * @return {@code true} if this build can compress and decompress with it
     */
    public static boolean isCompressionAvailable(CompressionAlgorithm algorithm) {
        if (algorithm == null) {
            throw new IllegalArgumentException("Compression algorithm cannot be null");
        }
        return nativeCompressionAvailable(algorithm.getValue());
    }

    /**
     * Returns the native library's short description of a result code. An
     * unrecognised code describes itself as unknown rather than failing.
     *
     * @param code any {@code ERR_*} value from {@link TidesDBException}
     * @return the description, never {@code null}
     */
    public static String strerror(int code) {
        return nativeStrerror(code);
    }

    /**
     * Raises this process's open-file ceiling toward {@code desired} descriptors
     * so a database can keep more SSTables open. The engine sizes
     * {@link Config#getMaxOpenSSTables()} to fit this at open time, so call it
     * <strong>before</strong> {@link #open(Config)}. This is an explicit, opt-in
     * operator action; TidesDB never raises the limit itself.
     *
     * <p>On POSIX systems this raises the {@code RLIMIT_NOFILE} soft limit toward
     * the hard limit; on Windows it raises the CRT stdio cap (max 8192). A failed
     * or partial raise is non-fatal.
     *
     * @param desired target descriptor count; values &le; 0 just report the
     *        current ceiling
     * @return the open-file ceiling in effect after the attempt
     */
    public static long raiseOpenFileLimit(long desired) {
        return nativeRaiseOpenFileLimit(desired);
    }

    /**
     * Flushes, quiesces the workers, and releases all native resources.
     *
     * <p>This method is idempotent; subsequent calls are no-ops. After closing,
     * all other operations on this instance throw {@link IllegalStateException}.
     *
     * <p>Callers should close all {@link TidesDBIterator}, {@link Transaction},
     * and {@link Snapshot} instances before calling this method.
     *
     * <p>The shutdown path reports no status, so an I/O error while writing the
     * last of the data is logged rather than thrown. A caller that needs its data
     * on the device asks for that before closing, with {@link #syncWal()},
     * {@link #checkpoint()}, or by running under {@link SyncMode#SYNC_FULL}.
     */
    @Override
    public void close() {
        if (!closed && nativeHandle != 0) {
            closed = true;

            // Drain all registered hooks. Loop because a concurrent setCommitHook
            // may have passed checkOwnerOpen before closed=true but hasn't
            // registered yet; its registration is rejected (closed is true), but a
            // retry loop guarantees the set is empty before calling nativeClose.
            while (true) {
                List<ColumnFamily> batch;
                synchronized (hookBearingCFs) {
                    if (hookBearingCFs.isEmpty()) break;
                    batch = new ArrayList<>(hookBearingCFs);
                    hookBearingCFs.clear();
                }
                for (ColumnFamily cf : batch) {
                    cf.clearHookOnClose();
                }
            }

            nativeClose(nativeHandle);
            nativeHandle = 0;
        }
    }

    /* ===== column families ===== */

    /**
     * Creates a column family and registers it in the manifest.
     *
     * @param name the column family name; must not be {@code null} or empty, and
     *        must be shorter than {@link ColumnFamilyConfig#MAX_NAME_LENGTH}
     * @param config the column family configuration; must not be {@code null}.
     *        Its {@code name} field is ignored, since the {@code name} argument
     *        is authoritative
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty,
     *         or {@code config} is {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native column family cannot be created
     */
    public void createColumnFamily(String name, ColumnFamilyConfig config) throws TidesDBException {
        checkNotClosed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Column family name cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("Column family config cannot be null");
        }

        nativeCreateColumnFamily(nativeHandle, name,
            config.getLevelSizeRatio(),
            config.getMinLevels(),
            config.getDividingLevelOffset(),
            config.isKeepValuesInline(),
            config.getBtreeKlogBlockSize(),
            config.getEncodingPipeline(),
            config.isEnableBloomFilter(),
            config.getBloomFpr(),
            config.getDefaultIsolationLevel().getValue(),
            config.getL1FileCountTrigger(),
            config.getTombstoneDensityTrigger(),
            config.getTombstoneDensityMinEntries()
        );
    }

    /**
     * Drops a column family by name, deleting its SSTables and manifest records.
     * Waits out any compaction already running on the family, so a heavily
     * compacting family takes longer to drop.
     *
     * <p>This destroys data and cannot be undone. Any {@link ColumnFamily} handle
     * held for the family is invalid afterwards.
     *
     * @param name the column family name; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native column family cannot be dropped
     */
    public void dropColumnFamily(String name) throws TidesDBException {
        checkNotClosed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Column family name cannot be null or empty");
        }
        nativeDropColumnFamily(nativeHandle, name);
    }

    /**
     * Atomically renames a column family. The family is claimed against the
     * compaction scheduler, the whole database memtable is flushed — it is
     * shared, so this is not just this family's data — and new flushes are frozen
     * while the directory moves and the family reloads.
     *
     * @param oldName the current column family name
     * @param newName the new column family name
     * @throws IllegalArgumentException if either name is {@code null} or empty
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the rename fails, including
     *         {@link TidesDBException#ERR_LOCKED} when the family stayed under
     *         compaction for the whole quiesce window
     */
    public void renameColumnFamily(String oldName, String newName) throws TidesDBException {
        checkNotClosed();
        if (oldName == null || oldName.isEmpty()) {
            throw new IllegalArgumentException("Old column family name cannot be null or empty");
        }
        if (newName == null || newName.isEmpty()) {
            throw new IllegalArgumentException("New column family name cannot be null or empty");
        }
        nativeRenameColumnFamily(nativeHandle, oldName, newName);
    }

    /**
     * Clones a column family to a new name, copying its SSTables. The source is
     * claimed against the compaction scheduler and the whole database memtable is
     * flushed first, so the copy carries everything written before the call
     * rather than only what had already reached disk.
     *
     * <p>The result is a point-in-time copy: later writes to the source do not
     * appear in it.
     *
     * @param sourceName the source column family name
     * @param destName the new cloned column family name
     * @throws IllegalArgumentException if either name is {@code null} or empty
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the clone fails
     */
    public void cloneColumnFamily(String sourceName, String destName) throws TidesDBException {
        checkNotClosed();
        if (sourceName == null || sourceName.isEmpty()) {
            throw new IllegalArgumentException("Source column family name cannot be null or empty");
        }
        if (destName == null || destName.isEmpty()) {
            throw new IllegalArgumentException("Destination column family name cannot be null or empty");
        }
        nativeCloneColumnFamily(nativeHandle, sourceName, destName);
    }

    /**
     * Looks up a column family handle by name.
     *
     * @param name the column family name; must not be {@code null} or empty
     * @return the column family handle, never {@code null}
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException with {@link TidesDBException#ERR_NOT_FOUND} if no
     *         such column family exists
     */
    public ColumnFamily getColumnFamily(String name) throws TidesDBException {
        checkNotClosed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Column family name cannot be null or empty");
        }
        long cfHandle = nativeGetColumnFamily(nativeHandle, name);
        return new ColumnFamily(cfHandle, name, this);
    }

    /**
     * Lists the names of every column family.
     *
     * @return the column family names, never {@code null}; empty when there are
     *         none
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native list operation fails
     */
    public String[] listColumnFamilies() throws TidesDBException {
        checkNotClosed();
        return nativeListColumnFamilies(nativeHandle);
    }

    /* ===== transactions ===== */

    /**
     * Begins a transaction at the database default isolation level.
     *
     * <p>Close the returned transaction before closing this database.
     *
     * @return a new transaction
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native transaction cannot be started
     */
    public Transaction beginTransaction() throws TidesDBException {
        checkNotClosed();
        return new Transaction(nativeBeginTransaction(nativeHandle));
    }

    /**
     * Begins a transaction at an explicit isolation level.
     *
     * <p>Close the returned transaction before closing this database.
     *
     * @param isolationLevel the isolation level; must not be {@code null}
     * @return a new transaction
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native transaction cannot be started
     */
    public Transaction beginTransaction(IsolationLevel isolationLevel) throws TidesDBException {
        checkNotClosed();
        if (isolationLevel == null) {
            throw new IllegalArgumentException("Isolation level cannot be null");
        }
        return new Transaction(
            nativeBeginTransactionWithIsolation(nativeHandle, isolationLevel.getValue()));
    }

    /**
     * Begins a transaction at the given column family's default isolation level.
     *
     * @param cf the column family whose default isolation to use; must not be
     *        {@code null}
     * @return a new transaction
     * @throws IllegalArgumentException if {@code cf} is {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native transaction cannot be started
     */
    public Transaction beginTransaction(ColumnFamily cf) throws TidesDBException {
        checkNotClosed();
        if (cf == null) {
            throw new IllegalArgumentException("Column family cannot be null");
        }
        return new Transaction(nativeBeginTransactionCf(nativeHandle, cf.getNativeHandle()));
    }

    /**
     * Names the database as it stands now, so it can be read again later.
     *
     * <p>The snapshot holds the reclamation floor at its own sequence for as long
     * as it lives. Release it as soon as the point in time is no longer wanted.
     *
     * @return a new snapshot
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native snapshot cannot be created
     */
    public Snapshot createSnapshot() throws TidesDBException {
        checkNotClosed();
        return new Snapshot(nativeSnapshotCreate(nativeHandle));
    }

    /**
     * Begins a transaction whose reads resolve as of a snapshot rather than as of
     * now: the same keys, the same families, answered as they stood when the
     * snapshot was taken.
     *
     * <p>The snapshot must outlive the transaction, because it is what holds the
     * floor under the versions being read.
     *
     * @param snapshot the snapshot to read at; must not be {@code null}
     * @return a new transaction
     * @throws IllegalArgumentException if {@code snapshot} is {@code null}
     * @throws IllegalStateException if this database is closed or the snapshot
     *         has been released
     * @throws TidesDBException if the native transaction cannot be started
     */
    public Transaction beginTransactionAtSnapshot(Snapshot snapshot) throws TidesDBException {
        checkNotClosed();
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot cannot be null");
        }
        return new Transaction(
            nativeBeginTransactionAtSnapshot(nativeHandle, snapshot.getNativeHandle()));
    }

    /**
     * Begins a transaction reading as of an explicit sequence, for a point in
     * time no snapshot was taken at — a sequence read back from
     * {@link Transaction#getReadSnapshot()}, or one recorded elsewhere.
     *
     * <p>It refuses rather than approximates. A sequence is readable only while
     * something holds the reclamation floor under it: an open transaction, or a
     * snapshot taken in advance. Once a collection has run past that sequence the
     * call fails with {@link TidesDBException#ERR_TOO_OLD} and reads nothing.
     *
     * @param seq the sequence to read at
     * @return a new transaction
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException with {@link TidesDBException#ERR_TOO_OLD} when a
     *         collection has already run below {@code seq}
     */
    public Transaction beginTransactionAtSeq(long seq) throws TidesDBException {
        checkNotClosed();
        return new Transaction(nativeBeginTransactionAtSeq(nativeHandle, seq));
    }

    /**
     * Returns the oldest sequence {@link #beginTransactionAtSeq(long)} will still
     * accept, which is the highest reclamation floor any collection has taken. It
     * only ever rises, and a snapshot or an open transaction is what keeps it
     * from rising past a point still wanted.
     *
     * @return the oldest readable sequence
     * @throws IllegalStateException if this database is closed
     */
    public long getOldestReadableSeq() {
        checkNotClosed();
        return nativeOldestReadableSeq(nativeHandle);
    }

    /**
     * Lists the transactions that were durably prepared before the last shutdown
     * and never committed or rolled back, so a coordinator can finish deciding
     * them. One that was decided in the log is settled during open and never
     * appears here.
     *
     * <p>The caller owns every returned {@link Transaction} and must resolve it
     * with {@link Transaction#commitPrepared()} or
     * {@link Transaction#rollbackPrepared()}, then free it.
     *
     * @return the in-doubt transactions, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the recovery scan fails
     */
    public PreparedTransaction[] recoverPrepared() throws TidesDBException {
        checkNotClosed();
        return nativeRecoverPrepared(nativeHandle);
    }

    /* ===== maintenance ===== */

    /**
     * Synchronously rotates and flushes the shared memtable to SSTables, waiting
     * a bounded time for the immutable queue to drain.
     *
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException with {@link TidesDBException#ERR_LOCKED} if the
     *         queue had not drained when the wait expired, which says flush is
     *         not keeping up rather than that anything is wrong with the call
     */
    public void flushMemtable() throws TidesDBException {
        checkNotClosed();
        nativeFlushMemtable(nativeHandle);
    }

    /**
     * Reports whether the memtable is currently flushing or rotating.
     *
     * @return {@code true} if flushing
     * @throws IllegalStateException if this database is closed
     */
    public boolean isFlushing() {
        checkNotClosed();
        return nativeIsFlushing(nativeHandle);
    }

    /**
     * Forces an fsync of the write-ahead log. Useful for explicit durability
     * control under {@link SyncMode#SYNC_NONE} or {@link SyncMode#SYNC_INTERVAL}.
     *
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the sync fails
     */
    public void syncWal() throws TidesDBException {
        checkNotClosed();
        nativeSyncWal(nativeHandle);
    }

    /**
     * Writes a consistent, directly-openable copy of the database into
     * {@code dir}: flushes the memtable, then copies the manifest, the shared
     * value log, and every SSTable it references at a single manifest snapshot
     * while compaction is held off, so the copy references no file that a merge
     * could delete mid-copy.
     *
     * @param dir the destination directory, created if absent; must not be
     *        {@code null} or empty
     * @throws IllegalArgumentException if {@code dir} is {@code null} or empty
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the backup fails, including
     *         {@link TidesDBException#ERR_LOCKED} if a family stayed under
     *         compaction for the whole freeze window
     */
    public void backup(String dir) throws TidesDBException {
        checkNotClosed();
        if (dir == null || dir.isEmpty()) {
            throw new IllegalArgumentException("Backup directory cannot be null or empty");
        }
        nativeBackup(nativeHandle, dir);
    }

    /**
     * Establishes a durability barrier in the live database: flushes the memtable
     * to L1, then forces the value log, the write-ahead log, and the manifest to
     * disk regardless of the configured sync mode.
     *
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the checkpoint fails, including
     *         {@link TidesDBException#ERR_LOCKED} if the flush it begins with
     *         could not drain the immutable queue
     */
    public void checkpoint() throws TidesDBException {
        checkNotClosed();
        nativeCheckpoint(nativeHandle);
    }

    /* ===== statistics ===== */

    /**
     * Collects database-level statistics.
     *
     * @return the statistics, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native stats retrieval fails
     */
    public DbStats getDbStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetDbStats(nativeHandle);
    }

    /**
     * Collects block-cache statistics.
     *
     * @return the statistics, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native stats retrieval fails
     */
    public CacheStats getCacheStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetCacheStats(nativeHandle);
    }

    /**
     * Collects where writers have been made to wait. A write latency tail is
     * answerable from this alone: compare each reason's maximum against the tail
     * you measured, and its total against the others.
     *
     * @return the statistics, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native stats retrieval fails
     */
    public StallStats getStallStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetStallStats(nativeHandle);
    }

    /**
     * Collects what each class of file asked of the device. This is the other
     * half of {@link #getStallStats()}: that says writers waited on the log, this
     * says whether the device was the reason.
     *
     * @return the statistics, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native stats retrieval fails
     */
    public IoStats getIoStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetIoStats(nativeHandle);
    }

    /**
     * Reports what each encoding chain achieved on the key logs, one entry per
     * chain the live SSTables were written with. A table written before a family
     * changed its codec keeps reporting what its own pipeline achieved.
     *
     * @return the per-chain statistics, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native stats retrieval fails
     */
    public EncodingStats[] getKlogEncodingStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetKlogEncodingStats(nativeHandle);
    }

    /**
     * Reports what each encoding chain achieved on the separated values, read
     * back from the chain each value records with itself.
     *
     * @return the per-chain statistics, never {@code null}
     * @throws IllegalStateException if this database is closed
     * @throws TidesDBException if the native stats retrieval fails
     */
    public EncodingStats[] getVlogEncodingStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetVlogEncodingStats(nativeHandle);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("TidesDB instance is closed");
        }
    }

    /**
     * Reports whether this database instance has been closed.
     */
    boolean isClosed() {
        return closed;
    }

    /**
     * Registers a column family that has an installed commit hook.
     */
    void registerHookColumnFamily(ColumnFamily cf) {
        synchronized (hookBearingCFs) {
            if (!closed) hookBearingCFs.add(cf);
        }
    }

    /**
     * Unregisters a column family whose commit hook has been cleared.
     */
    void unregisterHookColumnFamily(ColumnFamily cf) {
        synchronized (hookBearingCFs) {
            hookBearingCFs.remove(cf);
        }
    }

    long getNativeHandle() {
        return nativeHandle;
    }

    private static native long nativeOpen(String dbPath, int numFlushThreads,
                                          int numCompactionThreads, int logLevel,
                                          long blockCacheSize, long maxOpenSSTables,
                                          boolean logToFile, long logTruncationAt,
                                          long memtableWriteBufferSize,
                                          int memtableSkipListMaxLevel,
                                          float memtableSkipListProbability,
                                          int memtableSyncMode, long memtableSyncIntervalUs,
                                          long valueSeparationThreshold, long vlogSegmentSize,
                                          int memtableL0QueueStallThreshold,
                                          int memtableIdleFlushSeconds,
                                          long txnTimeoutSeconds) throws TidesDBException;

    private static native void nativeClose(long handle);

    private static native boolean nativeCompressionAvailable(int algorithm);

    private static native String nativeStrerror(int code);

    private static native long nativeRaiseOpenFileLimit(long desired);

    private static native void nativeCreateColumnFamily(long handle, String name,
        long levelSizeRatio, int minLevels, int dividingLevelOffset, boolean keepValuesInline,
        long btreeKlogBlockSize, int[] encodingPipeline, boolean enableBloomFilter, double bloomFpr,
        int defaultIsolationLevel, int l1FileCountTrigger, double tombstoneDensityTrigger,
        long tombstoneDensityMinEntries) throws TidesDBException;

    private static native void nativeDropColumnFamily(long handle, String name) throws TidesDBException;

    private static native void nativeRenameColumnFamily(long handle, String oldName, String newName) throws TidesDBException;

    private static native void nativeCloneColumnFamily(long handle, String sourceName, String destName) throws TidesDBException;

    private static native long nativeGetColumnFamily(long handle, String name) throws TidesDBException;

    private static native String[] nativeListColumnFamilies(long handle) throws TidesDBException;

    private static native long nativeBeginTransaction(long handle) throws TidesDBException;

    private static native long nativeBeginTransactionWithIsolation(long handle, int isolationLevel) throws TidesDBException;

    private static native long nativeBeginTransactionCf(long handle, long cfHandle) throws TidesDBException;

    private static native long nativeSnapshotCreate(long handle) throws TidesDBException;

    private static native long nativeBeginTransactionAtSnapshot(long handle, long snapshotHandle) throws TidesDBException;

    private static native long nativeBeginTransactionAtSeq(long handle, long seq) throws TidesDBException;

    private static native long nativeOldestReadableSeq(long handle);

    private static native PreparedTransaction[] nativeRecoverPrepared(long handle) throws TidesDBException;

    private static native void nativeFlushMemtable(long handle) throws TidesDBException;

    private static native boolean nativeIsFlushing(long handle);

    private static native void nativeSyncWal(long handle) throws TidesDBException;

    private static native void nativeBackup(long handle, String dir) throws TidesDBException;

    private static native void nativeCheckpoint(long handle) throws TidesDBException;

    private static native DbStats nativeGetDbStats(long handle) throws TidesDBException;

    private static native CacheStats nativeGetCacheStats(long handle) throws TidesDBException;

    private static native StallStats nativeGetStallStats(long handle) throws TidesDBException;

    private static native IoStats nativeGetIoStats(long handle) throws TidesDBException;

    private static native EncodingStats[] nativeGetKlogEncodingStats(long handle) throws TidesDBException;

    private static native EncodingStats[] nativeGetVlogEncodingStats(long handle) throws TidesDBException;
}
