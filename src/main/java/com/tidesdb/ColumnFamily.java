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

/**
 * A column family in TidesDB: an isolated key-value store within a database,
 * with its own independent configuration.
 *
 * <p>A {@code ColumnFamily} is a handle returned by
 * {@link TidesDB#getColumnFamily(String)} and is not independently closeable. It
 * is invalid once its owning database is closed or the family is dropped.
 *
 * <p>The memtable and write-ahead log are shared across every family, so
 * flushing and WAL syncing live on {@link TidesDB} rather than here.
 *
 * <p>This class is not guaranteed to be thread-safe.
 */
public class ColumnFamily {

    static {
        NativeLibrary.load();
    }

    private final long nativeHandle;
    private final String name;
    private long commitHookCtxHandle = 0;
    private final TidesDB owner;

    ColumnFamily(long nativeHandle, String name, TidesDB owner) {
        this.nativeHandle = nativeHandle;
        this.name = name;
        this.owner = owner;
    }

    /**
     * Checks that the owning database is open. Throws if the owner is closed.
     */
    void checkOwnerOpen() {
        if (owner != null && owner.isClosed()) {
            throw new IllegalStateException("TidesDB instance is closed");
        }
    }

    private long ownerHandle() {
        checkOwnerOpen();
        return owner == null ? 0 : owner.getNativeHandle();
    }

    /**
     * Returns the name of this column family.
     *
     * @return the column family name, never {@code null}
     */
    public String getName() {
        return name;
    }

    /**
     * Collects statistics for this column family.
     *
     * @return the statistics, never {@code null}
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the native stats retrieval fails, including
     *         {@link TidesDBException#ERR_LOCKED} when descriptor pressure kept a
     *         level from being read
     */
    public CfStats getStats() throws TidesDBException {
        checkOwnerOpen();
        return nativeGetStats(nativeHandle);
    }

    /**
     * Estimates the distinct key count of this column family.
     *
     * @return the estimated distinct key count
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the estimate fails, including
     *         {@link TidesDBException#ERR_LOCKED} when descriptor pressure kept a
     *         level from being read
     */
    public long estimateCardinality() throws TidesDBException {
        checkOwnerOpen();
        return nativeEstimateCardinality(nativeHandle);
    }

    /**
     * Synchronously runs one forced compaction pass on this column family,
     * merging even when no trigger is due.
     *
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException with {@link TidesDBException#ERR_LOCKED} if a
     *         compaction is already running, in which case the work asked for is
     *         usually already under way
     */
    public void compact() throws TidesDBException {
        nativeCompact(ownerHandle(), nativeHandle);
    }

    /**
     * Synchronously compacts every SSTable overlapping
     * {@code [startKey, endKey)}, merging toward the largest level affected.
     * Blocks the calling thread until the merge commits or fails.
     *
     * <p>A {@code null} or empty endpoint is unbounded on that side. Both being
     * unbounded is rejected in favour of {@link #compact()}.
     *
     * @param startKey the range start, or {@code null}/empty for unbounded
     * @param endKey the range end, or {@code null}/empty for unbounded
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the range is invalid, a compaction is already
     *         running, or the merge fails
     */
    public void compactRange(byte[] startKey, byte[] endKey) throws TidesDBException {
        nativeCompactRange(ownerHandle(), nativeHandle, startKey, endKey);
    }

    /**
     * Reports whether a compaction is in progress on this column family.
     *
     * @return {@code true} if compacting
     * @throws IllegalStateException if the owning database is closed
     */
    public boolean isCompacting() {
        checkOwnerOpen();
        return nativeIsCompacting(nativeHandle);
    }

    /**
     * Applies a new configuration to this column family at runtime. Every field
     * may change, since byte-wise key ordering keeps all SSTables mergeable. The
     * family name and id are preserved; a rename is separate.
     *
     * @param config the configuration to apply; must not be {@code null}. Its
     *        {@code name} field is ignored
     * @param persistToDisk {@code true} to persist the new config in the
     *        manifest, {@code false} to apply in memory only
     * @throws IllegalArgumentException if {@code config} is {@code null}
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the update fails
     */
    public void updateRuntimeConfig(ColumnFamilyConfig config, boolean persistToDisk)
            throws TidesDBException {
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }
        nativeUpdateRuntimeConfig(ownerHandle(), nativeHandle,
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
            config.getTombstoneDensityMinEntries(),
            persistToDisk);
    }

    /**
     * Describes the key range {@code [keyA, keyB)} for a query planner, reporting
     * both what a scan of it would cost and how many live keys it holds.
     *
     * <p>The count is memtable-aware. A range small enough to walk is counted
     * exactly and reports {@link RangeStats#isKeysExact()}; a wider one is
     * estimated from SSTable metadata without walking, so the call stays cheap
     * enough for plan time whatever the range covers.
     *
     * @param keyA the range start, inclusive; must not be {@code null} or empty
     * @param keyB the range end, exclusive; must not be {@code null} or empty
     * @return the range statistics, never {@code null}
     * @throws IllegalArgumentException if either bound is {@code null} or empty
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the call fails, including
     *         {@link TidesDBException#ERR_LOCKED} if the layout moved mid-scan
     */
    public RangeStats rangeStats(byte[] keyA, byte[] keyB) throws TidesDBException {
        if (keyA == null || keyA.length == 0) {
            throw new IllegalArgumentException("keyA cannot be null or empty");
        }
        if (keyB == null || keyB.length == 0) {
            throw new IllegalArgumentException("keyB cannot be null or empty");
        }
        return nativeRangeStats(ownerHandle(), nativeHandle, keyA, keyB);
    }

    /**
     * Sets a commit hook for this column family. The hook fires synchronously
     * after every transaction commit, receiving the full batch of committed
     * operations atomically. Keep the callback fast to avoid stalling writers.
     *
     * <p>Hooks are runtime-only and not persisted. After a database restart,
     * hooks must be re-registered by the application.
     *
     * @param hook the commit hook callback; must not be {@code null}
     * @throws IllegalArgumentException if {@code hook} is {@code null}
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the hook cannot be set
     */
    public void setCommitHook(CommitHook hook) throws TidesDBException {
        if (hook == null) {
            throw new IllegalArgumentException("Hook cannot be null, use clearCommitHook() instead");
        }
        long dbHandle = ownerHandle();
        boolean firstInstall = (commitHookCtxHandle == 0);
        commitHookCtxHandle = nativeSetCommitHook(dbHandle, nativeHandle, hook, commitHookCtxHandle);
        if (firstInstall && owner != null) {
            owner.registerHookColumnFamily(this);
        }
    }

    /**
     * Clears the commit hook for this column family. After this call, no further
     * commit callbacks fire.
     *
     * @throws IllegalStateException if the owning database is closed
     * @throws TidesDBException if the hook cannot be cleared
     */
    public void clearCommitHook() throws TidesDBException {
        long dbHandle = ownerHandle();
        commitHookCtxHandle = nativeSetCommitHook(dbHandle, nativeHandle, null, commitHookCtxHandle);
        if (owner != null) {
            owner.unregisterHookColumnFamily(this);
        }
    }

    /**
     * Best-effort hook detach during database close. The hook is removed from
     * the engine without caller interaction. Errors are swallowed.
     */
    void clearHookOnClose() {
        if (commitHookCtxHandle != 0) {
            try {
                long dbHandle = owner == null ? 0 : owner.getNativeHandle();
                nativeSetCommitHook(dbHandle, nativeHandle, null, commitHookCtxHandle);
            } catch (TidesDBException ignored) {
                // Best-effort during shutdown.
            }
            commitHookCtxHandle = 0;
        }
    }

    long getNativeHandle() {
        return nativeHandle;
    }

    @Override
    public String toString() {
        return "ColumnFamily{name='" + name + "'}";
    }

    private static native CfStats nativeGetStats(long cfHandle) throws TidesDBException;

    private static native long nativeEstimateCardinality(long cfHandle) throws TidesDBException;

    private static native void nativeCompact(long dbHandle, long cfHandle) throws TidesDBException;

    private static native void nativeCompactRange(long dbHandle, long cfHandle, byte[] startKey,
                                                  byte[] endKey) throws TidesDBException;

    private static native boolean nativeIsCompacting(long cfHandle);

    private static native void nativeUpdateRuntimeConfig(long dbHandle, long cfHandle,
        long levelSizeRatio, int minLevels, int dividingLevelOffset, boolean keepValuesInline,
        long btreeKlogBlockSize, int[] encodingPipeline, boolean enableBloomFilter, double bloomFpr,
        int defaultIsolationLevel, int l1FileCountTrigger, double tombstoneDensityTrigger,
        long tombstoneDensityMinEntries, boolean persistToDisk) throws TidesDBException;

    private static native RangeStats nativeRangeStats(long dbHandle, long cfHandle, byte[] keyA,
                                                      byte[] keyB) throws TidesDBException;

    private static native long nativeSetCommitHook(long dbHandle, long cfHandle, CommitHook hook,
                                                   long oldCtxHandle) throws TidesDBException;
}
