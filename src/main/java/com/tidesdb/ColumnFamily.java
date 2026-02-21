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
 * Represents a column family in TidesDB.
 * Column families are isolated key-value stores with independent configuration.
 */
public class ColumnFamily {
    
    static {
        NativeLibrary.load();
    }
    
    private final long nativeHandle;
    private final String name;
    private long commitHookCtxHandle = 0;
    
    ColumnFamily(long nativeHandle, String name) {
        this.nativeHandle = nativeHandle;
        this.name = name;
    }
    
    /**
     * Gets the name of this column family.
     *
     * @return the column family name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Retrieves statistics about this column family.
     *
     * @return column family statistics
     * @throws TidesDBException if the stats cannot be retrieved
     */
    public Stats getStats() throws TidesDBException {
        return nativeGetStats(nativeHandle);
    }
    
    /**
     * Manually triggers compaction for this column family.
     *
     * @throws TidesDBException if compaction fails
     */
    public void compact() throws TidesDBException {
        nativeCompact(nativeHandle);
    }
    
    /**
     * Manually triggers memtable flush for this column family.
     *
     * @throws TidesDBException if flush fails
     */
    public void flushMemtable() throws TidesDBException {
        nativeFlushMemtable(nativeHandle);
    }
    
    /**
     * Checks if a flush operation is currently in progress for this column family.
     *
     * @return true if flushing is in progress
     */
    public boolean isFlushing() {
        return nativeIsFlushing(nativeHandle);
    }
    
    /**
     * Checks if a compaction operation is currently in progress for this column family.
     *
     * @return true if compaction is in progress
     */
    public boolean isCompacting() {
        return nativeIsCompacting(nativeHandle);
    }
    
    /**
     * Updates runtime-safe configuration settings for this column family.
     * Configuration changes are applied to new operations only.
     * 
     * <p>Updatable settings (safe to change at runtime):</p>
     * <ul>
     *   <li>writeBufferSize - Memtable flush threshold</li>
     *   <li>skipListMaxLevel - Skip list level for new memtables</li>
     *   <li>skipListProbability - Skip list probability for new memtables</li>
     *   <li>bloomFPR - False positive rate for new SSTables</li>
     *   <li>indexSampleRatio - Index sampling ratio for new SSTables</li>
     *   <li>syncMode - Durability mode</li>
     *   <li>syncIntervalUs - Sync interval in microseconds</li>
     * </ul>
     *
     * @param config the new configuration
     * @param persistToDisk if true, saves changes to config.ini
     * @throws TidesDBException if the update fails
     */
    public void updateRuntimeConfig(ColumnFamilyConfig config, boolean persistToDisk) throws TidesDBException {
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }
        nativeUpdateRuntimeConfig(nativeHandle,
            config.getWriteBufferSize(),
            config.getSkipListMaxLevel(),
            config.getSkipListProbability(),
            config.getBloomFPR(),
            config.getIndexSampleRatio(),
            config.getSyncMode().getValue(),
            config.getSyncIntervalUs(),
            persistToDisk);
    }
    
    /**
     * Estimates the computational cost of iterating between two keys in this column family.
     * The returned value is an opaque double — meaningful only for comparison with other
     * values from the same method. Uses only in-memory metadata and performs no disk I/O.
     * Key order does not matter — the method normalizes the range internally.
     *
     * @param keyA first key (bound of range)
     * @param keyB second key (bound of range)
     * @return estimated traversal cost (higher = more expensive), 0.0 if no overlapping data
     * @throws TidesDBException if the estimation fails
     */
    public double rangeCost(byte[] keyA, byte[] keyB) throws TidesDBException {
        if (keyA == null || keyA.length == 0) {
            throw new IllegalArgumentException("keyA cannot be null or empty");
        }
        if (keyB == null || keyB.length == 0) {
            throw new IllegalArgumentException("keyB cannot be null or empty");
        }
        return nativeRangeCost(nativeHandle, keyA, keyB);
    }
    
    /**
     * Sets a commit hook (Change Data Capture) for this column family.
     * The hook fires synchronously after every transaction commit, receiving the full
     * batch of committed operations atomically. Keep the callback fast to avoid
     * stalling writers.
     *
     * <p>Hooks are runtime-only and not persisted. After a database restart,
     * hooks must be re-registered by the application.</p>
     *
     * @param hook the commit hook callback
     * @throws TidesDBException if the hook cannot be set
     */
    public void setCommitHook(CommitHook hook) throws TidesDBException {
        if (hook == null) {
            throw new IllegalArgumentException("Hook cannot be null, use clearCommitHook() instead");
        }
        commitHookCtxHandle = nativeSetCommitHook(nativeHandle, hook, commitHookCtxHandle);
    }
    
    /**
     * Clears the commit hook for this column family.
     * After this call, no further commit callbacks will fire.
     *
     * @throws TidesDBException if the hook cannot be cleared
     */
    public void clearCommitHook() throws TidesDBException {
        commitHookCtxHandle = nativeSetCommitHook(nativeHandle, null, commitHookCtxHandle);
    }
    
    long getNativeHandle() {
        return nativeHandle;
    }
    
    private static native Stats nativeGetStats(long handle) throws TidesDBException;
    private static native void nativeCompact(long handle) throws TidesDBException;
    private static native void nativeFlushMemtable(long handle) throws TidesDBException;
    private static native boolean nativeIsFlushing(long handle);
    private static native boolean nativeIsCompacting(long handle);
    private static native void nativeUpdateRuntimeConfig(long handle, long writeBufferSize,
        int skipListMaxLevel, float skipListProbability, double bloomFPR, int indexSampleRatio,
        int syncMode, long syncIntervalUs, boolean persistToDisk) throws TidesDBException;
    private static native double nativeRangeCost(long handle, byte[] keyA, byte[] keyB) throws TidesDBException;
    private static native long nativeSetCommitHook(long handle, CommitHook hook, long oldCtxHandle) throws TidesDBException;
}
