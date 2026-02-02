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

/**
 * TidesDB is the main database class providing access to TidesDB functionality.
 * This class wraps the native TidesDB library through JNI.
 */
public class TidesDB implements Closeable {
    
    static {
        NativeLibrary.load();
    }
    
    private long nativeHandle;
    private boolean closed = false;
    
    private TidesDB(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }
    
    /**
     * Opens a TidesDB instance with the given configuration.
     *
     * @param config the database configuration
     * @return a new TidesDB instance
     * @throws TidesDBException if the database cannot be opened
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
            config.getLogTruncationAt()
        );
        
        return new TidesDB(handle);
    }
    
    /**
     * Closes the database instance and releases all resources.
     */
    @Override
    public void close() {
        if (!closed && nativeHandle != 0) {
            nativeClose(nativeHandle);
            nativeHandle = 0;
            closed = true;
        }
    }
    
    /**
     * Creates a new column family with the given configuration.
     *
     * @param name the column family name
     * @param config the column family configuration
     * @throws TidesDBException if the column family cannot be created
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
            config.getWriteBufferSize(),
            config.getLevelSizeRatio(),
            config.getMinLevels(),
            config.getDividingLevelOffset(),
            config.getKlogValueThreshold(),
            config.getCompressionAlgorithm().getValue(),
            config.isEnableBloomFilter(),
            config.getBloomFPR(),
            config.isEnableBlockIndexes(),
            config.getIndexSampleRatio(),
            config.getBlockIndexPrefixLen(),
            config.getSyncMode().getValue(),
            config.getSyncIntervalUs(),
            config.getComparatorName(),
            config.getSkipListMaxLevel(),
            config.getSkipListProbability(),
            config.getDefaultIsolationLevel().getValue(),
            config.getMinDiskSpace(),
            config.getL1FileCountTrigger(),
            config.getL0QueueStallThreshold(),
            config.isUseBtree()
        );
    }
    
    /**
     * Drops a column family and all associated data.
     *
     * @param name the column family name
     * @throws TidesDBException if the column family cannot be dropped
     */
    public void dropColumnFamily(String name) throws TidesDBException {
        checkNotClosed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Column family name cannot be null or empty");
        }
        nativeDropColumnFamily(nativeHandle, name);
    }
    
    /**
     * Retrieves a column family by name.
     *
     * @param name the column family name
     * @return the column family
     * @throws TidesDBException if the column family is not found
     */
    public ColumnFamily getColumnFamily(String name) throws TidesDBException {
        checkNotClosed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Column family name cannot be null or empty");
        }
        long cfHandle = nativeGetColumnFamily(nativeHandle, name);
        return new ColumnFamily(cfHandle, name);
    }
    
    /**
     * Lists all column families in the database.
     *
     * @return array of column family names
     * @throws TidesDBException if the list cannot be retrieved
     */
    public String[] listColumnFamilies() throws TidesDBException {
        checkNotClosed();
        return nativeListColumnFamilies(nativeHandle);
    }
    
    /**
     * Begins a new transaction with default isolation level.
     *
     * @return a new transaction
     * @throws TidesDBException if the transaction cannot be started
     */
    public Transaction beginTransaction() throws TidesDBException {
        checkNotClosed();
        long txnHandle = nativeBeginTransaction(nativeHandle);
        return new Transaction(txnHandle);
    }
    
    /**
     * Begins a new transaction with the specified isolation level.
     *
     * @param isolationLevel the isolation level
     * @return a new transaction
     * @throws TidesDBException if the transaction cannot be started
     */
    public Transaction beginTransaction(IsolationLevel isolationLevel) throws TidesDBException {
        checkNotClosed();
        if (isolationLevel == null) {
            throw new IllegalArgumentException("Isolation level cannot be null");
        }
        long txnHandle = nativeBeginTransactionWithIsolation(nativeHandle, isolationLevel.getValue());
        return new Transaction(txnHandle);
    }
    
    /**
     * Retrieves statistics about the block cache.
     *
     * @return cache statistics
     * @throws TidesDBException if the stats cannot be retrieved
     */
    public CacheStats getCacheStats() throws TidesDBException {
        checkNotClosed();
        return nativeGetCacheStats(nativeHandle);
    }
    
    /**
     * Registers a custom comparator with the database.
     *
     * @param name the comparator name
     * @param context optional context string
     * @throws TidesDBException if the comparator cannot be registered
     */
    public void registerComparator(String name, String context) throws TidesDBException {
        checkNotClosed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Comparator name cannot be null or empty");
        }
        nativeRegisterComparator(nativeHandle, name, context);
    }
    
    /**
     * Creates an on-disk snapshot of the database without blocking normal reads/writes.
     *
     * @param dir the backup directory (must be non-existent or empty)
     * @throws TidesDBException if the backup fails
     */
    public void backup(String dir) throws TidesDBException {
        checkNotClosed();
        if (dir == null || dir.isEmpty()) {
            throw new IllegalArgumentException("Backup directory cannot be null or empty");
        }
        nativeBackup(nativeHandle, dir);
    }
    
    /**
     * Atomically renames a column family and its underlying directory.
     * The operation waits for any in-progress flush or compaction to complete before renaming.
     *
     * @param oldName the current column family name
     * @param newName the new column family name
     * @throws TidesDBException if the rename fails
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
    
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("TidesDB instance is closed");
        }
    }
    
    long getNativeHandle() {
        return nativeHandle;
    }
    
    private static native long nativeOpen(String dbPath, int numFlushThreads, int numCompactionThreads,
                                          int logLevel, long blockCacheSize, long maxOpenSSTables,
                                          boolean logToFile, long logTruncationAt) throws TidesDBException;
    
    private static native void nativeClose(long handle);
    
    private static native void nativeCreateColumnFamily(long handle, String name,
        long writeBufferSize, long levelSizeRatio, int minLevels, int dividingLevelOffset,
        long klogValueThreshold, int compressionAlgorithm, boolean enableBloomFilter,
        double bloomFPR, boolean enableBlockIndexes, int indexSampleRatio, int blockIndexPrefixLen,
        int syncMode, long syncIntervalUs, String comparatorName, int skipListMaxLevel,
        float skipListProbability, int defaultIsolationLevel, long minDiskSpace,
        int l1FileCountTrigger, int l0QueueStallThreshold, boolean useBtree) throws TidesDBException;
    
    private static native void nativeDropColumnFamily(long handle, String name) throws TidesDBException;
    
    private static native long nativeGetColumnFamily(long handle, String name) throws TidesDBException;
    
    private static native String[] nativeListColumnFamilies(long handle) throws TidesDBException;
    
    private static native long nativeBeginTransaction(long handle) throws TidesDBException;
    
    private static native long nativeBeginTransactionWithIsolation(long handle, int isolationLevel) throws TidesDBException;
    
    private static native CacheStats nativeGetCacheStats(long handle) throws TidesDBException;
    
    private static native void nativeRegisterComparator(long handle, String name, String context) throws TidesDBException;
    
    private static native void nativeBackup(long handle, String dir) throws TidesDBException;
    
    private static native void nativeRenameColumnFamily(long handle, String oldName, String newName) throws TidesDBException;
}
