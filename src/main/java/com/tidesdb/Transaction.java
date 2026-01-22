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
 * Represents a transaction in TidesDB.
 * Transactions provide atomic operations on the database.
 */
public class Transaction implements Closeable {
    
    static {
        NativeLibrary.load();
    }
    
    private long nativeHandle;
    private boolean freed = false;
    
    Transaction(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }
    
    /**
     * Adds a key-value pair to the transaction.
     *
     * @param cf the column family
     * @param key the key
     * @param value the value
     * @param ttl Unix timestamp (seconds since epoch) for expiration, or -1 for no expiration
     * @throws TidesDBException if the put fails
     */
    public void put(ColumnFamily cf, byte[] key, byte[] value, long ttl) throws TidesDBException {
        checkNotFreed();
        if (cf == null) {
            throw new IllegalArgumentException("Column family cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        nativePut(nativeHandle, cf.getNativeHandle(), key, value, ttl);
    }
    
    /**
     * Adds a key-value pair to the transaction with no expiration.
     *
     * @param cf the column family
     * @param key the key
     * @param value the value
     * @throws TidesDBException if the put fails
     */
    public void put(ColumnFamily cf, byte[] key, byte[] value) throws TidesDBException {
        put(cf, key, value, -1);
    }
    
    /**
     * Retrieves a value from the transaction.
     *
     * @param cf the column family
     * @param key the key
     * @return the value, or null if not found
     * @throws TidesDBException if the get fails
     */
    public byte[] get(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        if (cf == null) {
            throw new IllegalArgumentException("Column family cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return nativeGet(nativeHandle, cf.getNativeHandle(), key);
    }
    
    /**
     * Removes a key-value pair from the transaction.
     *
     * @param cf the column family
     * @param key the key
     * @throws TidesDBException if the delete fails
     */
    public void delete(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        if (cf == null) {
            throw new IllegalArgumentException("Column family cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        nativeDelete(nativeHandle, cf.getNativeHandle(), key);
    }
    
    /**
     * Commits the transaction.
     *
     * @throws TidesDBException if the commit fails
     */
    public void commit() throws TidesDBException {
        checkNotFreed();
        nativeCommit(nativeHandle);
    }
    
    /**
     * Rolls back the transaction.
     *
     * @throws TidesDBException if the rollback fails
     */
    public void rollback() throws TidesDBException {
        checkNotFreed();
        nativeRollback(nativeHandle);
    }
    
    /**
     * Creates a savepoint within the transaction.
     *
     * @param name the savepoint name
     * @throws TidesDBException if the savepoint cannot be created
     */
    public void savepoint(String name) throws TidesDBException {
        checkNotFreed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Savepoint name cannot be null or empty");
        }
        nativeSavepoint(nativeHandle, name);
    }
    
    /**
     * Rolls back the transaction to a savepoint.
     *
     * @param name the savepoint name
     * @throws TidesDBException if the rollback fails
     */
    public void rollbackToSavepoint(String name) throws TidesDBException {
        checkNotFreed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Savepoint name cannot be null or empty");
        }
        nativeRollbackToSavepoint(nativeHandle, name);
    }
    
    /**
     * Releases a savepoint without rolling back.
     *
     * @param name the savepoint name
     * @throws TidesDBException if the release fails
     */
    public void releaseSavepoint(String name) throws TidesDBException {
        checkNotFreed();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Savepoint name cannot be null or empty");
        }
        nativeReleaseSavepoint(nativeHandle, name);
    }
    
    /**
     * Creates a new iterator for a column family within this transaction.
     *
     * @param cf the column family
     * @return a new iterator
     * @throws TidesDBException if the iterator cannot be created
     */
    public TidesDBIterator newIterator(ColumnFamily cf) throws TidesDBException {
        checkNotFreed();
        if (cf == null) {
            throw new IllegalArgumentException("Column family cannot be null");
        }
        long iterHandle = nativeNewIterator(nativeHandle, cf.getNativeHandle());
        return new TidesDBIterator(iterHandle);
    }
    
    /**
     * Frees the transaction resources.
     */
    public void free() {
        if (!freed && nativeHandle != 0) {
            nativeFree(nativeHandle);
            nativeHandle = 0;
            freed = true;
        }
    }
    
    /**
     * Closes the transaction (same as free).
     */
    @Override
    public void close() {
        free();
    }
    
    private void checkNotFreed() {
        if (freed) {
            throw new IllegalStateException("Transaction has been freed");
        }
    }
    
    long getNativeHandle() {
        return nativeHandle;
    }
    
    private static native void nativePut(long handle, long cfHandle, byte[] key, byte[] value, long ttl) throws TidesDBException;
    private static native byte[] nativeGet(long handle, long cfHandle, byte[] key) throws TidesDBException;
    private static native void nativeDelete(long handle, long cfHandle, byte[] key) throws TidesDBException;
    private static native void nativeCommit(long handle) throws TidesDBException;
    private static native void nativeRollback(long handle) throws TidesDBException;
    private static native void nativeSavepoint(long handle, String name) throws TidesDBException;
    private static native void nativeRollbackToSavepoint(long handle, String name) throws TidesDBException;
    private static native void nativeReleaseSavepoint(long handle, String name) throws TidesDBException;
    private static native long nativeNewIterator(long handle, long cfHandle) throws TidesDBException;
    private static native void nativeFree(long handle);
}
