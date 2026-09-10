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
 * Iterator for traversing key-value pairs in a column family.
 * Provides efficient bidirectional traversal over the underlying storage.
 *
 * <p>{@code TidesDBIterator} implements {@link java.io.Closeable} for use with
 * try-with-resources. Close iterators before the owning {@link Transaction} is
 * freed. After this iterator is freed, all operations except {@link #isValid()}
 * and {@code close()} throw {@link IllegalStateException}. The {@link #isValid()}
 * method returns {@code false} on a freed iterator.
 *
 * <p>A scan reaches the same SSTables a point read does, so every method here
 * can fail with {@link TidesDBException#ERR_LOCKED} for the same reason and with
 * the same remedy: the position did not move, nothing is wrong with the
 * iterator, and the step should be retried. That is not the end of the range,
 * which is what {@link #isValid()} reports.
 *
 * <p>This class is not guaranteed to be thread-safe.
 */
public class TidesDBIterator implements Closeable {
    
    static {
        NativeLibrary.load();
    }
    
    private long nativeHandle;
    private boolean freed = false;
    
    TidesDBIterator(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }
    
    /**
     * Positions the iterator at the first key.
     *
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native seek fails
     */
    public void seekToFirst() throws TidesDBException {
        checkNotFreed();
        nativeSeekToFirst(nativeHandle);
    }
    
    /**
     * Positions the iterator at the last key.
     *
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native seek fails
     */
    public void seekToLast() throws TidesDBException {
        checkNotFreed();
        nativeSeekToLast(nativeHandle);
    }
    
    /**
     * Positions the iterator at the first key greater than or equal to the
     * target key.
     *
     * @param key the target key; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code key} is {@code null} or empty
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native seek fails
     */
    public void seek(byte[] key) throws TidesDBException {
        checkNotFreed();
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        nativeSeek(nativeHandle, key);
    }
    
    /**
     * Positions the iterator at the last key less than or equal to the target
     * key.
     *
     * @param key the target key; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code key} is {@code null} or empty
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native seek fails
     */
    public void seekForPrev(byte[] key) throws TidesDBException {
        checkNotFreed();
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        nativeSeekForPrev(nativeHandle, key);
    }
    
    /**
     * Returns whether the iterator is positioned at a valid entry.
     *
     * <p>Returns {@code false} if this iterator has been freed, without
     * throwing an exception.
     *
     * @return {@code true} if the iterator is at a valid entry
     */
    public boolean isValid() {
        if (freed) {
            return false;
        }
        return nativeValid(nativeHandle);
    }
    
    /**
     * Moves the iterator to the next entry.
     *
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native next operation fails
     */
    public void next() throws TidesDBException {
        checkNotFreed();
        nativeNext(nativeHandle);
    }
    
    /**
     * Moves the iterator to the previous entry.
     *
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native prev operation fails
     */
    public void prev() throws TidesDBException {
        checkNotFreed();
        nativePrev(nativeHandle);
    }
    
    /**
     * Retrieves the key at the current iterator position.
     *
     * @return the current key
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native key retrieval fails
     */
    public byte[] key() throws TidesDBException {
        checkNotFreed();
        return nativeKey(nativeHandle);
    }
    
    /**
     * Retrieves the value at the current iterator position.
     *
     * @return the current value
     * @throws IllegalStateException if this iterator is freed
     * @throws TidesDBException if the native value retrieval fails
     */
    public byte[] value() throws TidesDBException {
        checkNotFreed();
        return nativeValue(nativeHandle);
    }
    
    /**
     * Retrieves the current key and value from the iterator in a single call.
     * More efficient than calling {@link #key()} and {@link #value()} separately
     * as it requires only one JNI crossing.
     *
     * @return a KeyValue containing the current key and value
     * @throws TidesDBException if the key/value cannot be retrieved
     */
    public KeyValue keyValue() throws TidesDBException {
        checkNotFreed();
        return nativeKeyValue(nativeHandle);
    }

    /**
     * Frees the iterator and releases all native resources.
     *
     * <p>This method is idempotent; subsequent calls are no-ops. After freeing,
     * all operations except {@link #isValid()} throw {@link IllegalStateException}.
     */
    public void free() {
        if (!freed && nativeHandle != 0) {
            nativeFree(nativeHandle);
            nativeHandle = 0;
            freed = true;
        }
    }
    
    /**
     * Closes this iterator. Equivalent to {@link #free()}.
     */
    @Override
    public void close() {
        free();
    }
    
    private void checkNotFreed() {
        if (freed) {
            throw new IllegalStateException("Iterator has been freed");
        }
    }
    
    private static native void nativeSeekToFirst(long handle) throws TidesDBException;
    private static native void nativeSeekToLast(long handle) throws TidesDBException;
    private static native void nativeSeek(long handle, byte[] key) throws TidesDBException;
    private static native void nativeSeekForPrev(long handle, byte[] key) throws TidesDBException;
    private static native boolean nativeValid(long handle);
    private static native void nativeNext(long handle) throws TidesDBException;
    private static native void nativePrev(long handle) throws TidesDBException;
    private static native byte[] nativeKey(long handle) throws TidesDBException;
    private static native byte[] nativeValue(long handle) throws TidesDBException;
    private static native KeyValue nativeKeyValue(long handle) throws TidesDBException;
    private static native void nativeFree(long handle);
}
