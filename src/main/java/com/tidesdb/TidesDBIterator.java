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
 * Provides efficient bidirectional traversal.
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
     * @throws TidesDBException if the seek fails
     */
    public void seekToFirst() throws TidesDBException {
        checkNotFreed();
        nativeSeekToFirst(nativeHandle);
    }
    
    /**
     * Positions the iterator at the last key.
     *
     * @throws TidesDBException if the seek fails
     */
    public void seekToLast() throws TidesDBException {
        checkNotFreed();
        nativeSeekToLast(nativeHandle);
    }
    
    /**
     * Positions the iterator at the first key >= target key.
     *
     * @param key the target key
     * @throws TidesDBException if the seek fails
     */
    public void seek(byte[] key) throws TidesDBException {
        checkNotFreed();
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        nativeSeek(nativeHandle, key);
    }
    
    /**
     * Positions the iterator at the last key <= target key.
     *
     * @param key the target key
     * @throws TidesDBException if the seek fails
     */
    public void seekForPrev(byte[] key) throws TidesDBException {
        checkNotFreed();
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        nativeSeekForPrev(nativeHandle, key);
    }
    
    /**
     * Returns true if the iterator is positioned at a valid entry.
     *
     * @return true if valid
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
     * @throws TidesDBException if the move fails
     */
    public void next() throws TidesDBException {
        checkNotFreed();
        nativeNext(nativeHandle);
    }
    
    /**
     * Moves the iterator to the previous entry.
     *
     * @throws TidesDBException if the move fails
     */
    public void prev() throws TidesDBException {
        checkNotFreed();
        nativePrev(nativeHandle);
    }
    
    /**
     * Retrieves the current key from the iterator.
     *
     * @return the current key
     * @throws TidesDBException if the key cannot be retrieved
     */
    public byte[] key() throws TidesDBException {
        checkNotFreed();
        return nativeKey(nativeHandle);
    }
    
    /**
     * Retrieves the current value from the iterator.
     *
     * @return the current value
     * @throws TidesDBException if the value cannot be retrieved
     */
    public byte[] value() throws TidesDBException {
        checkNotFreed();
        return nativeValue(nativeHandle);
    }
    
    /**
     * Frees the iterator resources.
     */
    public void free() {
        if (!freed && nativeHandle != 0) {
            nativeFree(nativeHandle);
            nativeHandle = 0;
            freed = true;
        }
    }
    
    /**
     * Closes the iterator (same as free).
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
    private static native void nativeFree(long handle);
}
