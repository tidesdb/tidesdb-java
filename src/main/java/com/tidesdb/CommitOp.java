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
 * Represents a single operation in a committed transaction batch.
 * Passed to the commit hook callback after a transaction commits.
 */
public class CommitOp {
    
    private final byte[] key;
    private final byte[] value;
    private final long ttl;
    private final boolean delete;
    
    /**
     * Creates a new CommitOp.
     *
     * @param key the key
     * @param value the value (null for deletes)
     * @param ttl time-to-live (-1 for no expiry)
     * @param delete true if this is a delete operation
     */
    public CommitOp(byte[] key, byte[] value, long ttl, boolean delete) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;
        this.delete = delete;
    }
    
    /**
     * Gets the key for this operation.
     *
     * @return the key bytes
     */
    public byte[] getKey() {
        return key;
    }
    
    /**
     * Gets the value for this operation.
     *
     * @return the value bytes, or null for delete operations
     */
    public byte[] getValue() {
        return value;
    }
    
    /**
     * Gets the TTL for this operation.
     *
     * @return the TTL in seconds since epoch, or -1 for no expiry
     */
    public long getTtl() {
        return ttl;
    }
    
    /**
     * Returns whether this is a delete operation.
     *
     * @return true if delete, false if put
     */
    public boolean isDelete() {
        return delete;
    }
}
