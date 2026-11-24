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
 * Transaction isolation level, weakest to strongest. Each constant maps to an
 * integer used by the JNI bridge.
 *
 * <p>Only the levels above {@link #READ_COMMITTED} can have a commit refused
 * with {@link TidesDBException#ERR_CONFLICT}; {@link DbStats#getTxnConflicts()}
 * counts those refusals.
 */
public enum IsolationLevel {

    /**
     * Every version is visible, including sequences still in progress.
     */
    READ_UNCOMMITTED(0),

    /**
     * The newest committed version, with the read ceiling re-read on every
     * operation. This is the default.
     */
    READ_COMMITTED(1),

    /**
     * The read ceiling is frozen when the transaction begins, and a commit
     * validates that every key it read still holds the version it read and that
     * no key appeared inside a range it scanned, against commits still in flight
     * as well as committed ones.
     *
     * <p>A scanned range is recorded when its iterator is freed, so close
     * iterators before committing.
     */
    REPEATABLE_READ(2),

    /**
     * Frozen read ceiling, and a commit claims each key it writes on a
     * first-committer-wins basis rather than validating what it read.
     */
    SNAPSHOT(3),

    /**
     * Both the {@link #REPEATABLE_READ} read validation and the
     * {@link #SNAPSHOT} write claim.
     */
    SERIALIZABLE(4);
    
    private final int value;
    
    IsolationLevel(int value) {
        this.value = value;
    }
    
    /**
     * Returns the JNI numeric mapping for this isolation level.
     *
     * @return the integer value passed to the native library
     */
    public int getValue() {
        return value;
    }
    
    /**
     * Returns the {@link IsolationLevel} constant matching the given JNI
     * integer value.
     *
     * @param value the JNI integer value
     * @return the matching constant
     * @throws IllegalArgumentException if {@code value} does not map to any
     *         known constant
     */
    public static IsolationLevel fromValue(int value) {
        for (IsolationLevel level : values()) {
            if (level.value == value) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown isolation level value: " + value);
    }
}
