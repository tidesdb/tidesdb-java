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
 * Transaction lifecycle state, reported by {@link Transaction#state()} for
 * two-phase-commit coordination. Each constant maps to an integer used by the
 * JNI bridge.
 */
public enum TransactionState {

    /**
     * Buffering writes, not yet resolved.
     */
    ACTIVE(0),

    /**
     * Durably prepared under an xid, awaiting commit or rollback.
     */
    PREPARED(1),

    /**
     * Committed and applied.
     */
    COMMITTED(2),

    /**
     * Rolled back or expired.
     */
    ABORTED(3);

    private final int value;

    TransactionState(int value) {
        this.value = value;
    }

    /**
     * Returns the JNI numeric mapping for this transaction state.
     *
     * @return the integer value passed to the native library
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the {@link TransactionState} constant matching the given JNI
     * integer value.
     *
     * @param value the JNI integer value
     * @return the matching constant
     * @throws IllegalArgumentException if {@code value} does not map to any
     *         known constant
     */
    public static TransactionState fromValue(int value) {
        for (TransactionState state : values()) {
            if (state.value == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown transaction state value: " + value);
    }
}
