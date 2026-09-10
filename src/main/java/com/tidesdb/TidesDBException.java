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
 * Checked exception thrown by TidesDB native operations. Each exception carries
 * an integer error code corresponding to a TidesDB status. Use {@link #getErrorCode()}
 * to retrieve the numeric code and {@link #getErrorMessage()} for a human-readable
 * description.
 *
 * <p>Two codes describe conditions a caller is expected to handle rather than
 * report: {@link #ERR_LOCKED} is transient contention and the remedy is always
 * to retry, and {@link #ERR_CONFLICT} is the engine's first-committer-wins
 * verdict on a transaction. {@link #isRetryable()} identifies the former.
 */
public class TidesDBException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * The error code.
     */
    private final int errorCode;

    /**
     * Error codes returned by the native TidesDB library.
     */
    public static final int ERR_SUCCESS = 0;

    /**
     * Memory allocation failure.
     */
    public static final int ERR_MEMORY = -1;

    /**
     * Invalid arguments passed to a native operation.
     */
    public static final int ERR_INVALID_ARGS = -2;

    /**
     * Requested entry not found.
     */
    public static final int ERR_NOT_FOUND = -3;

    /**
     * I/O error during a native operation.
     */
    public static final int ERR_IO = -4;

    /**
     * Data corruption detected.
     */
    public static final int ERR_CORRUPTION = -5;

    /**
     * Entry already exists.
     */
    public static final int ERR_EXISTS = -6;

    /**
     * Transaction conflict. The engine's own first-committer-wins verdict:
     * another transaction committed a conflicting write first.
     */
    public static final int ERR_CONFLICT = -7;

    /**
     * A value does not fit the space that has to hold it, such as a caller's
     * buffer too small for what was asked of it.
     */
    public static final int ERR_TOO_LARGE = -8;

    /**
     * Memory limit exceeded.
     */
    public static final int ERR_MEMORY_LIMIT = -9;

    /**
     * Invalid database handle, or the database is closing.
     */
    public static final int ERR_INVALID_DB = -10;

    /**
     * Unknown error.
     */
    public static final int ERR_UNKNOWN = -11;

    /**
     * Transient contention: something else held what the call needed, nothing
     * was written, and the remedy is always to try again. It is not confined to
     * operations that take a column family exclusively, since a read has to open
     * SSTables and walk sources a compaction may be moving. Treat it as retry,
     * never as absence and never as an error to surface.
     */
    public static final int ERR_LOCKED = -12;

    /**
     * The database is read-only.
     */
    public static final int ERR_READONLY = -13;

    /**
     * The transaction's timeout has passed; the next operation on it expired it.
     */
    public static final int ERR_TXN_EXPIRED = -14;

    /**
     * The device or filesystem is out of space. Distinct from {@link #ERR_IO}
     * because the data is intact and the operation succeeds once space is freed.
     */
    public static final int ERR_NO_SPACE = -15;

    /**
     * The transaction was aborted from outside through
     * {@link Transaction#requestAbort()}. Distinct from {@link #ERR_CONFLICT}:
     * a conflict is the engine's own verdict, where this says an outside
     * authority decided the transaction loses and the engine never got a say.
     */
    public static final int ERR_TXN_ABORTED = -16;

    /**
     * The point in time asked for is no longer reconstructable: a collection has
     * already run below that sequence, so the versions it would resolve to are
     * gone.
     */
    public static final int ERR_TOO_OLD = -17;

    /**
     * Creates an exception with a message and an unknown error code.
     *
     * @param message the detail message
     */
    public TidesDBException(String message) {
        super(message);
        this.errorCode = ERR_UNKNOWN;
    }

    /**
     * Creates an exception with a message and a specific error code.
     *
     * @param message the detail message
     * @param errorCode the TidesDB error code
     */
    public TidesDBException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Creates an exception with a message and a cause, using an unknown
     * error code.
     *
     * @param message the detail message
     * @param cause the underlying cause
     */
    public TidesDBException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ERR_UNKNOWN;
    }

    /**
     * Creates an exception with a message, error code, and cause.
     *
     * @param message the detail message
     * @param errorCode the TidesDB error code
     * @param cause the underlying cause
     */
    public TidesDBException(String message, int errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Returns the TidesDB error code.
     *
     * @return one of the {@code ERR_*} constants defined in this class
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Returns whether the operation failed to transient contention and should
     * simply be retried. Nothing was written and nothing is wrong with the
     * database.
     *
     * @return {@code true} when the error code is {@link #ERR_LOCKED}
     */
    public boolean isRetryable() {
        return errorCode == ERR_LOCKED;
    }

    /**
     * Returns a human-readable description of the error code. The mapping is
     * local to this class and does not invoke any native method.
     *
     * @return the error description
     */
    public String getErrorMessage() {
        switch (errorCode) {
            case ERR_SUCCESS:
                return "success";
            case ERR_MEMORY:
                return "memory allocation failed";
            case ERR_INVALID_ARGS:
                return "invalid arguments";
            case ERR_NOT_FOUND:
                return "not found";
            case ERR_IO:
                return "I/O error";
            case ERR_CORRUPTION:
                return "data corruption";
            case ERR_EXISTS:
                return "already exists";
            case ERR_CONFLICT:
                return "transaction conflict";
            case ERR_TOO_LARGE:
                return "value too large for the space that must hold it";
            case ERR_MEMORY_LIMIT:
                return "memory limit exceeded";
            case ERR_INVALID_DB:
                return "invalid database handle";
            case ERR_LOCKED:
                return "resource is locked, retry";
            case ERR_READONLY:
                return "database is read-only";
            case ERR_TXN_EXPIRED:
                return "transaction expired";
            case ERR_NO_SPACE:
                return "no space left on device";
            case ERR_TXN_ABORTED:
                return "transaction aborted by request";
            case ERR_TOO_OLD:
                return "sequence is no longer reconstructable";
            default:
                return "unknown error";
        }
    }
}
