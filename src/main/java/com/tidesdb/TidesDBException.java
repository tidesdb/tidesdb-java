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
 * Exception thrown by TidesDB operations.
 */
public class TidesDBException extends Exception {
    
    private final int errorCode;
    
    /**
     * Error codes from TidesDB.
     */
    public static final int ERR_SUCCESS = 0;
    public static final int ERR_MEMORY = -1;
    public static final int ERR_INVALID_ARGS = -2;
    public static final int ERR_NOT_FOUND = -3;
    public static final int ERR_IO = -4;
    public static final int ERR_CORRUPTION = -5;
    public static final int ERR_EXISTS = -6;
    public static final int ERR_CONFLICT = -7;
    public static final int ERR_TOO_LARGE = -8;
    public static final int ERR_MEMORY_LIMIT = -9;
    public static final int ERR_INVALID_DB = -10;
    public static final int ERR_UNKNOWN = -11;
    public static final int ERR_LOCKED = -12;
    
    public TidesDBException(String message) {
        super(message);
        this.errorCode = ERR_UNKNOWN;
    }
    
    public TidesDBException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public TidesDBException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ERR_UNKNOWN;
    }
    
    public TidesDBException(String message, int errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    /**
     * Gets the error code.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return errorCode;
    }
    
    /**
     * Returns a human-readable error message for the error code.
     *
     * @return error message
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
                return "key or value too large";
            case ERR_MEMORY_LIMIT:
                return "memory limit exceeded";
            case ERR_INVALID_DB:
                return "invalid database handle";
            case ERR_LOCKED:
                return "database is locked";
            default:
                return "unknown error";
        }
    }
}
