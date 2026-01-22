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
 * Logging level for TidesDB.
 */
public enum LogLevel {
    DEBUG(0),
    INFO(1),
    WARN(2),
    ERROR(3),
    FATAL(4),
    NONE(5);
    
    private final int value;
    
    LogLevel(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
    
    public static LogLevel fromValue(int value) {
        for (LogLevel level : values()) {
            if (level.value == value) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown log level value: " + value);
    }
}
