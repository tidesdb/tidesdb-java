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
 * Compression algorithm for column families.
 */
public enum CompressionAlgorithm {
    NO_COMPRESSION(0),
    LZ4_COMPRESSION(1),
    ZSTD_COMPRESSION(2),
    LZ4_FAST_COMPRESSION(3);
    
    private final int value;
    
    CompressionAlgorithm(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
    
    public static CompressionAlgorithm fromValue(int value) {
        for (CompressionAlgorithm algo : values()) {
            if (algo.value == value) {
                return algo;
            }
        }
        throw new IllegalArgumentException("Unknown compression algorithm value: " + value);
    }
}
