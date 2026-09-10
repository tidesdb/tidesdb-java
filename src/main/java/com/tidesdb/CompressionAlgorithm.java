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
 * A built-in compression codec, usable as an entry in a column family's encoding
 * pipeline. Each constant's {@link #getValue()} is its encoding id, which is
 * persisted in SSTable and value-log metadata.
 *
 * <p>Every constant is always defined, but whether a backend can actually be
 * used is a build-time choice of the native library. Ask
 * {@link TidesDB#isCompressionAvailable(CompressionAlgorithm)} before choosing
 * one, rather than discovering it when a node fails to decode. {@link #NONE} is
 * always available.
 */
public enum CompressionAlgorithm {

    /**
     * No compression; data is stored verbatim.
     */
    NONE(0),

    /**
     * Snappy compression.
     */
    SNAPPY(1),

    /**
     * LZ4 compression with default settings.
     */
    LZ4(2),

    /**
     * Zstandard compression.
     */
    ZSTD(3),

    /**
     * LZ4 compression optimised for speed.
     */
    LZ4_FAST(4);

    private final int value;

    CompressionAlgorithm(int value) {
        this.value = value;
    }

    /**
     * Returns this codec's encoding id, the value stored in SSTable and
     * value-log metadata and accepted by
     * {@link ColumnFamilyConfig.Builder#encodingPipelineIds(int...)}.
     *
     * @return the encoding id
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the {@link CompressionAlgorithm} constant matching the given
     * encoding id.
     *
     * @param value the encoding id
     * @return the matching constant
     * @throws IllegalArgumentException if {@code value} does not map to any
     *         built-in codec
     */
    public static CompressionAlgorithm fromValue(int value) {
        for (CompressionAlgorithm algo : values()) {
            if (algo.value == value) {
                return algo;
            }
        }
        throw new IllegalArgumentException("Unknown compression algorithm value: " + value);
    }
}
