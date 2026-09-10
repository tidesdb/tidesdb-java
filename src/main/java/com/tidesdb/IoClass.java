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
 * The kinds of file the engine writes, so device time can be attributed rather
 * than pooled.
 *
 * <p>The ordinal of each constant is its index into {@link IoStats}.
 */
public enum IoClass {

    /**
     * Key logs, written by flush and compaction.
     */
    SSTABLE(0),

    /**
     * The write-ahead log, written by its own single flush thread.
     */
    WAL(1),

    /**
     * Value log segments, written by a commit that separates a value and by the
     * reclaim that copies live values forward. This is the write cost of
     * key/value separation, so it is counted apart from the key logs whose size
     * that separation is what keeps down.
     */
    VLOG(2);

    static {
        NativeLibrary.load();
    }

    private final int value;

    IoClass(int value) {
        this.value = value;
    }

    /**
     * Returns the JNI numeric mapping for this I/O class, which is also its
     * index into {@link IoStats}.
     *
     * @return the integer value used by the native library
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the native library's stable short name for this class, suitable
     * for a log line or a stats table's row label.
     *
     * @return the name, never {@code null}
     */
    public String getNativeName() {
        return nativeName(value);
    }

    private static native String nativeName(int cls);

    /**
     * Returns the {@link IoClass} constant matching the given JNI integer
     * value.
     *
     * @param value the JNI integer value
     * @return the matching constant
     * @throws IllegalArgumentException if {@code value} does not map to any
     *         known constant
     */
    public static IoClass fromValue(int value) {
        for (IoClass cls : values()) {
            if (cls.value == value) {
                return cls;
            }
        }
        throw new IllegalArgumentException("Unknown I/O class value: " + value);
    }
}
