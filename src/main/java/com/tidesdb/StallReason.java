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
 * The places a caller's own thread can be made to wait inside a write. A commit
 * that took far longer than its peers was held at exactly one of these, and
 * knowing which is the difference between a device that cannot keep up and an
 * engine that is not letting it.
 *
 * <p>The ordinal of each constant is its index into {@link StallStats}.
 */
public enum StallReason {

    /**
     * Waiting on the write-ahead log, either for staging-ring space or, under a
     * syncing mode, for the record to reach the file. The two are one figure
     * because both are the same wait on the same single writer.
     */
    WAL_APPEND(0),

    /**
     * Waiting to take the rotation lock, so another committer was rotating.
     */
    ROTATE_LOCK(1),

    /**
     * Performing the rotation, which this thread pays on everyone's behalf.
     */
    ROTATE_WORK(2),

    /**
     * Held by write admission because the unflushed backlog was too deep.
     */
    ADMISSION(3),

    /**
     * Inside a manifest commit, which every flush install, every compaction
     * install and every DDL serialises through, so a database making no
     * progress is often waiting here.
     */
    MANIFEST_COMMIT(4);

    static {
        NativeLibrary.load();
    }

    private final int value;

    StallReason(int value) {
        this.value = value;
    }

    /**
     * Returns the JNI numeric mapping for this stall reason, which is also its
     * index into {@link StallStats}.
     *
     * @return the integer value used by the native library
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the native library's stable short name for this reason, suitable
     * for a log line or a stats table's row label.
     *
     * @return the name, never {@code null}
     */
    public String getNativeName() {
        return nativeName(value);
    }

    private static native String nativeName(int reason);

    /**
     * Returns the {@link StallReason} constant matching the given JNI integer
     * value.
     *
     * @param value the JNI integer value
     * @return the matching constant
     * @throws IllegalArgumentException if {@code value} does not map to any
     *         known constant
     */
    public static StallReason fromValue(int value) {
        for (StallReason reason : values()) {
            if (reason.value == value) {
                return reason;
            }
        }
        throw new IllegalArgumentException("Unknown stall reason value: " + value);
    }
}
