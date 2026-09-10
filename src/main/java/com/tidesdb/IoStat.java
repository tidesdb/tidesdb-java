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
 * What one {@link IoClass} asked of the device. Created by the native library
 * and returned inside {@link IoStats}.
 */
public class IoStat {

    private final long ops;
    private final long bytes;
    private final long totalUs;
    private final long maxUs;

    /**
     * Creates a new {@code IoStat}. Typically called by the JNI bridge rather
     * than application code.
     *
     * @param ops writes issued
     * @param bytes bytes written
     * @param totalUs the summed time inside those writes, in microseconds
     * @param maxUs the slowest single write, in microseconds
     */
    public IoStat(long ops, long bytes, long totalUs, long maxUs) {
        this.ops = ops;
        this.bytes = bytes;
        this.totalUs = totalUs;
        this.maxUs = maxUs;
    }

    /**
     * Returns the number of writes issued.
     *
     * @return the write count
     */
    public long getOps() {
        return ops;
    }

    /**
     * Returns the number of bytes written.
     *
     * @return the byte count
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Returns the summed time inside those writes.
     *
     * @return the total time in microseconds
     */
    public long getTotalUs() {
        return totalUs;
    }

    /**
     * Returns the slowest single write.
     *
     * @return the slowest write in microseconds
     */
    public long getMaxUs() {
        return maxUs;
    }

    /**
     * Returns the throughput this class actually achieved, in bytes per second.
     * Compare it against what the storage can sustain, because a saturated
     * device and a stalled engine look identical from the application.
     *
     * @return bytes per second, or 0.0 when no time has been spent writing
     */
    public double getBytesPerSecond() {
        if (totalUs == 0) {
            return 0.0;
        }
        return (double) bytes * 1_000_000.0 / (double) totalUs;
    }

    @Override
    public String toString() {
        return "IoStat{" +
            "ops=" + ops +
            ", bytes=" + bytes +
            ", totalUs=" + totalUs +
            ", maxUs=" + maxUs +
            '}';
    }
}
