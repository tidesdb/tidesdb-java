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
 * How much waiting one {@link StallReason} accounted for since the database
 * opened. Created by the native library and returned inside {@link StallStats}.
 */
public class StallStat {

    private final long count;
    private final long totalUs;
    private final long maxUs;

    /**
     * Creates a new {@code StallStat}. Typically called by the JNI bridge
     * rather than application code.
     *
     * @param count how many times a thread waited here
     * @param totalUs the summed wait in microseconds
     * @param maxUs the longest single wait in microseconds
     */
    public StallStat(long count, long totalUs, long maxUs) {
        this.count = count;
        this.totalUs = totalUs;
        this.maxUs = maxUs;
    }

    /**
     * Returns how many times a thread waited here.
     *
     * @return the wait count
     */
    public long getCount() {
        return count;
    }

    /**
     * Returns the summed wait, so a reason's share of all waiting is
     * comparable.
     *
     * @return the total wait in microseconds
     */
    public long getTotalUs() {
        return totalUs;
    }

    /**
     * Returns the longest single wait, which is what a latency tail is made of.
     *
     * @return the longest wait in microseconds
     */
    public long getMaxUs() {
        return maxUs;
    }

    @Override
    public String toString() {
        return "StallStat{" +
            "count=" + count +
            ", totalUs=" + totalUs +
            ", maxUs=" + maxUs +
            '}';
    }
}
