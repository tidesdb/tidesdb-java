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
 * What a query planner needs to know about a key range, returned from
 * {@link ColumnFamily#rangeStats(byte[], byte[])}. Both figures are taken from
 * one layout snapshot, so they describe the same instant.
 */
public class RangeStats {

    private final long sstablesOverlapping;
    private final long estimatedKeys;
    private final boolean keysExact;

    /**
     * Creates a new {@code RangeStats}. Typically called by the JNI bridge
     * rather than application code.
     *
     * @param sstablesOverlapping sorted runs a scan of the range would merge
     * @param estimatedKeys live keys the range holds
     * @param keysExact whether {@code estimatedKeys} was counted rather than
     *        estimated from metadata
     */
    public RangeStats(long sstablesOverlapping, long estimatedKeys, boolean keysExact) {
        this.sstablesOverlapping = sstablesOverlapping;
        this.estimatedKeys = estimatedKeys;
        this.keysExact = keysExact;
    }

    /**
     * Returns the number of sorted runs a scan of the range would merge, which
     * is the shape of its cost.
     *
     * @return the overlapping SSTable count
     */
    public long getSstablesOverlapping() {
        return sstablesOverlapping;
    }

    /**
     * Returns the live keys the range holds, with tombstoned and superseded
     * versions excluded.
     *
     * @return the key count
     */
    public long getEstimatedKeys() {
        return estimatedKeys;
    }

    /**
     * Returns whether {@link #getEstimatedKeys()} was counted rather than
     * estimated from metadata, so a planner can trust it outright instead of
     * hedging.
     *
     * @return {@code true} when the count is exact
     */
    public boolean isKeysExact() {
        return keysExact;
    }

    @Override
    public String toString() {
        return "RangeStats{" +
            "sstablesOverlapping=" + sstablesOverlapping +
            ", estimatedKeys=" + estimatedKeys +
            ", keysExact=" + keysExact +
            '}';
    }
}
