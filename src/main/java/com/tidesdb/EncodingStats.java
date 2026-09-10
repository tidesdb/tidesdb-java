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

import java.util.Arrays;

/**
 * What one encoding chain achieved on the data it wrote, returned from
 * {@link TidesDB#getKlogEncodingStats()} and
 * {@link TidesDB#getVlogEncodingStats()}.
 *
 * <p>Reported per chain rather than per column family because a family can
 * change its codec, and compaction rewrites data under whichever pipeline is
 * merging it, so a single figure for a family would average across settings
 * that no longer apply and describe none of them.
 */
public class EncodingStats {

    /**
     * The most encoding chains reported separately, for either the key logs or
     * the value log. A collector never returns more than this many entries.
     */
    public static final int MAX_CHAINS = 16;

    private final int[] ids;
    private final long logicalBytes;
    private final long storedBytes;
    private final long itemCount;

    /**
     * Creates a new {@code EncodingStats}. Typically called by the JNI bridge
     * rather than application code.
     *
     * @param ids the codec ids in the order applied, empty when the data was
     *        stored verbatim
     * @param logicalBytes what the data amounts to before encoding
     * @param storedBytes what it occupies on disk
     * @param itemCount values for the value log, SSTables for the key logs
     */
    public EncodingStats(int[] ids, long logicalBytes, long storedBytes, long itemCount) {
        this.ids = ids == null ? new int[0] : ids.clone();
        this.logicalBytes = logicalBytes;
        this.storedBytes = storedBytes;
        this.itemCount = itemCount;
    }

    /**
     * Returns the codec ids in the order applied, empty when the data was
     * stored verbatim. Ids in the range of {@link CompressionAlgorithm} name a
     * built-in compression codec.
     *
     * @return a copy of the codec ids
     */
    public int[] getIds() {
        return ids.clone();
    }

    /**
     * Returns what the data amounts to before encoding.
     *
     * @return the logical byte count
     */
    public long getLogicalBytes() {
        return logicalBytes;
    }

    /**
     * Returns what the data occupies on disk.
     *
     * @return the stored byte count
     */
    public long getStoredBytes() {
        return storedBytes;
    }

    /**
     * Returns the number of items this chain wrote: values for the value log,
     * SSTables for the key logs.
     *
     * @return the item count
     */
    public long getItemCount() {
        return itemCount;
    }

    /**
     * Returns the realised compression ratio, logical over stored, so a value
     * above 1.0 means the data shrank.
     *
     * @return the ratio, or 0.0 when nothing has been stored
     */
    public double getRatio() {
        if (storedBytes == 0) {
            return 0.0;
        }
        return (double) logicalBytes / (double) storedBytes;
    }

    @Override
    public String toString() {
        return "EncodingStats{" +
            "ids=" + Arrays.toString(ids) +
            ", logicalBytes=" + logicalBytes +
            ", storedBytes=" + storedBytes +
            ", itemCount=" + itemCount +
            ", ratio=" + getRatio() +
            '}';
    }
}
