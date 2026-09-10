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
 * Per-column-family statistics returned by {@link ColumnFamily#getStats()}.
 *
 * <p>Shared-memtable, value-log, and MVCC figures are database-level and live in
 * {@link DbStats} instead.
 *
 * <p>The per-level arrays are indexed by level minus one, and only the first
 * {@link #getNumLevels()} entries are meaningful.
 */
public class CfStats {

    /**
     * The maximum number of SSTable levels a column family keeps, which is the
     * length of every per-level array on this class.
     */
    public static final int MAX_LEVELS = 8;

    private final int numLevels;
    private final ColumnFamilyConfig config;
    private final long[] levelSizes;
    private final int[] levelNumSstables;
    private final long[] levelKeyCounts;
    private final long[] levelTombstoneCounts;
    private final long totalKeys;
    private final long totalDataSize;
    private final double avgKeySize;
    private final double avgValueSize;
    private final double readAmp;
    private final long btreeTotalNodes;
    private final long btreeMaxHeight;
    private final double btreeAvgHeight;
    private final long totalTombstones;
    private final double tombstoneRatio;
    private final double maxSstDensity;
    private final int maxSstDensityLevel;
    private final long walBytesWritten;
    private final long flushBytesWritten;
    private final long compactionBytesWritten;
    private final long compactionBytesRead;
    private final long userBytesWritten;
    private final long compactionCount;
    private final long unflushedKeyCount;
    private final long filterResidentBytes;

    /**
     * Creates a new {@code CfStats}. Typically called by the JNI bridge rather
     * than application code.
     *
     * @param numLevels number of levels in the column family
     * @param config a copy of the column family configuration
     * @param levelSizes on-disk size of each level, indexed by level minus one
     * @param levelNumSstables SSTable count of each level
     * @param levelKeyCounts key count of each level
     * @param levelTombstoneCounts tombstone count of each level
     * @param totalKeys total distinct keys across every SSTable
     * @param totalDataSize the family's own on-disk size
     * @param avgKeySize average key length in bytes, over distinct keys
     * @param avgValueSize average value length in bytes, over distinct keys
     * @param readAmp point-lookup read amplification
     * @param btreeTotalNodes total btree nodes across the column family
     * @param btreeMaxHeight maximum btree height
     * @param btreeAvgHeight average btree height
     * @param totalTombstones sum of tombstone counts across every SSTable
     * @param tombstoneRatio tombstones over total keys
     * @param maxSstDensity worst per-SSTable tombstone density observed
     * @param maxSstDensityLevel 1-based level where that density was observed
     * @param walBytesWritten this family's share of the shared write-ahead log
     * @param flushBytesWritten on-disk bytes this family's flushes wrote to L1
     * @param compactionBytesWritten on-disk bytes this family's compactions wrote
     * @param compactionBytesRead on-disk bytes this family's compactions read
     * @param userBytesWritten logical key and value bytes committed
     * @param compactionCount compactions this family has run
     * @param unflushedKeyCount distinct keys resident in the shared memtables
     * @param filterResidentBytes memory this family's filters hold outside the cache
     */
    public CfStats(int numLevels, ColumnFamilyConfig config, long[] levelSizes,
                   int[] levelNumSstables, long[] levelKeyCounts, long[] levelTombstoneCounts,
                   long totalKeys, long totalDataSize, double avgKeySize, double avgValueSize,
                   double readAmp, long btreeTotalNodes, long btreeMaxHeight, double btreeAvgHeight,
                   long totalTombstones, double tombstoneRatio, double maxSstDensity,
                   int maxSstDensityLevel, long walBytesWritten, long flushBytesWritten,
                   long compactionBytesWritten, long compactionBytesRead, long userBytesWritten,
                   long compactionCount, long unflushedKeyCount, long filterResidentBytes) {
        this.numLevels = numLevels;
        this.config = config;
        this.levelSizes = levelSizes == null ? new long[MAX_LEVELS] : levelSizes.clone();
        this.levelNumSstables = levelNumSstables == null ? new int[MAX_LEVELS] : levelNumSstables.clone();
        this.levelKeyCounts = levelKeyCounts == null ? new long[MAX_LEVELS] : levelKeyCounts.clone();
        this.levelTombstoneCounts =
            levelTombstoneCounts == null ? new long[MAX_LEVELS] : levelTombstoneCounts.clone();
        this.totalKeys = totalKeys;
        this.totalDataSize = totalDataSize;
        this.avgKeySize = avgKeySize;
        this.avgValueSize = avgValueSize;
        this.readAmp = readAmp;
        this.btreeTotalNodes = btreeTotalNodes;
        this.btreeMaxHeight = btreeMaxHeight;
        this.btreeAvgHeight = btreeAvgHeight;
        this.totalTombstones = totalTombstones;
        this.tombstoneRatio = tombstoneRatio;
        this.maxSstDensity = maxSstDensity;
        this.maxSstDensityLevel = maxSstDensityLevel;
        this.walBytesWritten = walBytesWritten;
        this.flushBytesWritten = flushBytesWritten;
        this.compactionBytesWritten = compactionBytesWritten;
        this.compactionBytesRead = compactionBytesRead;
        this.userBytesWritten = userBytesWritten;
        this.compactionCount = compactionCount;
        this.unflushedKeyCount = unflushedKeyCount;
        this.filterResidentBytes = filterResidentBytes;
    }

    /**
     * Returns the number of levels in the column family. Only the first this
     * many entries of each per-level array are meaningful.
     *
     * @return the level count
     */
    public int getNumLevels() {
        return numLevels;
    }

    /**
     * Returns a copy of the column family's configuration as the engine holds
     * it, including its persisted name.
     *
     * @return the configuration
     */
    public ColumnFamilyConfig getConfig() {
        return config;
    }

    /**
     * Returns the on-disk size of each level, indexed by level minus one.
     *
     * @return a copy of the per-level sizes in bytes
     */
    public long[] getLevelSizes() {
        return levelSizes.clone();
    }

    /**
     * Returns the SSTable count of each level, indexed by level minus one.
     *
     * @return a copy of the per-level SSTable counts
     */
    public int[] getLevelNumSstables() {
        return levelNumSstables.clone();
    }

    /**
     * Returns the key count of each level, indexed by level minus one.
     *
     * @return a copy of the per-level key counts
     */
    public long[] getLevelKeyCounts() {
        return levelKeyCounts.clone();
    }

    /**
     * Returns the tombstone count of each level, indexed by level minus one.
     *
     * @return a copy of the per-level tombstone counts
     */
    public long[] getLevelTombstoneCounts() {
        return levelTombstoneCounts.clone();
    }

    /**
     * Returns the total distinct keys across every SSTable in the column family.
     * Add {@link #getUnflushedKeyCount()} for the live logical key count
     * including what is still in memory.
     *
     * @return the key count
     */
    public long getTotalKeys() {
        return totalKeys;
    }

    /**
     * Returns the family's own on-disk size, the sum of its key logs. Values
     * below the separation threshold are inside those bytes already; what
     * spilled lives in the shared value log, reported by
     * {@link DbStats#getVlogFileSize()}.
     *
     * @return the on-disk size in bytes
     */
    public long getTotalDataSize() {
        return totalDataSize;
    }

    /**
     * Returns the average key length in bytes, over distinct keys.
     *
     * @return the average key size
     */
    public double getAvgKeySize() {
        return avgKeySize;
    }

    /**
     * Returns the average value length in bytes, over distinct keys.
     * Tombstones contribute zero.
     *
     * @return the average value size
     */
    public double getAvgValueSize() {
        return avgValueSize;
    }

    /**
     * Returns the point-lookup read amplification, the SSTables a worst-case get
     * may probe.
     *
     * @return the read amplification
     */
    public double getReadAmp() {
        return readAmp;
    }

    /**
     * Returns the total btree nodes across the column family.
     *
     * @return the node count
     */
    public long getBtreeTotalNodes() {
        return btreeTotalNodes;
    }

    /**
     * Returns the maximum btree height.
     *
     * @return the maximum height
     */
    public long getBtreeMaxHeight() {
        return btreeMaxHeight;
    }

    /**
     * Returns the average btree height.
     *
     * @return the average height
     */
    public double getBtreeAvgHeight() {
        return btreeAvgHeight;
    }

    /**
     * Returns the sum of tombstone counts across every SSTable.
     *
     * @return the tombstone count
     */
    public long getTotalTombstones() {
        return totalTombstones;
    }

    /**
     * Returns tombstones over total keys, or 0 when there are no keys.
     *
     * @return the tombstone ratio
     */
    public double getTombstoneRatio() {
        return tombstoneRatio;
    }

    /**
     * Returns the worst per-SSTable tombstone density observed.
     *
     * @return the maximum density
     */
    public double getMaxSstDensity() {
        return maxSstDensity;
    }

    /**
     * Returns the 1-based level where {@link #getMaxSstDensity()} was observed.
     *
     * @return the level, or 0 if none
     */
    public int getMaxSstDensityLevel() {
        return maxSstDensityLevel;
    }

    /**
     * Returns this family's share of the shared write-ahead log, as the encoded
     * size of its own entries. This is an attribution rather than a measurement:
     * the batch header and the block framing belong to no single family, so
     * these do not sum to what the log wrote. {@link DbStats#getWalBytesWritten()}
     * is the measured figure.
     *
     * @return the attributed WAL byte count
     */
    public long getWalBytesWritten() {
        return walBytesWritten;
    }

    /**
     * Returns the on-disk bytes this family's flushes wrote to L1.
     *
     * @return the flush byte count
     */
    public long getFlushBytesWritten() {
        return flushBytesWritten;
    }

    /**
     * Returns the on-disk bytes this family's compactions wrote.
     *
     * @return the compaction output byte count
     */
    public long getCompactionBytesWritten() {
        return compactionBytesWritten;
    }

    /**
     * Returns the on-disk bytes this family's compactions read as input.
     *
     * @return the compaction input byte count
     */
    public long getCompactionBytesRead() {
        return compactionBytesRead;
    }

    /**
     * Returns the logical key and value bytes committed to this family.
     *
     * @return the user byte count
     */
    public long getUserBytesWritten() {
        return userBytesWritten;
    }

    /**
     * Returns the number of compactions this family has run.
     *
     * @return the compaction count
     */
    public long getCompactionCount() {
        return compactionCount;
    }

    /**
     * Returns the distinct keys resident in the shared memtables for this family
     * and not yet in any SSTable.
     *
     * @return the unflushed key count
     */
    public long getUnflushedKeyCount() {
        return unflushedKeyCount;
    }

    /**
     * Returns the memory this family's partition range filters hold outside the
     * block cache. The filter bit arrays themselves are not counted here: those
     * are fetched per probe and live in the cache. A directory is built on a
     * table's first probe, so this reads zero for a family nothing has read from
     * yet and rises as tables are touched.
     *
     * @return the resident filter bytes
     */
    public long getFilterResidentBytes() {
        return filterResidentBytes;
    }

    @Override
    public String toString() {
        return "CfStats{" +
            "numLevels=" + numLevels +
            ", levelSizes=" + Arrays.toString(levelSizes) +
            ", levelNumSstables=" + Arrays.toString(levelNumSstables) +
            ", levelKeyCounts=" + Arrays.toString(levelKeyCounts) +
            ", levelTombstoneCounts=" + Arrays.toString(levelTombstoneCounts) +
            ", totalKeys=" + totalKeys +
            ", totalDataSize=" + totalDataSize +
            ", avgKeySize=" + avgKeySize +
            ", avgValueSize=" + avgValueSize +
            ", readAmp=" + readAmp +
            ", btreeTotalNodes=" + btreeTotalNodes +
            ", btreeMaxHeight=" + btreeMaxHeight +
            ", btreeAvgHeight=" + btreeAvgHeight +
            ", totalTombstones=" + totalTombstones +
            ", tombstoneRatio=" + tombstoneRatio +
            ", maxSstDensity=" + maxSstDensity +
            ", maxSstDensityLevel=" + maxSstDensityLevel +
            ", walBytesWritten=" + walBytesWritten +
            ", flushBytesWritten=" + flushBytesWritten +
            ", compactionBytesWritten=" + compactionBytesWritten +
            ", compactionBytesRead=" + compactionBytesRead +
            ", userBytesWritten=" + userBytesWritten +
            ", compactionCount=" + compactionCount +
            ", unflushedKeyCount=" + unflushedKeyCount +
            ", filterResidentBytes=" + filterResidentBytes +
            ", config=" + config +
            '}';
    }
}
