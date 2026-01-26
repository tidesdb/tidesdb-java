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
 * Statistics about a column family.
 */
public class Stats {
    
    private final int numLevels;
    private final long memtableSize;
    private final long[] levelSizes;
    private final int[] levelNumSSTables;
    private final ColumnFamilyConfig config;
    private final long totalKeys;
    private final long totalDataSize;
    private final double avgKeySize;
    private final double avgValueSize;
    private final long[] levelKeyCounts;
    private final double readAmp;
    private final double hitRate;
    
    public Stats(int numLevels, long memtableSize, long[] levelSizes, int[] levelNumSSTables, 
                 ColumnFamilyConfig config, long totalKeys, long totalDataSize, 
                 double avgKeySize, double avgValueSize, long[] levelKeyCounts,
                 double readAmp, double hitRate) {
        this.numLevels = numLevels;
        this.memtableSize = memtableSize;
        this.levelSizes = levelSizes;
        this.levelNumSSTables = levelNumSSTables;
        this.config = config;
        this.totalKeys = totalKeys;
        this.totalDataSize = totalDataSize;
        this.avgKeySize = avgKeySize;
        this.avgValueSize = avgValueSize;
        this.levelKeyCounts = levelKeyCounts;
        this.readAmp = readAmp;
        this.hitRate = hitRate;
    }
    
    /**
     * Gets the number of levels.
     *
     * @return the number of levels
     */
    public int getNumLevels() {
        return numLevels;
    }
    
    /**
     * Gets the memtable size in bytes.
     *
     * @return the memtable size
     */
    public long getMemtableSize() {
        return memtableSize;
    }
    
    /**
     * Gets the sizes of each level in bytes.
     *
     * @return array of level sizes
     */
    public long[] getLevelSizes() {
        return levelSizes;
    }
    
    /**
     * Gets the number of SSTables at each level.
     *
     * @return array of SSTable counts per level
     */
    public int[] getLevelNumSSTables() {
        return levelNumSSTables;
    }
    
    /**
     * Gets the column family configuration.
     *
     * @return the configuration
     */
    public ColumnFamilyConfig getConfig() {
        return config;
    }
    
    /**
     * Gets the total number of keys across memtable and all SSTables.
     *
     * @return total key count
     */
    public long getTotalKeys() {
        return totalKeys;
    }
    
    /**
     * Gets the total data size (klog + vlog) across all SSTables.
     *
     * @return total data size in bytes
     */
    public long getTotalDataSize() {
        return totalDataSize;
    }
    
    /**
     * Gets the average key size in bytes.
     *
     * @return average key size
     */
    public double getAvgKeySize() {
        return avgKeySize;
    }
    
    /**
     * Gets the average value size in bytes.
     *
     * @return average value size
     */
    public double getAvgValueSize() {
        return avgValueSize;
    }
    
    /**
     * Gets the number of keys per level.
     *
     * @return array of key counts per level
     */
    public long[] getLevelKeyCounts() {
        return levelKeyCounts;
    }
    
    /**
     * Gets the read amplification (point lookup cost multiplier).
     *
     * @return read amplification factor
     */
    public double getReadAmp() {
        return readAmp;
    }
    
    /**
     * Gets the cache hit rate for this column family.
     *
     * @return hit rate (0.0 to 1.0), or 0.0 if cache is disabled
     */
    public double getHitRate() {
        return hitRate;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Stats{numLevels=").append(numLevels);
        sb.append(", memtableSize=").append(memtableSize);
        sb.append(", totalKeys=").append(totalKeys);
        sb.append(", totalDataSize=").append(totalDataSize);
        sb.append(", avgKeySize=").append(avgKeySize);
        sb.append(", avgValueSize=").append(avgValueSize);
        sb.append(", readAmp=").append(readAmp);
        sb.append(", hitRate=").append(hitRate);
        if (levelSizes != null) {
            sb.append(", levelSizes=[");
            for (int i = 0; i < levelSizes.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(levelSizes[i]);
            }
            sb.append("]");
        }
        if (levelNumSSTables != null) {
            sb.append(", levelNumSSTables=[");
            for (int i = 0; i < levelNumSSTables.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(levelNumSSTables[i]);
            }
            sb.append("]");
        }
        if (levelKeyCounts != null) {
            sb.append(", levelKeyCounts=[");
            for (int i = 0; i < levelKeyCounts.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(levelKeyCounts[i]);
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
