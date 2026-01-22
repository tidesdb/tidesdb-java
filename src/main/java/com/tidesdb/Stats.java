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
    
    public Stats(int numLevels, long memtableSize, long[] levelSizes, int[] levelNumSSTables, ColumnFamilyConfig config) {
        this.numLevels = numLevels;
        this.memtableSize = memtableSize;
        this.levelSizes = levelSizes;
        this.levelNumSSTables = levelNumSSTables;
        this.config = config;
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
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Stats{numLevels=").append(numLevels);
        sb.append(", memtableSize=").append(memtableSize);
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
        sb.append("}");
        return sb.toString();
    }
}
