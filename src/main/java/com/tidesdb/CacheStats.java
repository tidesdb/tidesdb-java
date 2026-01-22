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
 * Statistics about the block cache.
 */
public class CacheStats {
    
    private final boolean enabled;
    private final long totalEntries;
    private final long totalBytes;
    private final long hits;
    private final long misses;
    private final double hitRate;
    private final long numPartitions;
    
    public CacheStats(boolean enabled, long totalEntries, long totalBytes, long hits, long misses, double hitRate, long numPartitions) {
        this.enabled = enabled;
        this.totalEntries = totalEntries;
        this.totalBytes = totalBytes;
        this.hits = hits;
        this.misses = misses;
        this.hitRate = hitRate;
        this.numPartitions = numPartitions;
    }
    
    /**
     * Returns whether the cache is enabled.
     *
     * @return true if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Gets the total number of entries in the cache.
     *
     * @return total entries
     */
    public long getTotalEntries() {
        return totalEntries;
    }
    
    /**
     * Gets the total bytes used by the cache.
     *
     * @return total bytes
     */
    public long getTotalBytes() {
        return totalBytes;
    }
    
    /**
     * Gets the number of cache hits.
     *
     * @return cache hits
     */
    public long getHits() {
        return hits;
    }
    
    /**
     * Gets the number of cache misses.
     *
     * @return cache misses
     */
    public long getMisses() {
        return misses;
    }
    
    /**
     * Gets the cache hit rate.
     *
     * @return hit rate (0.0 to 1.0)
     */
    public double getHitRate() {
        return hitRate;
    }
    
    /**
     * Gets the number of cache partitions.
     *
     * @return number of partitions
     */
    public long getNumPartitions() {
        return numPartitions;
    }
    
    @Override
    public String toString() {
        return "CacheStats{" +
            "enabled=" + enabled +
            ", totalEntries=" + totalEntries +
            ", totalBytes=" + totalBytes +
            ", hits=" + hits +
            ", misses=" + misses +
            ", hitRate=" + hitRate +
            ", numPartitions=" + numPartitions +
            '}';
    }
}
