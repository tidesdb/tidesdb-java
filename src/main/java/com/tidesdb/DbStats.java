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
 * Database-level aggregate statistics across the entire TidesDB instance.
 */
public class DbStats {
    
    private final int numColumnFamilies;
    private final long totalMemory;
    private final long availableMemory;
    private final long resolvedMemoryLimit;
    private final int memoryPressureLevel;
    private final int flushPendingCount;
    private final long totalMemtableBytes;
    private final int totalImmutableCount;
    private final int totalSstableCount;
    private final long totalDataSizeBytes;
    private final int numOpenSstables;
    private final long globalSeq;
    private final long txnMemoryBytes;
    private final long compactionQueueSize;
    private final long flushQueueSize;
    
    public DbStats(int numColumnFamilies, long totalMemory, long availableMemory,
                   long resolvedMemoryLimit, int memoryPressureLevel, int flushPendingCount,
                   long totalMemtableBytes, int totalImmutableCount, int totalSstableCount,
                   long totalDataSizeBytes, int numOpenSstables, long globalSeq,
                   long txnMemoryBytes, long compactionQueueSize, long flushQueueSize) {
        this.numColumnFamilies = numColumnFamilies;
        this.totalMemory = totalMemory;
        this.availableMemory = availableMemory;
        this.resolvedMemoryLimit = resolvedMemoryLimit;
        this.memoryPressureLevel = memoryPressureLevel;
        this.flushPendingCount = flushPendingCount;
        this.totalMemtableBytes = totalMemtableBytes;
        this.totalImmutableCount = totalImmutableCount;
        this.totalSstableCount = totalSstableCount;
        this.totalDataSizeBytes = totalDataSizeBytes;
        this.numOpenSstables = numOpenSstables;
        this.globalSeq = globalSeq;
        this.txnMemoryBytes = txnMemoryBytes;
        this.compactionQueueSize = compactionQueueSize;
        this.flushQueueSize = flushQueueSize;
    }
    
    /**
     * Gets the number of column families.
     *
     * @return number of column families
     */
    public int getNumColumnFamilies() {
        return numColumnFamilies;
    }
    
    /**
     * Gets the system total memory.
     *
     * @return total memory in bytes
     */
    public long getTotalMemory() {
        return totalMemory;
    }
    
    /**
     * Gets the system available memory at open time.
     *
     * @return available memory in bytes
     */
    public long getAvailableMemory() {
        return availableMemory;
    }
    
    /**
     * Gets the resolved memory limit (auto or configured).
     *
     * @return resolved memory limit in bytes
     */
    public long getResolvedMemoryLimit() {
        return resolvedMemoryLimit;
    }
    
    /**
     * Gets the current memory pressure level.
     * 0=normal, 1=elevated, 2=high, 3=critical.
     *
     * @return memory pressure level
     */
    public int getMemoryPressureLevel() {
        return memoryPressureLevel;
    }
    
    /**
     * Gets the number of pending flush operations (queued + in-flight).
     *
     * @return flush pending count
     */
    public int getFlushPendingCount() {
        return flushPendingCount;
    }
    
    /**
     * Gets the total bytes in active memtables across all column families.
     *
     * @return total memtable bytes
     */
    public long getTotalMemtableBytes() {
        return totalMemtableBytes;
    }
    
    /**
     * Gets the total immutable memtables across all column families.
     *
     * @return total immutable count
     */
    public int getTotalImmutableCount() {
        return totalImmutableCount;
    }
    
    /**
     * Gets the total SSTables across all column families and levels.
     *
     * @return total SSTable count
     */
    public int getTotalSstableCount() {
        return totalSstableCount;
    }
    
    /**
     * Gets the total data size (klog + vlog) across all column families.
     *
     * @return total data size in bytes
     */
    public long getTotalDataSizeBytes() {
        return totalDataSizeBytes;
    }
    
    /**
     * Gets the number of currently open SSTable file handles.
     *
     * @return number of open SSTables
     */
    public int getNumOpenSstables() {
        return numOpenSstables;
    }
    
    /**
     * Gets the current global sequence number.
     *
     * @return global sequence number
     */
    public long getGlobalSeq() {
        return globalSeq;
    }
    
    /**
     * Gets the bytes held by in-flight transactions.
     *
     * @return transaction memory bytes
     */
    public long getTxnMemoryBytes() {
        return txnMemoryBytes;
    }
    
    /**
     * Gets the number of pending compaction tasks.
     *
     * @return compaction queue size
     */
    public long getCompactionQueueSize() {
        return compactionQueueSize;
    }
    
    /**
     * Gets the number of pending flush tasks in queue.
     *
     * @return flush queue size
     */
    public long getFlushQueueSize() {
        return flushQueueSize;
    }
    
    @Override
    public String toString() {
        return "DbStats{" +
            "numColumnFamilies=" + numColumnFamilies +
            ", totalMemory=" + totalMemory +
            ", availableMemory=" + availableMemory +
            ", resolvedMemoryLimit=" + resolvedMemoryLimit +
            ", memoryPressureLevel=" + memoryPressureLevel +
            ", flushPendingCount=" + flushPendingCount +
            ", totalMemtableBytes=" + totalMemtableBytes +
            ", totalImmutableCount=" + totalImmutableCount +
            ", totalSstableCount=" + totalSstableCount +
            ", totalDataSizeBytes=" + totalDataSizeBytes +
            ", numOpenSstables=" + numOpenSstables +
            ", globalSeq=" + globalSeq +
            ", txnMemoryBytes=" + txnMemoryBytes +
            ", compactionQueueSize=" + compactionQueueSize +
            ", flushQueueSize=" + flushQueueSize +
            '}';
    }
}
