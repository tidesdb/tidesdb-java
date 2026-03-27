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
    private final boolean unifiedMemtableEnabled;
    private final long unifiedMemtableBytes;
    private final int unifiedImmutableCount;
    private final boolean unifiedIsFlushing;
    private final int unifiedNextCfIndex;
    private final long unifiedWalGeneration;
    private final boolean objectStoreEnabled;
    private final String objectStoreConnector;
    private final long localCacheBytesUsed;
    private final long localCacheBytesMax;
    private final int localCacheNumFiles;
    private final long lastUploadedGeneration;
    private final long uploadQueueDepth;
    private final long totalUploads;
    private final long totalUploadFailures;
    private final boolean replicaMode;

    public DbStats(int numColumnFamilies, long totalMemory, long availableMemory,
                   long resolvedMemoryLimit, int memoryPressureLevel, int flushPendingCount,
                   long totalMemtableBytes, int totalImmutableCount, int totalSstableCount,
                   long totalDataSizeBytes, int numOpenSstables, long globalSeq,
                   long txnMemoryBytes, long compactionQueueSize, long flushQueueSize,
                   boolean unifiedMemtableEnabled, long unifiedMemtableBytes,
                   int unifiedImmutableCount, boolean unifiedIsFlushing,
                   int unifiedNextCfIndex, long unifiedWalGeneration,
                   boolean objectStoreEnabled, String objectStoreConnector,
                   long localCacheBytesUsed, long localCacheBytesMax, int localCacheNumFiles,
                   long lastUploadedGeneration, long uploadQueueDepth,
                   long totalUploads, long totalUploadFailures, boolean replicaMode) {
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
        this.unifiedMemtableEnabled = unifiedMemtableEnabled;
        this.unifiedMemtableBytes = unifiedMemtableBytes;
        this.unifiedImmutableCount = unifiedImmutableCount;
        this.unifiedIsFlushing = unifiedIsFlushing;
        this.unifiedNextCfIndex = unifiedNextCfIndex;
        this.unifiedWalGeneration = unifiedWalGeneration;
        this.objectStoreEnabled = objectStoreEnabled;
        this.objectStoreConnector = objectStoreConnector;
        this.localCacheBytesUsed = localCacheBytesUsed;
        this.localCacheBytesMax = localCacheBytesMax;
        this.localCacheNumFiles = localCacheNumFiles;
        this.lastUploadedGeneration = lastUploadedGeneration;
        this.uploadQueueDepth = uploadQueueDepth;
        this.totalUploads = totalUploads;
        this.totalUploadFailures = totalUploadFailures;
        this.replicaMode = replicaMode;
    }

    public int getNumColumnFamilies() {
        return numColumnFamilies;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public long getAvailableMemory() {
        return availableMemory;
    }

    public long getResolvedMemoryLimit() {
        return resolvedMemoryLimit;
    }

    public int getMemoryPressureLevel() {
        return memoryPressureLevel;
    }

    public int getFlushPendingCount() {
        return flushPendingCount;
    }

    public long getTotalMemtableBytes() {
        return totalMemtableBytes;
    }

    public int getTotalImmutableCount() {
        return totalImmutableCount;
    }

    public int getTotalSstableCount() {
        return totalSstableCount;
    }

    public long getTotalDataSizeBytes() {
        return totalDataSizeBytes;
    }

    public int getNumOpenSstables() {
        return numOpenSstables;
    }

    public long getGlobalSeq() {
        return globalSeq;
    }

    public long getTxnMemoryBytes() {
        return txnMemoryBytes;
    }

    public long getCompactionQueueSize() {
        return compactionQueueSize;
    }

    public long getFlushQueueSize() {
        return flushQueueSize;
    }

    public boolean isUnifiedMemtableEnabled() {
        return unifiedMemtableEnabled;
    }

    public long getUnifiedMemtableBytes() {
        return unifiedMemtableBytes;
    }

    public int getUnifiedImmutableCount() {
        return unifiedImmutableCount;
    }

    public boolean isUnifiedIsFlushing() {
        return unifiedIsFlushing;
    }

    public int getUnifiedNextCfIndex() {
        return unifiedNextCfIndex;
    }

    public long getUnifiedWalGeneration() {
        return unifiedWalGeneration;
    }

    public boolean isObjectStoreEnabled() {
        return objectStoreEnabled;
    }

    public String getObjectStoreConnector() {
        return objectStoreConnector;
    }

    public long getLocalCacheBytesUsed() {
        return localCacheBytesUsed;
    }

    public long getLocalCacheBytesMax() {
        return localCacheBytesMax;
    }

    public int getLocalCacheNumFiles() {
        return localCacheNumFiles;
    }

    public long getLastUploadedGeneration() {
        return lastUploadedGeneration;
    }

    public long getUploadQueueDepth() {
        return uploadQueueDepth;
    }

    public long getTotalUploads() {
        return totalUploads;
    }

    public long getTotalUploadFailures() {
        return totalUploadFailures;
    }

    public boolean isReplicaMode() {
        return replicaMode;
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
            ", unifiedMemtableEnabled=" + unifiedMemtableEnabled +
            ", unifiedMemtableBytes=" + unifiedMemtableBytes +
            ", unifiedImmutableCount=" + unifiedImmutableCount +
            ", unifiedIsFlushing=" + unifiedIsFlushing +
            ", unifiedNextCfIndex=" + unifiedNextCfIndex +
            ", unifiedWalGeneration=" + unifiedWalGeneration +
            ", objectStoreEnabled=" + objectStoreEnabled +
            ", objectStoreConnector='" + objectStoreConnector + '\'' +
            ", localCacheBytesUsed=" + localCacheBytesUsed +
            ", localCacheBytesMax=" + localCacheBytesMax +
            ", localCacheNumFiles=" + localCacheNumFiles +
            ", lastUploadedGeneration=" + lastUploadedGeneration +
            ", uploadQueueDepth=" + uploadQueueDepth +
            ", totalUploads=" + totalUploads +
            ", totalUploadFailures=" + totalUploadFailures +
            ", replicaMode=" + replicaMode +
            '}';
    }
}
