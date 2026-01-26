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
 * Configuration for a column family.
 */
public class ColumnFamilyConfig {
    
    private long writeBufferSize;
    private long levelSizeRatio;
    private int minLevels;
    private int dividingLevelOffset;
    private long klogValueThreshold;
    private CompressionAlgorithm compressionAlgorithm;
    private boolean enableBloomFilter;
    private double bloomFPR;
    private boolean enableBlockIndexes;
    private int indexSampleRatio;
    private int blockIndexPrefixLen;
    private SyncMode syncMode;
    private long syncIntervalUs;
    private String comparatorName;
    private int skipListMaxLevel;
    private float skipListProbability;
    private IsolationLevel defaultIsolationLevel;
    private long minDiskSpace;
    private int l1FileCountTrigger;
    private int l0QueueStallThreshold;
    
    private ColumnFamilyConfig(Builder builder) {
        this.writeBufferSize = builder.writeBufferSize;
        this.levelSizeRatio = builder.levelSizeRatio;
        this.minLevels = builder.minLevels;
        this.dividingLevelOffset = builder.dividingLevelOffset;
        this.klogValueThreshold = builder.klogValueThreshold;
        this.compressionAlgorithm = builder.compressionAlgorithm;
        this.enableBloomFilter = builder.enableBloomFilter;
        this.bloomFPR = builder.bloomFPR;
        this.enableBlockIndexes = builder.enableBlockIndexes;
        this.indexSampleRatio = builder.indexSampleRatio;
        this.blockIndexPrefixLen = builder.blockIndexPrefixLen;
        this.syncMode = builder.syncMode;
        this.syncIntervalUs = builder.syncIntervalUs;
        this.comparatorName = builder.comparatorName;
        this.skipListMaxLevel = builder.skipListMaxLevel;
        this.skipListProbability = builder.skipListProbability;
        this.defaultIsolationLevel = builder.defaultIsolationLevel;
        this.minDiskSpace = builder.minDiskSpace;
        this.l1FileCountTrigger = builder.l1FileCountTrigger;
        this.l0QueueStallThreshold = builder.l0QueueStallThreshold;
    }
    
    /**
     * Creates a default column family configuration.
     *
     * @return a new ColumnFamilyConfig with default values
     */
    public static ColumnFamilyConfig defaultConfig() {
        return new Builder()
            .writeBufferSize(128 * 1024 * 1024)
            .levelSizeRatio(10)
            .minLevels(5)
            .dividingLevelOffset(2)
            .klogValueThreshold(512)
            .compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)
            .enableBloomFilter(true)
            .bloomFPR(0.01)
            .enableBlockIndexes(true)
            .indexSampleRatio(1)
            .blockIndexPrefixLen(16)
            .syncMode(SyncMode.SYNC_FULL)
            .syncIntervalUs(1000000)
            .comparatorName("")
            .skipListMaxLevel(12)
            .skipListProbability(0.25f)
            .defaultIsolationLevel(IsolationLevel.READ_COMMITTED)
            .minDiskSpace(100 * 1024 * 1024)
            .l1FileCountTrigger(4)
            .l0QueueStallThreshold(20)
            .build();
    }
    
    /**
     * Creates a new builder for ColumnFamilyConfig.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    public long getWriteBufferSize() { return writeBufferSize; }
    public long getLevelSizeRatio() { return levelSizeRatio; }
    public int getMinLevels() { return minLevels; }
    public int getDividingLevelOffset() { return dividingLevelOffset; }
    public long getKlogValueThreshold() { return klogValueThreshold; }
    public CompressionAlgorithm getCompressionAlgorithm() { return compressionAlgorithm; }
    public boolean isEnableBloomFilter() { return enableBloomFilter; }
    public double getBloomFPR() { return bloomFPR; }
    public boolean isEnableBlockIndexes() { return enableBlockIndexes; }
    public int getIndexSampleRatio() { return indexSampleRatio; }
    public int getBlockIndexPrefixLen() { return blockIndexPrefixLen; }
    public SyncMode getSyncMode() { return syncMode; }
    public long getSyncIntervalUs() { return syncIntervalUs; }
    public String getComparatorName() { return comparatorName; }
    public int getSkipListMaxLevel() { return skipListMaxLevel; }
    public float getSkipListProbability() { return skipListProbability; }
    public IsolationLevel getDefaultIsolationLevel() { return defaultIsolationLevel; }
    public long getMinDiskSpace() { return minDiskSpace; }
    public int getL1FileCountTrigger() { return l1FileCountTrigger; }
    public int getL0QueueStallThreshold() { return l0QueueStallThreshold; }
    
    /**
     * Builder for ColumnFamilyConfig.
     */
    public static class Builder {
        private long writeBufferSize = 128 * 1024 * 1024;
        private long levelSizeRatio = 10;
        private int minLevels = 5;
        private int dividingLevelOffset = 2;
        private long klogValueThreshold = 512;
        private CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.LZ4_COMPRESSION;
        private boolean enableBloomFilter = true;
        private double bloomFPR = 0.01;
        private boolean enableBlockIndexes = true;
        private int indexSampleRatio = 1;
        private int blockIndexPrefixLen = 16;
        private SyncMode syncMode = SyncMode.SYNC_FULL;
        private long syncIntervalUs = 1000000;
        private String comparatorName = "";
        private int skipListMaxLevel = 12;
        private float skipListProbability = 0.25f;
        private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_COMMITTED;
        private long minDiskSpace = 100 * 1024 * 1024;
        private int l1FileCountTrigger = 4;
        private int l0QueueStallThreshold = 20;
        
        public Builder writeBufferSize(long writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }
        
        public Builder levelSizeRatio(long levelSizeRatio) {
            this.levelSizeRatio = levelSizeRatio;
            return this;
        }
        
        public Builder minLevels(int minLevels) {
            this.minLevels = minLevels;
            return this;
        }
        
        public Builder dividingLevelOffset(int dividingLevelOffset) {
            this.dividingLevelOffset = dividingLevelOffset;
            return this;
        }
        
        public Builder klogValueThreshold(long klogValueThreshold) {
            this.klogValueThreshold = klogValueThreshold;
            return this;
        }
        
        public Builder compressionAlgorithm(CompressionAlgorithm compressionAlgorithm) {
            this.compressionAlgorithm = compressionAlgorithm;
            return this;
        }
        
        public Builder enableBloomFilter(boolean enableBloomFilter) {
            this.enableBloomFilter = enableBloomFilter;
            return this;
        }
        
        public Builder bloomFPR(double bloomFPR) {
            this.bloomFPR = bloomFPR;
            return this;
        }
        
        public Builder enableBlockIndexes(boolean enableBlockIndexes) {
            this.enableBlockIndexes = enableBlockIndexes;
            return this;
        }
        
        public Builder indexSampleRatio(int indexSampleRatio) {
            this.indexSampleRatio = indexSampleRatio;
            return this;
        }
        
        public Builder blockIndexPrefixLen(int blockIndexPrefixLen) {
            this.blockIndexPrefixLen = blockIndexPrefixLen;
            return this;
        }
        
        public Builder syncMode(SyncMode syncMode) {
            this.syncMode = syncMode;
            return this;
        }
        
        public Builder syncIntervalUs(long syncIntervalUs) {
            this.syncIntervalUs = syncIntervalUs;
            return this;
        }
        
        public Builder comparatorName(String comparatorName) {
            this.comparatorName = comparatorName;
            return this;
        }
        
        public Builder skipListMaxLevel(int skipListMaxLevel) {
            this.skipListMaxLevel = skipListMaxLevel;
            return this;
        }
        
        public Builder skipListProbability(float skipListProbability) {
            this.skipListProbability = skipListProbability;
            return this;
        }
        
        public Builder defaultIsolationLevel(IsolationLevel defaultIsolationLevel) {
            this.defaultIsolationLevel = defaultIsolationLevel;
            return this;
        }
        
        public Builder minDiskSpace(long minDiskSpace) {
            this.minDiskSpace = minDiskSpace;
            return this;
        }
        
        public Builder l1FileCountTrigger(int l1FileCountTrigger) {
            this.l1FileCountTrigger = l1FileCountTrigger;
            return this;
        }
        
        public Builder l0QueueStallThreshold(int l0QueueStallThreshold) {
            this.l0QueueStallThreshold = l0QueueStallThreshold;
            return this;
        }
        
        public ColumnFamilyConfig build() {
            return new ColumnFamilyConfig(this);
        }
    }
}
