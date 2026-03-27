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
 * Configuration for object store mode behavior.
 */
public class ObjectStoreConfig {

    private String localCachePath;
    private long localCacheMaxBytes;
    private boolean cacheOnRead;
    private boolean cacheOnWrite;
    private int maxConcurrentUploads;
    private int maxConcurrentDownloads;
    private long multipartThreshold;
    private long multipartPartSize;
    private boolean syncManifestToObject;
    private boolean replicateWal;
    private boolean walUploadSync;
    private long walSyncThresholdBytes;
    private boolean walSyncOnCommit;
    private boolean replicaMode;
    private long replicaSyncIntervalUs;
    private boolean replicaReplayWal;

    private ObjectStoreConfig(Builder builder) {
        this.localCachePath = builder.localCachePath;
        this.localCacheMaxBytes = builder.localCacheMaxBytes;
        this.cacheOnRead = builder.cacheOnRead;
        this.cacheOnWrite = builder.cacheOnWrite;
        this.maxConcurrentUploads = builder.maxConcurrentUploads;
        this.maxConcurrentDownloads = builder.maxConcurrentDownloads;
        this.multipartThreshold = builder.multipartThreshold;
        this.multipartPartSize = builder.multipartPartSize;
        this.syncManifestToObject = builder.syncManifestToObject;
        this.replicateWal = builder.replicateWal;
        this.walUploadSync = builder.walUploadSync;
        this.walSyncThresholdBytes = builder.walSyncThresholdBytes;
        this.walSyncOnCommit = builder.walSyncOnCommit;
        this.replicaMode = builder.replicaMode;
        this.replicaSyncIntervalUs = builder.replicaSyncIntervalUs;
        this.replicaReplayWal = builder.replicaReplayWal;
    }

    /**
     * Creates a default object store configuration matching tidesdb_objstore_default_config().
     *
     * @return a new ObjectStoreConfig with default values
     */
    public static ObjectStoreConfig defaultConfig() {
        return new Builder().build();
    }

    /**
     * Creates a new builder for ObjectStoreConfig.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public String getLocalCachePath() { return localCachePath; }
    public long getLocalCacheMaxBytes() { return localCacheMaxBytes; }
    public boolean isCacheOnRead() { return cacheOnRead; }
    public boolean isCacheOnWrite() { return cacheOnWrite; }
    public int getMaxConcurrentUploads() { return maxConcurrentUploads; }
    public int getMaxConcurrentDownloads() { return maxConcurrentDownloads; }
    public long getMultipartThreshold() { return multipartThreshold; }
    public long getMultipartPartSize() { return multipartPartSize; }
    public boolean isSyncManifestToObject() { return syncManifestToObject; }
    public boolean isReplicateWal() { return replicateWal; }
    public boolean isWalUploadSync() { return walUploadSync; }
    public long getWalSyncThresholdBytes() { return walSyncThresholdBytes; }
    public boolean isWalSyncOnCommit() { return walSyncOnCommit; }
    public boolean isReplicaMode() { return replicaMode; }
    public long getReplicaSyncIntervalUs() { return replicaSyncIntervalUs; }
    public boolean isReplicaReplayWal() { return replicaReplayWal; }

    /**
     * Builder for ObjectStoreConfig.
     */
    public static class Builder {
        private String localCachePath = null;
        private long localCacheMaxBytes = 0;
        private boolean cacheOnRead = true;
        private boolean cacheOnWrite = true;
        private int maxConcurrentUploads = 4;
        private int maxConcurrentDownloads = 8;
        private long multipartThreshold = 64 * 1024 * 1024;
        private long multipartPartSize = 8 * 1024 * 1024;
        private boolean syncManifestToObject = true;
        private boolean replicateWal = true;
        private boolean walUploadSync = false;
        private long walSyncThresholdBytes = 1048576;
        private boolean walSyncOnCommit = false;
        private boolean replicaMode = false;
        private long replicaSyncIntervalUs = 5000000;
        private boolean replicaReplayWal = true;

        public Builder localCachePath(String localCachePath) {
            this.localCachePath = localCachePath;
            return this;
        }

        public Builder localCacheMaxBytes(long localCacheMaxBytes) {
            this.localCacheMaxBytes = localCacheMaxBytes;
            return this;
        }

        public Builder cacheOnRead(boolean cacheOnRead) {
            this.cacheOnRead = cacheOnRead;
            return this;
        }

        public Builder cacheOnWrite(boolean cacheOnWrite) {
            this.cacheOnWrite = cacheOnWrite;
            return this;
        }

        public Builder maxConcurrentUploads(int maxConcurrentUploads) {
            this.maxConcurrentUploads = maxConcurrentUploads;
            return this;
        }

        public Builder maxConcurrentDownloads(int maxConcurrentDownloads) {
            this.maxConcurrentDownloads = maxConcurrentDownloads;
            return this;
        }

        public Builder multipartThreshold(long multipartThreshold) {
            this.multipartThreshold = multipartThreshold;
            return this;
        }

        public Builder multipartPartSize(long multipartPartSize) {
            this.multipartPartSize = multipartPartSize;
            return this;
        }

        public Builder syncManifestToObject(boolean syncManifestToObject) {
            this.syncManifestToObject = syncManifestToObject;
            return this;
        }

        public Builder replicateWal(boolean replicateWal) {
            this.replicateWal = replicateWal;
            return this;
        }

        public Builder walUploadSync(boolean walUploadSync) {
            this.walUploadSync = walUploadSync;
            return this;
        }

        public Builder walSyncThresholdBytes(long walSyncThresholdBytes) {
            this.walSyncThresholdBytes = walSyncThresholdBytes;
            return this;
        }

        public Builder walSyncOnCommit(boolean walSyncOnCommit) {
            this.walSyncOnCommit = walSyncOnCommit;
            return this;
        }

        public Builder replicaMode(boolean replicaMode) {
            this.replicaMode = replicaMode;
            return this;
        }

        public Builder replicaSyncIntervalUs(long replicaSyncIntervalUs) {
            this.replicaSyncIntervalUs = replicaSyncIntervalUs;
            return this;
        }

        public Builder replicaReplayWal(boolean replicaReplayWal) {
            this.replicaReplayWal = replicaReplayWal;
            return this;
        }

        public ObjectStoreConfig build() {
            return new ObjectStoreConfig(this);
        }
    }
}
