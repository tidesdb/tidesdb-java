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
 * Configuration for opening a TidesDB instance.
 */
public class Config {

    static {
        NativeLibrary.load();
    }

    private String dbPath;
    private int numFlushThreads;
    private int numCompactionThreads;
    private LogLevel logLevel;
    private long blockCacheSize;
    private long maxOpenSSTables;
    private boolean logToFile;
    private long logTruncationAt;
    private long maxMemoryUsage;
    private boolean unifiedMemtable;
    private long unifiedMemtableWriteBufferSize;
    private int unifiedMemtableSkipListMaxLevel;
    private float unifiedMemtableSkipListProbability;
    private int unifiedMemtableSyncMode;
    private long unifiedMemtableSyncIntervalUs;
    private String objectStoreFsPath;
    private ObjectStoreConfig objectStoreConfig;
    private int maxConcurrentFlushes;

    private Config(Builder builder) {
        this.dbPath = builder.dbPath;
        this.numFlushThreads = builder.numFlushThreads;
        this.numCompactionThreads = builder.numCompactionThreads;
        this.logLevel = builder.logLevel;
        this.blockCacheSize = builder.blockCacheSize;
        this.maxOpenSSTables = builder.maxOpenSSTables;
        this.logToFile = builder.logToFile;
        this.logTruncationAt = builder.logTruncationAt;
        this.maxMemoryUsage = builder.maxMemoryUsage;
        this.unifiedMemtable = builder.unifiedMemtable;
        this.unifiedMemtableWriteBufferSize = builder.unifiedMemtableWriteBufferSize;
        this.unifiedMemtableSkipListMaxLevel = builder.unifiedMemtableSkipListMaxLevel;
        this.unifiedMemtableSkipListProbability = builder.unifiedMemtableSkipListProbability;
        this.unifiedMemtableSyncMode = builder.unifiedMemtableSyncMode;
        this.unifiedMemtableSyncIntervalUs = builder.unifiedMemtableSyncIntervalUs;
        this.objectStoreFsPath = builder.objectStoreFsPath;
        this.objectStoreConfig = builder.objectStoreConfig;
        this.maxConcurrentFlushes = builder.maxConcurrentFlushes;
    }

    /**
     * Creates a default configuration. The {@code maxConcurrentFlushes} default is
     * sourced from the underlying C library via {@code tidesdb_default_config()} so
     * the binding tracks the engine's defaults automatically.
     *
     * @return a new Config with default values
     */
    public static Config defaultConfig() {
        return new Builder()
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .logToFile(false)
            .logTruncationAt(24 * 1024 * 1024)
            .maxMemoryUsage(0)
            .maxConcurrentFlushes(nativeDefaultMaxConcurrentFlushes())
            .build();
    }

    private static native int nativeDefaultMaxConcurrentFlushes();
    
    /**
     * Creates a new builder for Config.
     *
     * @param dbPath the database path
     * @return a new Builder
     */
    public static Builder builder(String dbPath) {
        return new Builder().dbPath(dbPath);
    }
    
    public String getDbPath() {
        return dbPath;
    }
    
    public int getNumFlushThreads() {
        return numFlushThreads;
    }
    
    public int getNumCompactionThreads() {
        return numCompactionThreads;
    }
    
    public LogLevel getLogLevel() {
        return logLevel;
    }
    
    public long getBlockCacheSize() {
        return blockCacheSize;
    }
    
    public long getMaxOpenSSTables() {
        return maxOpenSSTables;
    }
    
    public boolean isLogToFile() {
        return logToFile;
    }
    
    public long getLogTruncationAt() {
        return logTruncationAt;
    }
    
    public long getMaxMemoryUsage() {
        return maxMemoryUsage;
    }

    public boolean isUnifiedMemtable() {
        return unifiedMemtable;
    }

    public long getUnifiedMemtableWriteBufferSize() {
        return unifiedMemtableWriteBufferSize;
    }

    public int getUnifiedMemtableSkipListMaxLevel() {
        return unifiedMemtableSkipListMaxLevel;
    }

    public float getUnifiedMemtableSkipListProbability() {
        return unifiedMemtableSkipListProbability;
    }

    public int getUnifiedMemtableSyncMode() {
        return unifiedMemtableSyncMode;
    }

    public long getUnifiedMemtableSyncIntervalUs() {
        return unifiedMemtableSyncIntervalUs;
    }

    public String getObjectStoreFsPath() {
        return objectStoreFsPath;
    }

    public ObjectStoreConfig getObjectStoreConfig() {
        return objectStoreConfig;
    }

    public int getMaxConcurrentFlushes() {
        return maxConcurrentFlushes;
    }

    /**
     * Builder for Config.
     */
    public static class Builder {
        private String dbPath = "";
        private int numFlushThreads = 2;
        private int numCompactionThreads = 2;
        private LogLevel logLevel = LogLevel.INFO;
        private long blockCacheSize = 64 * 1024 * 1024;
        private long maxOpenSSTables = 256;
        private boolean logToFile = false;
        private long logTruncationAt = 24 * 1024 * 1024;
        private long maxMemoryUsage = 0;
        private boolean unifiedMemtable = false;
        private long unifiedMemtableWriteBufferSize = 0;
        private int unifiedMemtableSkipListMaxLevel = 0;
        private float unifiedMemtableSkipListProbability = 0;
        private int unifiedMemtableSyncMode = 0;
        private long unifiedMemtableSyncIntervalUs = 0;
        private String objectStoreFsPath = null;
        private ObjectStoreConfig objectStoreConfig = null;
        private int maxConcurrentFlushes = 0;

        public Builder dbPath(String dbPath) {
            this.dbPath = dbPath;
            return this;
        }
        
        public Builder numFlushThreads(int numFlushThreads) {
            this.numFlushThreads = numFlushThreads;
            return this;
        }
        
        public Builder numCompactionThreads(int numCompactionThreads) {
            this.numCompactionThreads = numCompactionThreads;
            return this;
        }
        
        public Builder logLevel(LogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }
        
        public Builder blockCacheSize(long blockCacheSize) {
            this.blockCacheSize = blockCacheSize;
            return this;
        }
        
        public Builder maxOpenSSTables(long maxOpenSSTables) {
            this.maxOpenSSTables = maxOpenSSTables;
            return this;
        }
        
        public Builder logToFile(boolean logToFile) {
            this.logToFile = logToFile;
            return this;
        }
        
        public Builder logTruncationAt(long logTruncationAt) {
            this.logTruncationAt = logTruncationAt;
            return this;
        }
        
        public Builder maxMemoryUsage(long maxMemoryUsage) {
            this.maxMemoryUsage = maxMemoryUsage;
            return this;
        }

        public Builder unifiedMemtable(boolean unifiedMemtable) {
            this.unifiedMemtable = unifiedMemtable;
            return this;
        }

        public Builder unifiedMemtableWriteBufferSize(long unifiedMemtableWriteBufferSize) {
            this.unifiedMemtableWriteBufferSize = unifiedMemtableWriteBufferSize;
            return this;
        }

        public Builder unifiedMemtableSkipListMaxLevel(int unifiedMemtableSkipListMaxLevel) {
            this.unifiedMemtableSkipListMaxLevel = unifiedMemtableSkipListMaxLevel;
            return this;
        }

        public Builder unifiedMemtableSkipListProbability(float unifiedMemtableSkipListProbability) {
            this.unifiedMemtableSkipListProbability = unifiedMemtableSkipListProbability;
            return this;
        }

        public Builder unifiedMemtableSyncMode(int unifiedMemtableSyncMode) {
            this.unifiedMemtableSyncMode = unifiedMemtableSyncMode;
            return this;
        }

        public Builder unifiedMemtableSyncIntervalUs(long unifiedMemtableSyncIntervalUs) {
            this.unifiedMemtableSyncIntervalUs = unifiedMemtableSyncIntervalUs;
            return this;
        }

        public Builder objectStoreFsPath(String objectStoreFsPath) {
            this.objectStoreFsPath = objectStoreFsPath;
            return this;
        }

        public Builder objectStoreConfig(ObjectStoreConfig objectStoreConfig) {
            this.objectStoreConfig = objectStoreConfig;
            return this;
        }

        public Builder maxConcurrentFlushes(int maxConcurrentFlushes) {
            this.maxConcurrentFlushes = maxConcurrentFlushes;
            return this;
        }

        public Config build() {
            validate();
            return new Config(this);
        }

        private void validate() throws IllegalArgumentException {
            if (dbPath == null) {
                throw new IllegalArgumentException("Database path cannot be null");
            }
            if (dbPath.isEmpty()) {
                dbPath = "";
            }
            if (numFlushThreads <= 0) {
                throw new IllegalArgumentException("Number of flush threads must be positive");
            }
            if (numCompactionThreads <= 0) {
                throw new IllegalArgumentException("Number of compaction threads must be positive");
            }
            if (logLevel == null) {
                throw new IllegalArgumentException("Log level cannot be null");
            }
            if (blockCacheSize < 0) {
                throw new IllegalArgumentException("Block cache size cannot be negative");
            }
            if (maxOpenSSTables <= 0) {
                throw new IllegalArgumentException("Max open SSTables must be positive");
            }
        }
    }
}
