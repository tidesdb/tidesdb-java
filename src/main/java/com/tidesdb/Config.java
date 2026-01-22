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
    
    private String dbPath;
    private int numFlushThreads;
    private int numCompactionThreads;
    private LogLevel logLevel;
    private long blockCacheSize;
    private long maxOpenSSTables;
    
    private Config(Builder builder) {
        this.dbPath = builder.dbPath;
        this.numFlushThreads = builder.numFlushThreads;
        this.numCompactionThreads = builder.numCompactionThreads;
        this.logLevel = builder.logLevel;
        this.blockCacheSize = builder.blockCacheSize;
        this.maxOpenSSTables = builder.maxOpenSSTables;
    }
    
    /**
     * Creates a default configuration.
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
            .build();
    }
    
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
        
        public Config build() {
            return new Config(this);
        }
    }
}
