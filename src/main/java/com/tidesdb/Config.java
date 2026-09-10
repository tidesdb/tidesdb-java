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
 * Database-level configuration for opening a {@link TidesDB} instance. The
 * memtable, write-ahead log, block cache, value log, and worker pool are
 * database-level and shared by every column family, so their settings live here
 * rather than on {@link ColumnFamilyConfig}.
 *
 * <p>Use {@link #builder(String)} to construct a configuration with custom
 * values, or {@link #defaultConfig(String)} to obtain one carrying the native
 * library's own defaults.
 *
 * <p>Instances are immutable once built. {@link Builder#build()} validates all
 * fields and throws {@link IllegalArgumentException} for invalid values.
 *
 * <p>A field left at zero is resolved to its default by the engine rather than
 * taken literally, for {@code numFlushThreads}, {@code numCompactionThreads},
 * {@code blockCacheSize}, {@code maxOpenSSTables}, {@code vlogSegmentSize},
 * {@code memtableWriteBufferSize}, {@code memtableSkipListMaxLevel} and
 * {@code memtableSkipListProbability}. The rest are used as given,
 * {@code memtableSyncMode} included, where zero is the meaningful value
 * {@link SyncMode#SYNC_NONE}.
 */
public class Config {

    static {
        NativeLibrary.load();
    }

    private final String dbPath;
    private final int numFlushThreads;
    private final int numCompactionThreads;
    private final LogLevel logLevel;
    private final long blockCacheSize;
    private final long maxOpenSSTables;
    private final boolean logToFile;
    private final long logTruncationAt;
    private final long memtableWriteBufferSize;
    private final int memtableSkipListMaxLevel;
    private final float memtableSkipListProbability;
    private final SyncMode memtableSyncMode;
    private final long memtableSyncIntervalUs;
    private final long valueSeparationThreshold;
    private final long vlogSegmentSize;
    private final int memtableL0QueueStallThreshold;
    private final int memtableIdleFlushSeconds;
    private final long txnTimeoutSeconds;

    private Config(Builder builder) {
        this.dbPath = builder.dbPath;
        this.numFlushThreads = builder.numFlushThreads;
        this.numCompactionThreads = builder.numCompactionThreads;
        this.logLevel = builder.logLevel;
        this.blockCacheSize = builder.blockCacheSize;
        this.maxOpenSSTables = builder.maxOpenSSTables;
        this.logToFile = builder.logToFile;
        this.logTruncationAt = builder.logTruncationAt;
        this.memtableWriteBufferSize = builder.memtableWriteBufferSize;
        this.memtableSkipListMaxLevel = builder.memtableSkipListMaxLevel;
        this.memtableSkipListProbability = builder.memtableSkipListProbability;
        this.memtableSyncMode = builder.memtableSyncMode;
        this.memtableSyncIntervalUs = builder.memtableSyncIntervalUs;
        this.valueSeparationThreshold = builder.valueSeparationThreshold;
        this.vlogSegmentSize = builder.vlogSegmentSize;
        this.memtableL0QueueStallThreshold = builder.memtableL0QueueStallThreshold;
        this.memtableIdleFlushSeconds = builder.memtableIdleFlushSeconds;
        this.txnTimeoutSeconds = builder.txnTimeoutSeconds;
    }

    /**
     * Creates a configuration carrying the native library's own defaults, as
     * returned by {@code tidesdb_default_config()}, with {@code dbPath} applied
     * on top. Use {@link #toBuilder()} to adjust individual fields.
     *
     * @param dbPath the database file-system path; must not be {@code null} or empty
     * @return a new {@code Config} holding the native defaults
     */
    public static Config defaultConfig(String dbPath) {
        if (dbPath == null || dbPath.isEmpty()) {
            throw new IllegalArgumentException("Database path cannot be null or empty");
        }
        return nativeDefaultConfig().toBuilder().dbPath(dbPath).build();
    }

    /**
     * Reads {@code tidesdb_default_config()}. The returned configuration carries
     * an empty {@code dbPath}.
     */
    private static native Config nativeDefaultConfig();

    /**
     * Assembles a configuration from the flat field list the JNI bridge reads
     * out of {@code tidesdb_config_t}. Called from native code only.
     */
    static Config fromNative(int numFlushThreads, int numCompactionThreads, int logLevel,
                             long blockCacheSize, long maxOpenSSTables, boolean logToFile,
                             long logTruncationAt, long memtableWriteBufferSize,
                             int memtableSkipListMaxLevel, float memtableSkipListProbability,
                             int memtableSyncMode, long memtableSyncIntervalUs,
                             long valueSeparationThreshold, long vlogSegmentSize,
                             int memtableL0QueueStallThreshold, int memtableIdleFlushSeconds,
                             long txnTimeoutSeconds) {
        return new Builder()
            .numFlushThreads(numFlushThreads)
            .numCompactionThreads(numCompactionThreads)
            .logLevel(LogLevel.fromValue(logLevel))
            .blockCacheSize(blockCacheSize)
            .maxOpenSSTables(maxOpenSSTables)
            .logToFile(logToFile)
            .logTruncationAt(logTruncationAt)
            .memtableWriteBufferSize(memtableWriteBufferSize)
            .memtableSkipListMaxLevel(memtableSkipListMaxLevel)
            .memtableSkipListProbability(memtableSkipListProbability)
            .memtableSyncMode(SyncMode.fromValue(memtableSyncMode))
            .memtableSyncIntervalUs(memtableSyncIntervalUs)
            .valueSeparationThreshold(valueSeparationThreshold)
            .vlogSegmentSize(vlogSegmentSize)
            .memtableL0QueueStallThreshold(memtableL0QueueStallThreshold)
            .memtableIdleFlushSeconds(memtableIdleFlushSeconds)
            .txnTimeoutSeconds(txnTimeoutSeconds)
            .build();
    }

    /**
     * Creates a new builder with the given database path. Every other field
     * starts at zero, which the engine resolves to its own default where the
     * class documentation says so.
     *
     * @param dbPath the database file-system path; must not be {@code null}
     * @return a new {@code Builder}
     */
    public static Builder builder(String dbPath) {
        return new Builder().dbPath(dbPath);
    }

    /**
     * Returns a builder pre-populated with this configuration's values.
     *
     * @return a new {@code Builder} carrying these values
     */
    public Builder toBuilder() {
        return new Builder()
            .dbPath(dbPath)
            .numFlushThreads(numFlushThreads)
            .numCompactionThreads(numCompactionThreads)
            .logLevel(logLevel)
            .blockCacheSize(blockCacheSize)
            .maxOpenSSTables(maxOpenSSTables)
            .logToFile(logToFile)
            .logTruncationAt(logTruncationAt)
            .memtableWriteBufferSize(memtableWriteBufferSize)
            .memtableSkipListMaxLevel(memtableSkipListMaxLevel)
            .memtableSkipListProbability(memtableSkipListProbability)
            .memtableSyncMode(memtableSyncMode)
            .memtableSyncIntervalUs(memtableSyncIntervalUs)
            .valueSeparationThreshold(valueSeparationThreshold)
            .vlogSegmentSize(vlogSegmentSize)
            .memtableL0QueueStallThreshold(memtableL0QueueStallThreshold)
            .memtableIdleFlushSeconds(memtableIdleFlushSeconds)
            .txnTimeoutSeconds(txnTimeoutSeconds);
    }

    /**
     * Returns the database file-system path.
     *
     * @return the database path
     */
    public String getDbPath() {
        return dbPath;
    }

    /**
     * Returns the number of flush worker threads.
     *
     * @return the flush thread count, or 0 for the engine default
     */
    public int getNumFlushThreads() {
        return numFlushThreads;
    }

    /**
     * Returns the number of compaction worker threads.
     *
     * @return the compaction thread count, or 0 for the engine default
     */
    public int getNumCompactionThreads() {
        return numCompactionThreads;
    }

    /**
     * Returns the minimum severity to emit.
     *
     * @return the log level
     */
    public LogLevel getLogLevel() {
        return logLevel;
    }

    /**
     * Returns the size in bytes of the database-level block cache for hot
     * SSTable blocks.
     *
     * @return the block cache size in bytes, or 0 for the engine default
     */
    public long getBlockCacheSize() {
        return blockCacheSize;
    }

    /**
     * Returns the maximum number of concurrently open SSTable file handles.
     * Lowered at open to what this process's open-file ceiling leaves; raise the
     * ceiling with {@link TidesDB#raiseOpenFileLimit(long)} first if the larger
     * figure is the one you want.
     *
     * @return the maximum open SSTable count, or 0 for the engine default
     */
    public long getMaxOpenSSTables() {
        return maxOpenSSTables;
    }

    /**
     * Returns whether the log is written to a file named {@code LOG} inside the
     * database directory rather than to stderr. The sink is process-wide rather
     * than per-database.
     *
     * @return {@code true} when logging to a file
     */
    public boolean isLogToFile() {
        return logToFile;
    }

    /**
     * Returns the size in bytes past which the log file is truncated and
     * reopened. Ignored unless {@link #isLogToFile()} is set.
     *
     * @return the truncation threshold in bytes, or 0 for never
     */
    public long getLogTruncationAt() {
        return logTruncationAt;
    }

    /**
     * Returns the memory the active memtable may occupy before it is rotated.
     * This is a memory budget rather than a promise about the size of what a
     * rotation flushes: an entry costs its key and value plus about a hundred
     * bytes of skip list node, pointer arrays and version struct.
     *
     * @return the write buffer size in bytes, or 0 for the engine default
     */
    public long getMemtableWriteBufferSize() {
        return memtableWriteBufferSize;
    }

    /**
     * Returns the skip list max level for the memtable.
     *
     * @return the max level, or 0 for the engine default
     */
    public int getMemtableSkipListMaxLevel() {
        return memtableSkipListMaxLevel;
    }

    /**
     * Returns the skip list level probability for the memtable.
     *
     * @return the probability, or 0 for the engine default
     */
    public float getMemtableSkipListProbability() {
        return memtableSkipListProbability;
    }

    /**
     * Returns the durability mode for the write-ahead log.
     *
     * @return the sync mode
     */
    public SyncMode getMemtableSyncMode() {
        return memtableSyncMode;
    }

    /**
     * Returns the fsync interval for {@link SyncMode#SYNC_INTERVAL}, in
     * microseconds. Ignored under the other sync modes.
     *
     * @return the interval in microseconds, or 0 for a one second default
     */
    public long getMemtableSyncIntervalUs() {
        return memtableSyncIntervalUs;
    }

    /**
     * Returns the size at or above which values are stored in the shared value
     * log and referenced from the key log, so a value stays inline only while it
     * is strictly under it.
     *
     * <p>Keep it at or under a quarter of a family's
     * {@link ColumnFamilyConfig#getBtreeKlogBlockSize()}: an inlined value
     * approaching the node size leaves a node holding one entry and spends the
     * btree fan-out that makes a lookup cheap. The pairing is advisory, and a
     * config that breaks it only logs a warning.
     *
     * @return the threshold in bytes, or 0 for the engine default
     */
    public long getValueSeparationThreshold() {
        return valueSeparationThreshold;
    }

    /**
     * Returns the size at which the value log seals its active segment and opens
     * a fresh one. A reclaim drains every segment worth draining, so this does
     * not change how much space the store settles at; it changes what reclaiming
     * costs.
     *
     * @return the segment size in bytes, or 0 for the engine default
     */
    public long getVlogSegmentSize() {
        return vlogSegmentSize;
    }

    /**
     * Returns the immutable-queue depth at which writes stall for backpressure.
     * Left at 0 the queue is unbounded and a writer outrunning the flush threads
     * is never paced, so it is a value to set deliberately rather than leave.
     *
     * @return the stall threshold, or 0 to never stall
     */
    public int getMemtableL0QueueStallThreshold() {
        return memtableL0QueueStallThreshold;
    }

    /**
     * Returns how long the active memtable may sit unwritten before the engine
     * rotates it on its own. A database that stops taking writes otherwise holds
     * that data in memory indefinitely.
     *
     * @return the idle flush interval in seconds, or 0 to never rotate on idle
     */
    public int getMemtableIdleFlushSeconds() {
        return memtableIdleFlushSeconds;
    }

    /**
     * Returns how long a transaction may stay active before the next operation
     * on it expires it. An abandoned transaction holds its snapshot and its
     * write reservations, which keeps the reclamation floor down and stops
     * compaction dropping old versions.
     *
     * @return the timeout in seconds, or 0 for no timeout
     */
    public long getTxnTimeoutSeconds() {
        return txnTimeoutSeconds;
    }

    @Override
    public String toString() {
        return "Config{" +
            "dbPath='" + dbPath + '\'' +
            ", numFlushThreads=" + numFlushThreads +
            ", numCompactionThreads=" + numCompactionThreads +
            ", logLevel=" + logLevel +
            ", blockCacheSize=" + blockCacheSize +
            ", maxOpenSSTables=" + maxOpenSSTables +
            ", logToFile=" + logToFile +
            ", logTruncationAt=" + logTruncationAt +
            ", memtableWriteBufferSize=" + memtableWriteBufferSize +
            ", memtableSkipListMaxLevel=" + memtableSkipListMaxLevel +
            ", memtableSkipListProbability=" + memtableSkipListProbability +
            ", memtableSyncMode=" + memtableSyncMode +
            ", memtableSyncIntervalUs=" + memtableSyncIntervalUs +
            ", valueSeparationThreshold=" + valueSeparationThreshold +
            ", vlogSegmentSize=" + vlogSegmentSize +
            ", memtableL0QueueStallThreshold=" + memtableL0QueueStallThreshold +
            ", memtableIdleFlushSeconds=" + memtableIdleFlushSeconds +
            ", txnTimeoutSeconds=" + txnTimeoutSeconds +
            '}';
    }

    /**
     * Builder for {@link Config}. Every field starts at zero, which the engine
     * resolves to its own default where {@link Config} says so. Call
     * {@link #build()} to create the immutable configuration; {@code build()}
     * validates all fields.
     */
    public static class Builder {

        private String dbPath = "";
        private int numFlushThreads = 0;
        private int numCompactionThreads = 0;
        private LogLevel logLevel = LogLevel.INFO;
        private long blockCacheSize = 0;
        private long maxOpenSSTables = 0;
        private boolean logToFile = false;
        private long logTruncationAt = 0;
        private long memtableWriteBufferSize = 0;
        private int memtableSkipListMaxLevel = 0;
        private float memtableSkipListProbability = 0.0f;
        private SyncMode memtableSyncMode = SyncMode.SYNC_NONE;
        private long memtableSyncIntervalUs = 0;
        private long valueSeparationThreshold = 0;
        private long vlogSegmentSize = 0;
        private int memtableL0QueueStallThreshold = 0;
        private int memtableIdleFlushSeconds = 0;
        private long txnTimeoutSeconds = 0;

        /**
         * Creates a new builder with default values.
         */
        public Builder() {
        }

        /**
         * Sets the database file-system path.
         *
         * @param dbPath the path; must not be {@code null}
         * @return this builder
         */
        public Builder dbPath(String dbPath) {
            this.dbPath = dbPath;
            return this;
        }

        /**
         * Sets the number of flush worker threads.
         *
         * @param numFlushThreads the thread count, or 0 for the engine default
         * @return this builder
         */
        public Builder numFlushThreads(int numFlushThreads) {
            this.numFlushThreads = numFlushThreads;
            return this;
        }

        /**
         * Sets the number of compaction worker threads.
         *
         * @param numCompactionThreads the thread count, or 0 for the engine default
         * @return this builder
         */
        public Builder numCompactionThreads(int numCompactionThreads) {
            this.numCompactionThreads = numCompactionThreads;
            return this;
        }

        /**
         * Sets the minimum severity to emit.
         *
         * @param logLevel the log level; must not be {@code null}
         * @return this builder
         */
        public Builder logLevel(LogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        /**
         * Sets the size of the database-level block cache for hot SSTable blocks.
         *
         * @param blockCacheSize the size in bytes, or 0 for the engine default
         * @return this builder
         */
        public Builder blockCacheSize(long blockCacheSize) {
            this.blockCacheSize = blockCacheSize;
            return this;
        }

        /**
         * Sets the maximum number of concurrently open SSTable file handles.
         *
         * @param maxOpenSSTables the maximum count, or 0 for the engine default
         * @return this builder
         */
        public Builder maxOpenSSTables(long maxOpenSSTables) {
            this.maxOpenSSTables = maxOpenSSTables;
            return this;
        }

        /**
         * Sets whether the log is written to a file named {@code LOG} inside the
         * database directory rather than to stderr.
         *
         * @param logToFile {@code true} to log to a file
         * @return this builder
         */
        public Builder logToFile(boolean logToFile) {
            this.logToFile = logToFile;
            return this;
        }

        /**
         * Sets the size past which the log file is truncated and reopened.
         *
         * @param logTruncationAt the threshold in bytes, or 0 for never
         * @return this builder
         */
        public Builder logTruncationAt(long logTruncationAt) {
            this.logTruncationAt = logTruncationAt;
            return this;
        }

        /**
         * Sets the memory the active memtable may occupy before it is rotated.
         *
         * @param memtableWriteBufferSize the size in bytes, or 0 for the engine default
         * @return this builder
         */
        public Builder memtableWriteBufferSize(long memtableWriteBufferSize) {
            this.memtableWriteBufferSize = memtableWriteBufferSize;
            return this;
        }

        /**
         * Sets the skip list max level for the memtable.
         *
         * @param memtableSkipListMaxLevel the max level, or 0 for the engine default
         * @return this builder
         */
        public Builder memtableSkipListMaxLevel(int memtableSkipListMaxLevel) {
            this.memtableSkipListMaxLevel = memtableSkipListMaxLevel;
            return this;
        }

        /**
         * Sets the skip list level probability for the memtable.
         *
         * @param memtableSkipListProbability the probability in [0.0, 1.0], or 0
         *        for the engine default
         * @return this builder
         */
        public Builder memtableSkipListProbability(float memtableSkipListProbability) {
            this.memtableSkipListProbability = memtableSkipListProbability;
            return this;
        }

        /**
         * Sets the durability mode for the write-ahead log.
         *
         * @param memtableSyncMode the sync mode; must not be {@code null}
         * @return this builder
         */
        public Builder memtableSyncMode(SyncMode memtableSyncMode) {
            this.memtableSyncMode = memtableSyncMode;
            return this;
        }

        /**
         * Sets the fsync interval for {@link SyncMode#SYNC_INTERVAL}.
         *
         * @param memtableSyncIntervalUs the interval in microseconds, or 0 for a
         *        one second default
         * @return this builder
         */
        public Builder memtableSyncIntervalUs(long memtableSyncIntervalUs) {
            this.memtableSyncIntervalUs = memtableSyncIntervalUs;
            return this;
        }

        /**
         * Sets the size at or above which values are stored in the shared value
         * log and referenced from the key log.
         *
         * @param valueSeparationThreshold the threshold in bytes, or 0 for the
         *        engine default
         * @return this builder
         */
        public Builder valueSeparationThreshold(long valueSeparationThreshold) {
            this.valueSeparationThreshold = valueSeparationThreshold;
            return this;
        }

        /**
         * Sets the size at which the value log seals its active segment.
         *
         * @param vlogSegmentSize the size in bytes, or 0 for the engine default
         * @return this builder
         */
        public Builder vlogSegmentSize(long vlogSegmentSize) {
            this.vlogSegmentSize = vlogSegmentSize;
            return this;
        }

        /**
         * Sets the immutable-queue depth at which writes stall for backpressure.
         *
         * @param memtableL0QueueStallThreshold the depth, or 0 to never stall
         * @return this builder
         */
        public Builder memtableL0QueueStallThreshold(int memtableL0QueueStallThreshold) {
            this.memtableL0QueueStallThreshold = memtableL0QueueStallThreshold;
            return this;
        }

        /**
         * Sets how long the active memtable may sit unwritten before the engine
         * rotates it on its own.
         *
         * @param memtableIdleFlushSeconds the interval in seconds, or 0 to never
         *        rotate on idle
         * @return this builder
         */
        public Builder memtableIdleFlushSeconds(int memtableIdleFlushSeconds) {
            this.memtableIdleFlushSeconds = memtableIdleFlushSeconds;
            return this;
        }

        /**
         * Sets how long a transaction may stay active before the next operation
         * on it expires it. A single transaction can override this with
         * {@link Transaction#setTimeout(long)}.
         *
         * @param txnTimeoutSeconds the timeout in seconds, or 0 for no timeout
         * @return this builder
         */
        public Builder txnTimeoutSeconds(long txnTimeoutSeconds) {
            this.txnTimeoutSeconds = txnTimeoutSeconds;
            return this;
        }

        /**
         * Validates all fields and creates the immutable {@link Config}.
         *
         * @return a new {@code Config}
         * @throws IllegalArgumentException if any field is invalid
         */
        public Config build() {
            validate();
            return new Config(this);
        }

        private void validate() {
            if (dbPath == null) {
                throw new IllegalArgumentException("Database path cannot be null");
            }
            if (numFlushThreads < 0) {
                throw new IllegalArgumentException(
                    "numFlushThreads must not be negative, was: " + numFlushThreads);
            }
            if (numCompactionThreads < 0) {
                throw new IllegalArgumentException(
                    "numCompactionThreads must not be negative, was: " + numCompactionThreads);
            }
            if (logLevel == null) {
                throw new IllegalArgumentException("Log level cannot be null");
            }
            if (blockCacheSize < 0) {
                throw new IllegalArgumentException(
                    "blockCacheSize must not be negative, was: " + blockCacheSize);
            }
            if (maxOpenSSTables < 0) {
                throw new IllegalArgumentException(
                    "maxOpenSSTables must not be negative, was: " + maxOpenSSTables);
            }
            if (logTruncationAt < 0) {
                throw new IllegalArgumentException(
                    "logTruncationAt must not be negative, was: " + logTruncationAt);
            }
            if (memtableWriteBufferSize < 0) {
                throw new IllegalArgumentException(
                    "memtableWriteBufferSize must not be negative, was: " + memtableWriteBufferSize);
            }
            if (memtableSkipListMaxLevel < 0) {
                throw new IllegalArgumentException(
                    "memtableSkipListMaxLevel must not be negative, was: " + memtableSkipListMaxLevel);
            }
            if (Float.isNaN(memtableSkipListProbability)
                    || Float.isInfinite(memtableSkipListProbability)
                    || memtableSkipListProbability < 0.0f
                    || memtableSkipListProbability > 1.0f) {
                throw new IllegalArgumentException(
                    "memtableSkipListProbability must be finite and in [0.0, 1.0], was: "
                        + memtableSkipListProbability);
            }
            if (memtableSyncMode == null) {
                throw new IllegalArgumentException("memtableSyncMode cannot be null");
            }
            if (memtableSyncIntervalUs < 0) {
                throw new IllegalArgumentException(
                    "memtableSyncIntervalUs must not be negative, was: " + memtableSyncIntervalUs);
            }
            if (valueSeparationThreshold < 0) {
                throw new IllegalArgumentException(
                    "valueSeparationThreshold must not be negative, was: " + valueSeparationThreshold);
            }
            if (vlogSegmentSize < 0) {
                throw new IllegalArgumentException(
                    "vlogSegmentSize must not be negative, was: " + vlogSegmentSize);
            }
            if (memtableL0QueueStallThreshold < 0) {
                throw new IllegalArgumentException(
                    "memtableL0QueueStallThreshold must not be negative, was: "
                        + memtableL0QueueStallThreshold);
            }
            if (memtableIdleFlushSeconds < 0) {
                throw new IllegalArgumentException(
                    "memtableIdleFlushSeconds must not be negative, was: " + memtableIdleFlushSeconds);
            }
            if (txnTimeoutSeconds < 0) {
                throw new IllegalArgumentException(
                    "txnTimeoutSeconds must not be negative, was: " + txnTimeoutSeconds);
            }
        }
    }
}
