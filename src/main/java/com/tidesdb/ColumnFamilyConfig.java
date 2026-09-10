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

import java.util.Arrays;

/**
 * Per-column-family configuration. Every field is mutable at runtime via
 * {@link ColumnFamily#updateRuntimeConfig(ColumnFamilyConfig, boolean)}, since
 * keys are ordered byte-wise and SSTables are therefore always mergeable.
 *
 * <p>The memtable, write-ahead log, and their sync and skip-list settings are
 * database-level and live on {@link Config}, not here.
 *
 * <p>Use {@link #builder()} to construct a configuration with custom values, or
 * {@link #defaultConfig()} to obtain one carrying the native library's own
 * defaults. Instances are immutable once built.
 */
public class ColumnFamilyConfig {

    static {
        NativeLibrary.load();
    }

    /**
     * The longest a column family name may be, including its terminator.
     */
    public static final int MAX_NAME_LENGTH = 128;

    /**
     * The most stacked encodings a column family may apply.
     */
    public static final int MAX_ENCODING_PIPELINE = 8;

    private final String name;
    private final long levelSizeRatio;
    private final int minLevels;
    private final int dividingLevelOffset;
    private final boolean keepValuesInline;
    private final long btreeKlogBlockSize;
    private final int[] encodingPipeline;
    private final boolean enableBloomFilter;
    private final double bloomFpr;
    private final IsolationLevel defaultIsolationLevel;
    private final int l1FileCountTrigger;
    private final double tombstoneDensityTrigger;
    private final long tombstoneDensityMinEntries;

    private ColumnFamilyConfig(Builder builder) {
        this.name = builder.name;
        this.levelSizeRatio = builder.levelSizeRatio;
        this.minLevels = builder.minLevels;
        this.dividingLevelOffset = builder.dividingLevelOffset;
        this.keepValuesInline = builder.keepValuesInline;
        this.btreeKlogBlockSize = builder.btreeKlogBlockSize;
        this.encodingPipeline = builder.encodingPipeline.clone();
        this.enableBloomFilter = builder.enableBloomFilter;
        this.bloomFpr = builder.bloomFpr;
        this.defaultIsolationLevel = builder.defaultIsolationLevel;
        this.l1FileCountTrigger = builder.l1FileCountTrigger;
        this.tombstoneDensityTrigger = builder.tombstoneDensityTrigger;
        this.tombstoneDensityMinEntries = builder.tombstoneDensityMinEntries;
    }

    /**
     * Returns a configuration carrying the native library's own defaults, as
     * returned by {@code tidesdb_default_column_family_config()}. Use
     * {@link #toBuilder()} to adjust individual fields.
     *
     * @return a new {@code ColumnFamilyConfig} holding the native defaults
     */
    public static ColumnFamilyConfig defaultConfig() {
        return nativeDefaultConfig();
    }

    /**
     * Reads {@code tidesdb_default_column_family_config()}.
     */
    private static native ColumnFamilyConfig nativeDefaultConfig();

    /**
     * Assembles a configuration from the flat field list the JNI bridge reads
     * out of {@code tidesdb_column_family_config_t}. Called from native code
     * only.
     */
    static ColumnFamilyConfig fromNative(String name, long levelSizeRatio, int minLevels,
                                         int dividingLevelOffset, boolean keepValuesInline,
                                         long btreeKlogBlockSize, int[] encodingPipeline,
                                         boolean enableBloomFilter, double bloomFpr,
                                         int defaultIsolationLevel, int l1FileCountTrigger,
                                         double tombstoneDensityTrigger,
                                         long tombstoneDensityMinEntries) {
        return new Builder()
            .name(name)
            .levelSizeRatio(levelSizeRatio)
            .minLevels(minLevels)
            .dividingLevelOffset(dividingLevelOffset)
            .keepValuesInline(keepValuesInline)
            .btreeKlogBlockSize(btreeKlogBlockSize)
            .encodingPipelineIds(encodingPipeline)
            .enableBloomFilter(enableBloomFilter)
            .bloomFpr(bloomFpr)
            .defaultIsolationLevel(IsolationLevel.fromValue(defaultIsolationLevel))
            .l1FileCountTrigger(l1FileCountTrigger)
            .tombstoneDensityTrigger(tombstoneDensityTrigger)
            .tombstoneDensityMinEntries(tombstoneDensityMinEntries)
            .build();
    }

    /**
     * Creates a new builder with the native library's defaults as its starting
     * point.
     *
     * @return a new {@code Builder}
     */
    public static Builder builder() {
        return defaultConfig().toBuilder();
    }

    /**
     * Returns a builder pre-populated with this configuration's values.
     *
     * @return a new {@code Builder} carrying these values
     */
    public Builder toBuilder() {
        return new Builder()
            .name(name)
            .levelSizeRatio(levelSizeRatio)
            .minLevels(minLevels)
            .dividingLevelOffset(dividingLevelOffset)
            .keepValuesInline(keepValuesInline)
            .btreeKlogBlockSize(btreeKlogBlockSize)
            .encodingPipelineIds(encodingPipeline)
            .enableBloomFilter(enableBloomFilter)
            .bloomFpr(bloomFpr)
            .defaultIsolationLevel(defaultIsolationLevel)
            .l1FileCountTrigger(l1FileCountTrigger)
            .tombstoneDensityTrigger(tombstoneDensityTrigger)
            .tombstoneDensityMinEntries(tombstoneDensityMinEntries);
    }

    /**
     * Returns the column family's persisted identity. Empty on a configuration
     * you built yourself: the name passed to
     * {@link TidesDB#createColumnFamily(String, ColumnFamilyConfig)} is
     * authoritative and this field is ignored there. It carries the family's
     * name on a configuration read back from {@link CfStats#getConfig()}.
     *
     * @return the column family name, never {@code null}
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the target size ratio between successive levels.
     *
     * @return the level size ratio
     */
    public long getLevelSizeRatio() {
        return levelSizeRatio;
    }

    /**
     * Returns the floor on the level count. The tree deepens as it fills and
     * sheds levels again as data is deleted, and this is the depth it will not
     * shed below. The engine keeps its own floor of a flush tier plus one level
     * for merges to land in, so a smaller value has no further effect.
     *
     * @return the minimum level count
     */
    public int getMinLevels() {
        return minLevels;
    }

    /**
     * Returns how far above the largest level the dividing level sits, so 1
     * means X = L - 2. The dividing level is where a merge writes output
     * partitioned to the largest level's file boundaries, which is what lets
     * later merges take one group of overlapping files at a time.
     *
     * @return the dividing level offset
     */
    public int getDividingLevelOffset() {
        return dividingLevelOffset;
    }

    /**
     * Returns whether every value is held in the key log whatever its size,
     * ignoring the database's {@link Config#getValueSeparationThreshold()}.
     *
     * <p>A separated value costs a scan one value-log read per row, so a family
     * that is scanned far more than it is merged can be worth keeping whole even
     * though its values are large. The cost is the one the threshold exists to
     * avoid, that compaction rewrites those bytes on every merge.
     *
     * @return {@code true} when values stay inline regardless of size
     */
    public boolean isKeepValuesInline() {
        return keepValuesInline;
    }

    /**
     * Returns the target size in bytes of a btree key-log node. Raise it
     * alongside the database's {@link Config#getValueSeparationThreshold()}
     * rather than on its own.
     *
     * @return the block size in bytes, or 0 to leave the choice to the btree
     */
    public long getBtreeKlogBlockSize() {
        return btreeKlogBlockSize;
    }

    /**
     * Returns the encoding ids applied in order to btree key-log nodes and
     * undone in reverse on read. Ids in the range of {@link CompressionAlgorithm}
     * name a built-in compression codec.
     *
     * @return a copy of the pipeline, empty when data is stored verbatim
     */
    public int[] getEncodingPipeline() {
        return encodingPipeline.clone();
    }

    /**
     * Returns whether a partition-range filter is built for point-get pruning.
     *
     * @return {@code true} when the bloom filter is enabled
     */
    public boolean isEnableBloomFilter() {
        return enableBloomFilter;
    }

    /**
     * Returns the target bloom false-positive rate when the filter is enabled.
     *
     * @return the false-positive rate
     */
    public double getBloomFpr() {
        return bloomFpr;
    }

    /**
     * Returns the isolation applied to a transaction opened against this family
     * without an explicit level.
     *
     * @return the default isolation level
     */
    public IsolationLevel getDefaultIsolationLevel() {
        return defaultIsolationLevel;
    }

    /**
     * Returns the L1 SSTable count that triggers compaction.
     *
     * @return the L1 file count trigger
     */
    public int getL1FileCountTrigger() {
        return l1FileCountTrigger;
    }

    /**
     * Returns the ratio in [0, 1] above which an SSTable's tombstone density
     * escalates compaction.
     *
     * @return the density trigger, or 0 to disable it
     */
    public double getTombstoneDensityTrigger() {
        return tombstoneDensityTrigger;
    }

    /**
     * Returns the minimum entry count for an SSTable to be judged by the density
     * trigger, filtering tiny-SSTable noise.
     *
     * @return the minimum entry count, or 0 to impose no minimum
     */
    public long getTombstoneDensityMinEntries() {
        return tombstoneDensityMinEntries;
    }

    @Override
    public String toString() {
        return "ColumnFamilyConfig{" +
            "name='" + name + '\'' +
            ", levelSizeRatio=" + levelSizeRatio +
            ", minLevels=" + minLevels +
            ", dividingLevelOffset=" + dividingLevelOffset +
            ", keepValuesInline=" + keepValuesInline +
            ", btreeKlogBlockSize=" + btreeKlogBlockSize +
            ", encodingPipeline=" + Arrays.toString(encodingPipeline) +
            ", enableBloomFilter=" + enableBloomFilter +
            ", bloomFpr=" + bloomFpr +
            ", defaultIsolationLevel=" + defaultIsolationLevel +
            ", l1FileCountTrigger=" + l1FileCountTrigger +
            ", tombstoneDensityTrigger=" + tombstoneDensityTrigger +
            ", tombstoneDensityMinEntries=" + tombstoneDensityMinEntries +
            '}';
    }

    /**
     * Builder for {@link ColumnFamilyConfig}. Call {@link #build()} to create
     * the immutable configuration; {@code build()} validates all fields.
     */
    public static class Builder {

        private String name = "";
        private long levelSizeRatio = 0;
        private int minLevels = 0;
        private int dividingLevelOffset = 0;
        private boolean keepValuesInline = false;
        private long btreeKlogBlockSize = 0;
        private int[] encodingPipeline = new int[0];
        private boolean enableBloomFilter = false;
        private double bloomFpr = 0.0;
        private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_COMMITTED;
        private int l1FileCountTrigger = 0;
        private double tombstoneDensityTrigger = 0.0;
        private long tombstoneDensityMinEntries = 0;

        /**
         * Creates a new builder with zeroed values. Prefer
         * {@link ColumnFamilyConfig#builder()}, which starts from the native
         * library's defaults.
         */
        public Builder() {
        }

        /**
         * Sets the column family name. Ignored by
         * {@link TidesDB#createColumnFamily(String, ColumnFamilyConfig)} and by
         * {@link ColumnFamily#updateRuntimeConfig(ColumnFamilyConfig, boolean)},
         * both of which take the name from elsewhere.
         *
         * @param name the name; {@code null} is treated as empty
         * @return this builder
         */
        public Builder name(String name) {
            this.name = name == null ? "" : name;
            return this;
        }

        /**
         * Sets the target size ratio between successive levels.
         *
         * @param levelSizeRatio the ratio
         * @return this builder
         */
        public Builder levelSizeRatio(long levelSizeRatio) {
            this.levelSizeRatio = levelSizeRatio;
            return this;
        }

        /**
         * Sets the floor on the level count.
         *
         * @param minLevels the minimum level count
         * @return this builder
         */
        public Builder minLevels(int minLevels) {
            this.minLevels = minLevels;
            return this;
        }

        /**
         * Sets how far above the largest level the dividing level sits.
         *
         * @param dividingLevelOffset the offset
         * @return this builder
         */
        public Builder dividingLevelOffset(int dividingLevelOffset) {
            this.dividingLevelOffset = dividingLevelOffset;
            return this;
        }

        /**
         * Sets whether every value is held in the key log whatever its size.
         *
         * @param keepValuesInline {@code true} to keep values inline
         * @return this builder
         */
        public Builder keepValuesInline(boolean keepValuesInline) {
            this.keepValuesInline = keepValuesInline;
            return this;
        }

        /**
         * Sets the target size of a btree key-log node.
         *
         * @param btreeKlogBlockSize the size in bytes, or 0 to leave the choice
         *        to the btree
         * @return this builder
         */
        public Builder btreeKlogBlockSize(long btreeKlogBlockSize) {
            this.btreeKlogBlockSize = btreeKlogBlockSize;
            return this;
        }

        /**
         * Sets the encoding pipeline from built-in compression algorithms,
         * applied in the order given.
         *
         * @param algorithms the algorithms; an empty list stores data verbatim
         * @return this builder
         */
        public Builder encodingPipeline(CompressionAlgorithm... algorithms) {
            if (algorithms == null) {
                this.encodingPipeline = new int[0];
                return this;
            }
            int[] ids = new int[algorithms.length];
            for (int i = 0; i < algorithms.length; i++) {
                if (algorithms[i] == null) {
                    throw new IllegalArgumentException("Encoding pipeline entry cannot be null");
                }
                ids[i] = algorithms[i].getValue();
            }
            this.encodingPipeline = ids;
            return this;
        }

        /**
         * Sets the encoding pipeline from raw encoding ids, applied in the order
         * given. Use this for an encoding the {@link CompressionAlgorithm} enum
         * does not name.
         *
         * @param ids the encoding ids, each in [0, 255]; an empty array stores
         *        data verbatim
         * @return this builder
         */
        public Builder encodingPipelineIds(int... ids) {
            this.encodingPipeline = ids == null ? new int[0] : ids.clone();
            return this;
        }

        /**
         * Sets a single-codec encoding pipeline, the common case. Passing
         * {@link CompressionAlgorithm#NONE} clears the pipeline so
         * data is stored verbatim.
         *
         * @param algorithm the algorithm; must not be {@code null}
         * @return this builder
         */
        public Builder compression(CompressionAlgorithm algorithm) {
            if (algorithm == null) {
                throw new IllegalArgumentException("Compression algorithm cannot be null");
            }
            if (algorithm == CompressionAlgorithm.NONE) {
                return encodingPipelineIds();
            }
            return encodingPipeline(algorithm);
        }

        /**
         * Sets whether a partition-range filter is built for point-get pruning.
         *
         * @param enableBloomFilter {@code true} to enable the filter
         * @return this builder
         */
        public Builder enableBloomFilter(boolean enableBloomFilter) {
            this.enableBloomFilter = enableBloomFilter;
            return this;
        }

        /**
         * Sets the target bloom false-positive rate.
         *
         * @param bloomFpr the rate, in (0.0, 1.0) when the filter is enabled
         * @return this builder
         */
        public Builder bloomFpr(double bloomFpr) {
            this.bloomFpr = bloomFpr;
            return this;
        }

        /**
         * Sets the isolation applied to a transaction opened against this family
         * without an explicit level.
         *
         * @param defaultIsolationLevel the level; must not be {@code null}
         * @return this builder
         */
        public Builder defaultIsolationLevel(IsolationLevel defaultIsolationLevel) {
            this.defaultIsolationLevel = defaultIsolationLevel;
            return this;
        }

        /**
         * Sets the L1 SSTable count that triggers compaction.
         *
         * @param l1FileCountTrigger the trigger count
         * @return this builder
         */
        public Builder l1FileCountTrigger(int l1FileCountTrigger) {
            this.l1FileCountTrigger = l1FileCountTrigger;
            return this;
        }

        /**
         * Sets the tombstone density above which compaction is escalated.
         *
         * @param tombstoneDensityTrigger the ratio in [0.0, 1.0], or 0 to disable
         * @return this builder
         */
        public Builder tombstoneDensityTrigger(double tombstoneDensityTrigger) {
            this.tombstoneDensityTrigger = tombstoneDensityTrigger;
            return this;
        }

        /**
         * Sets the minimum entry count for an SSTable to be judged by the
         * density trigger.
         *
         * @param tombstoneDensityMinEntries the minimum, or 0 for no minimum
         * @return this builder
         */
        public Builder tombstoneDensityMinEntries(long tombstoneDensityMinEntries) {
            this.tombstoneDensityMinEntries = tombstoneDensityMinEntries;
            return this;
        }

        /**
         * Validates all fields and creates the immutable
         * {@link ColumnFamilyConfig}.
         *
         * @return a new {@code ColumnFamilyConfig}
         * @throws IllegalArgumentException if any field is invalid
         */
        public ColumnFamilyConfig build() {
            validate();
            return new ColumnFamilyConfig(this);
        }

        private void validate() {
            if (name.length() >= MAX_NAME_LENGTH) {
                throw new IllegalArgumentException(
                    "name must be shorter than " + MAX_NAME_LENGTH + " characters, was: "
                        + name.length());
            }
            if (levelSizeRatio < 0) {
                throw new IllegalArgumentException(
                    "levelSizeRatio must not be negative, was: " + levelSizeRatio);
            }
            if (minLevels < 0) {
                throw new IllegalArgumentException(
                    "minLevels must not be negative, was: " + minLevels);
            }
            if (dividingLevelOffset < 0) {
                throw new IllegalArgumentException(
                    "dividingLevelOffset must not be negative, was: " + dividingLevelOffset);
            }
            if (btreeKlogBlockSize < 0) {
                throw new IllegalArgumentException(
                    "btreeKlogBlockSize must not be negative, was: " + btreeKlogBlockSize);
            }
            if (encodingPipeline.length > MAX_ENCODING_PIPELINE) {
                throw new IllegalArgumentException(
                    "encodingPipeline must hold at most " + MAX_ENCODING_PIPELINE
                        + " entries, was: " + encodingPipeline.length);
            }
            for (int id : encodingPipeline) {
                if (id < 0 || id > 255) {
                    throw new IllegalArgumentException(
                        "encoding id must be in [0, 255], was: " + id);
                }
            }
            if (Double.isNaN(bloomFpr) || Double.isInfinite(bloomFpr)
                    || bloomFpr < 0.0 || bloomFpr >= 1.0) {
                throw new IllegalArgumentException(
                    "bloomFpr must be finite and in [0.0, 1.0), was: " + bloomFpr);
            }
            if (defaultIsolationLevel == null) {
                throw new IllegalArgumentException("defaultIsolationLevel cannot be null");
            }
            if (l1FileCountTrigger < 0) {
                throw new IllegalArgumentException(
                    "l1FileCountTrigger must not be negative, was: " + l1FileCountTrigger);
            }
            if (Double.isNaN(tombstoneDensityTrigger) || Double.isInfinite(tombstoneDensityTrigger)
                    || tombstoneDensityTrigger < 0.0 || tombstoneDensityTrigger > 1.0) {
                throw new IllegalArgumentException(
                    "tombstoneDensityTrigger must be finite and in [0.0, 1.0], was: "
                        + tombstoneDensityTrigger);
            }
            if (tombstoneDensityMinEntries < 0) {
                throw new IllegalArgumentException(
                    "tombstoneDensityMinEntries must not be negative, was: "
                        + tombstoneDensityMinEntries);
            }
        }
    }
}
