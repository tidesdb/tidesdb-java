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
 * Database-level statistics returned by {@link TidesDB#getDbStats()}. The
 * memtable, the value log, and the MVCC clock are database-level and shared, so
 * their figures live here rather than on {@link CfStats}.
 */
public class DbStats {

    private final int numColumnFamilies;
    private final int immutableMemtableCount;
    private final int compactionPendingCount;
    private final int totalSstableCount;
    private final long totalDataSizeBytes;
    private final int numOpenSstables;
    private final long globalSeq;
    private final long minSnapshotSeq;
    private final int activeTxnCount;
    private final long txnMemoryBytes;
    private final long memtableBytes;
    private final boolean flushing;
    private final long nextCfIndex;
    private final long walGeneration;
    private final long flushCount;
    private final long compactionCount;
    private final long flushBytesWritten;
    private final long compactionBytesWritten;
    private final long compactionBytesRead;
    private final long walBytesWritten;
    private final long userBytesWritten;
    private final long vlogFileSize;
    private final long vlogValueCount;
    private final long vlogUsedBytes;
    private final long vlogStoredBytes;
    private final long vlogLiveBytes;
    private final long vlogSegmentCount;
    private final long vlogBytesWritten;
    private final long vlogDeadBytes;
    private final long vlogReclaimCalls;
    private final long vlogReclaimPasses;
    private final long vlogSegmentsRetired;
    private final long vlogSegmentsDrainable;
    private final long writesThrottled;
    private final long writesBlocked;
    private final long writeStallUs;
    private final long writeStallCeilingHits;

    /**
     * Creates a new {@code DbStats}. Typically called by the JNI bridge rather
     * than application code.
     *
     * @param numColumnFamilies number of column families
     * @param immutableMemtableCount immutable memtables awaiting flush
     * @param compactionPendingCount compaction jobs queued for the worker pool
     * @param totalSstableCount total SSTables across every family and level
     * @param totalDataSizeBytes on-disk key-log bytes summed across every family
     * @param numOpenSstables currently open SSTable file handles
     * @param globalSeq current database-global sequence number
     * @param minSnapshotSeq the oldest live-transaction snapshot
     * @param activeTxnCount live transactions joined to the registry
     * @param txnMemoryBytes bytes held by in-flight transactions
     * @param memtableBytes memory the active memtable occupies
     * @param flushing whether an immutable is queued or flushing
     * @param nextCfIndex next column family id to be assigned
     * @param walGeneration current write-ahead-log generation counter
     * @param flushCount SSTables flushed across every family
     * @param compactionCount compactions run across every family
     * @param flushBytesWritten flush output bytes summed across every family
     * @param compactionBytesWritten compaction output bytes summed across every family
     * @param compactionBytesRead compaction input bytes summed across every family
     * @param walBytesWritten framed write-ahead-log bytes, counted at the append
     * @param userBytesWritten logical committed bytes summed across every family
     * @param vlogFileSize total value-log file size in bytes
     * @param vlogValueCount values currently indexed in the value log
     * @param vlogUsedBytes uncompressed length the indexed values represent
     * @param vlogStoredBytes the on-disk length those same values occupy
     * @param vlogLiveBytes value-log bytes the live SSTables still reference
     * @param vlogSegmentCount value-log segment files currently open
     * @param vlogBytesWritten value-log bytes ever appended
     * @param vlogDeadBytes value-log bytes beyond what the live values account for
     * @param vlogReclaimCalls value-log reclaims attempted, lifetime
     * @param vlogReclaimPasses value-log reclaim passes that drained a segment
     * @param vlogSegmentsRetired value-log segment files a reclaim has unlinked
     * @param vlogSegmentsDrainable sealed segments worth rewriting tables for
     * @param writesThrottled commits the L0 admission policy made dwell
     * @param writesBlocked commits the L0 admission policy made wait
     * @param writeStallUs total microseconds commits were held in L0 admission
     * @param writeStallCeilingHits commits admitted only because the wait ceiling expired
     */
    public DbStats(int numColumnFamilies, int immutableMemtableCount, int compactionPendingCount,
                   int totalSstableCount, long totalDataSizeBytes, int numOpenSstables,
                   long globalSeq, long minSnapshotSeq, int activeTxnCount, long txnMemoryBytes,
                   long memtableBytes, boolean flushing, long nextCfIndex, long walGeneration,
                   long flushCount, long compactionCount, long flushBytesWritten,
                   long compactionBytesWritten, long compactionBytesRead, long walBytesWritten,
                   long userBytesWritten, long vlogFileSize, long vlogValueCount,
                   long vlogUsedBytes, long vlogStoredBytes, long vlogLiveBytes,
                   long vlogSegmentCount, long vlogBytesWritten, long vlogDeadBytes,
                   long vlogReclaimCalls, long vlogReclaimPasses, long vlogSegmentsRetired,
                   long vlogSegmentsDrainable, long writesThrottled, long writesBlocked,
                   long writeStallUs, long writeStallCeilingHits) {
        this.numColumnFamilies = numColumnFamilies;
        this.immutableMemtableCount = immutableMemtableCount;
        this.compactionPendingCount = compactionPendingCount;
        this.totalSstableCount = totalSstableCount;
        this.totalDataSizeBytes = totalDataSizeBytes;
        this.numOpenSstables = numOpenSstables;
        this.globalSeq = globalSeq;
        this.minSnapshotSeq = minSnapshotSeq;
        this.activeTxnCount = activeTxnCount;
        this.txnMemoryBytes = txnMemoryBytes;
        this.memtableBytes = memtableBytes;
        this.flushing = flushing;
        this.nextCfIndex = nextCfIndex;
        this.walGeneration = walGeneration;
        this.flushCount = flushCount;
        this.compactionCount = compactionCount;
        this.flushBytesWritten = flushBytesWritten;
        this.compactionBytesWritten = compactionBytesWritten;
        this.compactionBytesRead = compactionBytesRead;
        this.walBytesWritten = walBytesWritten;
        this.userBytesWritten = userBytesWritten;
        this.vlogFileSize = vlogFileSize;
        this.vlogValueCount = vlogValueCount;
        this.vlogUsedBytes = vlogUsedBytes;
        this.vlogStoredBytes = vlogStoredBytes;
        this.vlogLiveBytes = vlogLiveBytes;
        this.vlogSegmentCount = vlogSegmentCount;
        this.vlogBytesWritten = vlogBytesWritten;
        this.vlogDeadBytes = vlogDeadBytes;
        this.vlogReclaimCalls = vlogReclaimCalls;
        this.vlogReclaimPasses = vlogReclaimPasses;
        this.vlogSegmentsRetired = vlogSegmentsRetired;
        this.vlogSegmentsDrainable = vlogSegmentsDrainable;
        this.writesThrottled = writesThrottled;
        this.writesBlocked = writesBlocked;
        this.writeStallUs = writeStallUs;
        this.writeStallCeilingHits = writeStallCeilingHits;
    }

    /**
     * Returns the number of column families.
     *
     * @return the column family count
     */
    public int getNumColumnFamilies() {
        return numColumnFamilies;
    }

    /**
     * Returns the number of immutable memtables awaiting flush, which is the L0
     * queue depth.
     *
     * @return the immutable memtable count
     */
    public int getImmutableMemtableCount() {
        return immutableMemtableCount;
    }

    /**
     * Returns the number of compaction jobs queued for the worker pool.
     *
     * @return the pending compaction count
     */
    public int getCompactionPendingCount() {
        return compactionPendingCount;
    }

    /**
     * Returns the total SSTables across every column family and level.
     *
     * @return the SSTable count
     */
    public int getTotalSstableCount() {
        return totalSstableCount;
    }

    /**
     * Returns the on-disk key-log bytes summed across every column family and
     * level. The value log is reported separately by {@link #getVlogFileSize()},
     * since it is shared rather than owned by any one family.
     *
     * @return the on-disk data size in bytes
     */
    public long getTotalDataSizeBytes() {
        return totalDataSizeBytes;
    }

    /**
     * Returns the number of currently open SSTable file handles.
     *
     * @return the open SSTable count
     */
    public int getNumOpenSstables() {
        return numOpenSstables;
    }

    /**
     * Returns the current database-global sequence number, the MVCC clock.
     *
     * @return the global sequence
     */
    public long getGlobalSeq() {
        return globalSeq;
    }

    /**
     * Returns the oldest live-transaction snapshot, which is the compaction
     * garbage-collection floor.
     *
     * <p>The engine's sequence is an unsigned 64-bit value, so when no snapshot
     * is registered the floor sits at {@code UINT64_MAX} and arrives here as
     * {@code -1}. Compare sequences with
     * {@link Long#compareUnsigned(long, long)} rather than {@code <}.
     *
     * @return the minimum snapshot sequence, or {@code -1} when nothing holds
     *         the floor
     */
    public long getMinSnapshotSeq() {
        return minSnapshotSeq;
    }

    /**
     * Returns the live transactions joined to the registry, which is
     * repeatable-read and stronger only. Read-uncommitted and read-committed
     * transactions need no snapshot reservation and so are not counted.
     *
     * @return the active transaction count
     */
    public int getActiveTxnCount() {
        return activeTxnCount;
    }

    /**
     * Returns the bytes held by in-flight transactions, over the same registered
     * set as {@link #getActiveTxnCount()}.
     *
     * @return the transaction memory in bytes
     */
    public long getTxnMemoryBytes() {
        return txnMemoryBytes;
    }

    /**
     * Returns the memory the active memtable occupies, the figure
     * {@link Config#getMemtableWriteBufferSize()} is compared against, so it
     * counts skip list nodes and version structs as well as key and value bytes.
     *
     * @return the memtable size in bytes
     */
    public long getMemtableBytes() {
        return memtableBytes;
    }

    /**
     * Returns whether an immutable memtable is queued or flushing.
     *
     * @return {@code true} while flushing
     */
    public boolean isFlushing() {
        return flushing;
    }

    /**
     * Returns the next column family id to be assigned.
     *
     * @return the next column family index
     */
    public long getNextCfIndex() {
        return nextCfIndex;
    }

    /**
     * Returns the current write-ahead-log generation counter.
     *
     * @return the WAL generation
     */
    public long getWalGeneration() {
        return walGeneration;
    }

    /**
     * Returns the SSTables flushed across every column family.
     *
     * @return the flush count
     */
    public long getFlushCount() {
        return flushCount;
    }

    /**
     * Returns the compactions run across every column family.
     *
     * @return the compaction count
     */
    public long getCompactionCount() {
        return compactionCount;
    }

    /**
     * Returns the flush output bytes summed across every column family.
     *
     * @return the flush byte count
     */
    public long getFlushBytesWritten() {
        return flushBytesWritten;
    }

    /**
     * Returns the compaction output bytes summed across every column family.
     *
     * @return the compaction output byte count
     */
    public long getCompactionBytesWritten() {
        return compactionBytesWritten;
    }

    /**
     * Returns the compaction input bytes summed across every column family.
     *
     * @return the compaction input byte count
     */
    public long getCompactionBytesRead() {
        return compactionBytesRead;
    }

    /**
     * Returns the framed write-ahead-log bytes, counted at the append. The log
     * is reclaimed with its memtable and never appears in the on-disk totals,
     * but the device was still asked to write it, and a write-amplification
     * figure that omits it understates by a whole copy of the data.
     *
     * @return the WAL byte count
     */
    public long getWalBytesWritten() {
        return walBytesWritten;
    }

    /**
     * Returns the logical committed bytes summed across every column family.
     *
     * @return the user byte count
     */
    public long getUserBytesWritten() {
        return userBytesWritten;
    }

    /**
     * Returns the total value-log file size in bytes.
     *
     * @return the value-log size
     */
    public long getVlogFileSize() {
        return vlogFileSize;
    }

    /**
     * Returns the values currently indexed in the value log.
     *
     * @return the indexed value count
     */
    public long getVlogValueCount() {
        return vlogValueCount;
    }

    /**
     * Returns the uncompressed length the indexed values represent. This counts
     * everything the index still names, reachable or not, so it is not a measure
     * of live data.
     *
     * @return the used byte count
     */
    public long getVlogUsedBytes() {
        return vlogUsedBytes;
    }

    /**
     * Returns the on-disk length those same indexed values occupy. Read against
     * {@link #getVlogUsedBytes()} it is the encoding pipeline's realised ratio.
     *
     * @return the stored byte count
     */
    public long getVlogStoredBytes() {
        return vlogStoredBytes;
    }

    /**
     * Returns the value-log bytes the live SSTables still reference. This is the
     * figure space amplification is against: a store can hold many gigabytes of
     * values no tree can reach, and only this tells them apart from the ones
     * still worth keeping.
     *
     * @return the live byte count
     */
    public long getVlogLiveBytes() {
        return vlogLiveBytes;
    }

    /**
     * Returns the value-log segment files currently open, the one taking appends
     * included.
     *
     * @return the segment count
     */
    public long getVlogSegmentCount() {
        return vlogSegmentCount;
    }

    /**
     * Returns the value-log bytes ever appended, reclamation's own rewrites
     * included. On a store that separates its values most of the writing happens
     * here.
     *
     * @return the appended byte count
     */
    public long getVlogBytesWritten() {
        return vlogBytesWritten;
    }

    /**
     * Returns the value-log bytes beyond what the live values account for, the
     * space a reclaim could recover.
     *
     * @return the dead byte count
     */
    public long getVlogDeadBytes() {
        return vlogDeadBytes;
    }

    /**
     * Returns the value-log reclaims attempted, lifetime. Read beside
     * {@link #getVlogReclaimPasses()} this separates a reclaim that never runs
     * from one that runs and finds nothing worth draining.
     *
     * @return the reclaim call count
     */
    public long getVlogReclaimCalls() {
        return vlogReclaimCalls;
    }

    /**
     * Returns the value-log reclaim passes that drained a segment, since this
     * handle opened.
     *
     * @return the reclaim pass count
     */
    public long getVlogReclaimPasses() {
        return vlogReclaimPasses;
    }

    /**
     * Returns the value-log segment files a reclaim has unlinked.
     *
     * @return the retired segment count
     */
    public long getVlogSegmentsRetired() {
        return vlogSegmentsRetired;
    }

    /**
     * Returns the sealed segments holding so little live data that rewriting the
     * tables referencing them would free most of a file. Read against
     * {@link #getVlogDeadBytes()} this says how much of the garbage is currently
     * actionable, and a figure that stays high is reclamation falling behind.
     *
     * @return the drainable segment count
     */
    public long getVlogSegmentsDrainable() {
        return vlogSegmentsDrainable;
    }

    /**
     * Returns the commits the L0 admission policy made dwell before admitting.
     *
     * @return the throttled write count
     */
    public long getWritesThrottled() {
        return writesThrottled;
    }

    /**
     * Returns the commits the L0 admission policy made wait for the flush queue
     * to drain.
     *
     * @return the blocked write count
     */
    public long getWritesBlocked() {
        return writesBlocked;
    }

    /**
     * Returns the total microseconds commits were held in L0 admission, dwell
     * plus wait.
     *
     * @return the write stall time in microseconds
     */
    public long getWriteStallUs() {
        return writeStallUs;
    }

    /**
     * Returns the commits admitted only because the admission wait ceiling
     * expired. Any of these means flush did not keep up with ingestion.
     *
     * @return the ceiling hit count
     */
    public long getWriteStallCeilingHits() {
        return writeStallCeilingHits;
    }

    @Override
    public String toString() {
        return "DbStats{" +
            "numColumnFamilies=" + numColumnFamilies +
            ", immutableMemtableCount=" + immutableMemtableCount +
            ", compactionPendingCount=" + compactionPendingCount +
            ", totalSstableCount=" + totalSstableCount +
            ", totalDataSizeBytes=" + totalDataSizeBytes +
            ", numOpenSstables=" + numOpenSstables +
            ", globalSeq=" + globalSeq +
            ", minSnapshotSeq=" + minSnapshotSeq +
            ", activeTxnCount=" + activeTxnCount +
            ", txnMemoryBytes=" + txnMemoryBytes +
            ", memtableBytes=" + memtableBytes +
            ", flushing=" + flushing +
            ", nextCfIndex=" + nextCfIndex +
            ", walGeneration=" + walGeneration +
            ", flushCount=" + flushCount +
            ", compactionCount=" + compactionCount +
            ", flushBytesWritten=" + flushBytesWritten +
            ", compactionBytesWritten=" + compactionBytesWritten +
            ", compactionBytesRead=" + compactionBytesRead +
            ", walBytesWritten=" + walBytesWritten +
            ", userBytesWritten=" + userBytesWritten +
            ", vlogFileSize=" + vlogFileSize +
            ", vlogValueCount=" + vlogValueCount +
            ", vlogUsedBytes=" + vlogUsedBytes +
            ", vlogStoredBytes=" + vlogStoredBytes +
            ", vlogLiveBytes=" + vlogLiveBytes +
            ", vlogSegmentCount=" + vlogSegmentCount +
            ", vlogBytesWritten=" + vlogBytesWritten +
            ", vlogDeadBytes=" + vlogDeadBytes +
            ", vlogReclaimCalls=" + vlogReclaimCalls +
            ", vlogReclaimPasses=" + vlogReclaimPasses +
            ", vlogSegmentsRetired=" + vlogSegmentsRetired +
            ", vlogSegmentsDrainable=" + vlogSegmentsDrainable +
            ", writesThrottled=" + writesThrottled +
            ", writesBlocked=" + writesBlocked +
            ", writeStallUs=" + writeStallUs +
            ", writeStallCeilingHits=" + writeStallCeilingHits +
            '}';
    }
}
