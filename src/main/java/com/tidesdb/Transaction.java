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

import java.io.Closeable;

/**
 * A transaction in TidesDB. Every read and write goes through one, and
 * {@code Transaction} implements {@link java.io.Closeable} for use with
 * try-with-resources.
 *
 * <p>Close transactions before closing the owning {@link TidesDB} instance, and
 * before releasing any {@link Snapshot} they were opened against. After this
 * transaction is freed, all operations except {@link #requestAbort()} and
 * {@code close()} throw {@link IllegalStateException}.
 *
 * <p>This class is not guaranteed to be thread-safe. The one exception is
 * {@link #requestAbort()}, which may be called from a thread other than the one
 * running the transaction.
 */
public class Transaction implements Closeable {

    static {
        NativeLibrary.load();
    }

    /**
     * The longest bound {@link #deleteRange(ColumnFamily, byte[], byte[])} and
     * {@link #deletePrefix(ColumnFamily, byte[])} accept. An interval delete
     * holds its bounds in a fixed slot for the length of its commit, which is
     * what stops a concurrent write landing inside the range; a range too wide to
     * name in one call is expressed as several.
     */
    public static final int MAX_RANGE_BOUND_SIZE = 256;

    private long nativeHandle;
    private volatile boolean freed = false;

    Transaction(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /* ===== writes ===== */

    /**
     * Buffers a put into this transaction.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @param value the value; must not be {@code null}. It may be empty, which
     *        stores the key present carrying nothing — a distinct state from an
     *        absence, since a read returns it with a zero length rather than
     *        reporting the key missing
     * @param ttlSeconds how long the entry lives, in <strong>seconds from
     *        now</strong>; zero or negative never expires. The engine converts it
     *        to an absolute deadline once, here at the boundary, so a long
     *        recovery cannot extend an entry's life
     * @throws IllegalArgumentException if {@code cf} is {@code null}, {@code key}
     *         is {@code null} or empty, or {@code value} is {@code null}
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native put fails
     */
    public void put(ColumnFamily cf, byte[] key, byte[] value, long ttlSeconds)
            throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        requireKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        nativePut(nativeHandle, cf.getNativeHandle(), key, value, ttlSeconds);
    }

    /**
     * Buffers a put with no expiration. Equivalent to
     * {@code put(cf, key, value, 0)}.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @param value the value; must not be {@code null}
     * @throws IllegalArgumentException if {@code cf} is {@code null}, {@code key}
     *         is {@code null} or empty, or {@code value} is {@code null}
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native put fails
     */
    public void put(ColumnFamily cf, byte[] key, byte[] value) throws TidesDBException {
        put(cf, key, value, 0);
    }

    /**
     * Buffers a delete (tombstone) into this transaction.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or
     *         {@code key} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native delete fails
     */
    public void delete(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        requireKey(key);
        nativeDelete(nativeHandle, cf.getNativeHandle(), key);
    }

    /**
     * Buffers a single-delete: a tombstone the caller promises supersedes at most
     * one put, so the two can be reaped together at compaction.
     *
     * <p>Caller contract: between any two single-deletes on the same key, and
     * from the start of the key's history to its first single-delete, the key has
     * been put at most once. The engine cannot verify this; violating it can
     * leave older puts visible after the single-delete. When in doubt, prefer
     * {@link #delete(ColumnFamily, byte[])}.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or
     *         {@code key} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native single-delete fails
     */
    public void singleDelete(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        requireKey(key);
        nativeSingleDelete(nativeHandle, cf.getNativeHandle(), key);
    }

    /**
     * Buffers a delete of every key in the column family from {@code lo}
     * (inclusive) to {@code hi} (exclusive).
     *
     * <p>It costs one entry however many keys it covers, and it deletes keys
     * written before it as well as keys written after it — so it is not the same
     * as deleting the keys that happen to be there when you call it. A write to a
     * key inside the range survives it when it is newer: buffered after it in the
     * same transaction, or committed at a later sequence.
     *
     * <p>The delete is O(1) to write but not to reclaim: the keys it covers stay
     * on disk until a compaction rewrites the range.
     *
     * <p>At {@link IsolationLevel#SNAPSHOT} and above the commit is refused with
     * {@link TidesDBException#ERR_CONFLICT} when any key in the range was written
     * after this transaction drew its snapshot.
     *
     * @param cf the target column family; must not be {@code null}
     * @param lo the inclusive lower bound; must not be {@code null} or empty, and
     *        at most {@link #MAX_RANGE_BOUND_SIZE} bytes. Keys are never empty, so
     *        a single zero byte is a lower bound below every key there can be
     * @param hi the exclusive upper bound, or {@code null}/empty to run to the end
     *        of the family; at most {@link #MAX_RANGE_BOUND_SIZE} bytes
     * @throws IllegalArgumentException if {@code cf} is {@code null}, {@code lo}
     *         is {@code null} or empty, or either bound is too long
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native delete fails
     */
    public void deleteRange(ColumnFamily cf, byte[] lo, byte[] hi) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        if (lo == null || lo.length == 0) {
            throw new IllegalArgumentException("Lower bound cannot be null or empty");
        }
        requireBoundSize(lo, "Lower bound");
        if (hi != null) {
            requireBoundSize(hi, "Upper bound");
        }
        nativeDeleteRange(nativeHandle, cf.getNativeHandle(), lo, hi);
    }

    /**
     * Buffers a delete of every key in the column family that begins with
     * {@code prefix}. This is the one-bound form of
     * {@link #deleteRange(ColumnFamily, byte[], byte[])} and carries the same
     * semantics and costs.
     *
     * <p>A two-phase transaction holds its prefix from the prepare until phase
     * two resolves it, so a write under it is refused for as long as it stays in
     * doubt.
     *
     * @param cf the target column family; must not be {@code null}
     * @param prefix the prefix to delete under; must not be {@code null} or empty,
     *        and at most {@link #MAX_RANGE_BOUND_SIZE} bytes. A whole family is
     *        dropped with {@link TidesDB#dropColumnFamily(String)} rather than
     *        deleted a prefix at a time
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or
     *         {@code prefix} is {@code null}, empty, or too long
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native delete fails
     */
    public void deletePrefix(ColumnFamily cf, byte[] prefix) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        if (prefix == null || prefix.length == 0) {
            throw new IllegalArgumentException("Prefix cannot be null or empty");
        }
        requireBoundSize(prefix, "Prefix");
        nativeDeletePrefix(nativeHandle, cf.getNativeHandle(), prefix);
    }

    /* ===== reads ===== */

    /**
     * Reads a key at the transaction snapshot, recording the read into the
     * conflict footprint.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @return the value, or {@code null} if not found
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or
     *         {@code key} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native read fails, including
     *         {@link TidesDBException#ERR_LOCKED} when contention left the read
     *         unservable and it should be retried
     */
    public byte[] get(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        requireKey(key);
        return nativeGet(nativeHandle, cf.getNativeHandle(), key);
    }

    /**
     * Reads a key at the transaction snapshot <em>without</em> recording it into
     * the conflict footprint, for existence probes — such as primary-key
     * uniqueness — that should not pollute the write-write base.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @return the value, or {@code null} if not found
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or
     *         {@code key} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native read fails
     */
    public byte[] getNoTrack(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        requireKey(key);
        return nativeGetNoTrack(nativeHandle, cf.getNativeHandle(), key);
    }

    /**
     * Non-tracking existence check at the transaction snapshot.
     *
     * @param cf the target column family; must not be {@code null}
     * @param key the key; must not be {@code null} or empty
     * @return {@code true} if the key is present
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or
     *         {@code key} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native check fails, including
     *         {@link TidesDBException#ERR_LOCKED} when contention left the read
     *         unservable and it should be retried
     */
    public boolean contains(ColumnFamily cf, byte[] key) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        requireKey(key);
        return nativeContains(nativeHandle, cf.getNativeHandle(), key);
    }

    /**
     * Returns the sequence ceiling this transaction's reads filter at, so a
     * caller can reason about which committed versions the transaction can and
     * cannot see.
     *
     * <p>The engine's sequence is an unsigned 64-bit value, so a ceiling of
     * {@code UINT64_MAX} arrives here as {@code -1}. Compare sequences with
     * {@link Long#compareUnsigned(long, long)} rather than {@code <}.
     *
     * @return {@code -1} (an unsigned {@code UINT64_MAX}) for read-uncommitted,
     *         the current sequence for read-committed, and the sequence frozen at
     *         begin for repeatable-read and stronger
     * @throws IllegalStateException if this transaction is freed
     */
    public long getReadSnapshot() {
        checkNotFreed();
        return nativeReadSnapshot(nativeHandle);
    }

    /* ===== iterators ===== */

    /**
     * Creates an iterator over a column family at this transaction's snapshot.
     *
     * <p>Close the returned iterator before freeing this transaction.
     *
     * @param cf the column family to iterate; must not be {@code null}
     * @return a new iterator
     * @throws IllegalArgumentException if {@code cf} is {@code null}
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native iterator cannot be created
     */
    public TidesDBIterator newIterator(ColumnFamily cf) throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        return new TidesDBIterator(nativeNewIterator(nativeHandle, cf.getNativeHandle()));
    }

    /**
     * Creates an iterator over the part of a column family a scan will actually
     * read, at this transaction's snapshot.
     *
     * <p>An iterator holds one open cursor per SSTable that could answer it.
     * Telling it the range up front lets it leave out the SSTables whose own key
     * range cannot meet it, so a scan of a narrow band costs what that band costs
     * rather than what the whole column family costs.
     *
     * <p>The range is a promise about what will be read, not a fence the iterator
     * enforces. Results are defined only inside it, because the SSTables that
     * could answer outside it were never opened: seeking or stepping past either
     * end may report absent a key that exists. Use
     * {@link #newIterator(ColumnFamily)} for a scan whose extent is not known in
     * advance.
     *
     * @param cf the column family to iterate; must not be {@code null}
     * @param lower the range start, inclusive; must not be {@code null} or empty
     * @param upper the range end, inclusive for the purpose of choosing SSTables,
     *        so an exclusive end may be passed unchanged; must not be {@code null}
     *        or empty
     * @return a new iterator
     * @throws IllegalArgumentException if {@code cf} is {@code null}, or either
     *         bound is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native iterator cannot be created
     */
    public TidesDBIterator newRangeIterator(ColumnFamily cf, byte[] lower, byte[] upper)
            throws TidesDBException {
        checkNotFreed();
        requireCf(cf);
        if (lower == null || lower.length == 0) {
            throw new IllegalArgumentException("Lower bound cannot be null or empty");
        }
        if (upper == null || upper.length == 0) {
            throw new IllegalArgumentException("Upper bound cannot be null or empty");
        }
        return new TidesDBIterator(
            nativeNewRangeIterator(nativeHandle, cf.getNativeHandle(), lower, upper));
    }

    /* ===== lifecycle ===== */

    /**
     * Commits the transaction, writing its batch to the write-ahead log and
     * memtable.
     *
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException with {@link TidesDBException#ERR_CONFLICT} when
     *         another transaction committed a conflicting write first, or
     *         {@link TidesDBException#ERR_TXN_ABORTED} when an outside authority
     *         aborted it through {@link #requestAbort()}
     */
    public void commit() throws TidesDBException {
        checkNotFreed();
        nativeCommit(nativeHandle);
    }

    /**
     * Discards the transaction's buffered writes without committing.
     *
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native rollback fails
     */
    public void rollback() throws TidesDBException {
        checkNotFreed();
        nativeRollback(nativeHandle);
    }

    /**
     * Bounds how long this transaction may stay active, overriding the database's
     * {@link Config#getTxnTimeoutSeconds()} for this one. The deadline is
     * measured from the moment of this call, so calling it again on a live
     * transaction extends it.
     *
     * <p>The transaction is not aborted in the background when the deadline
     * passes: the next operation on it notices, aborts it, and fails with
     * {@link TidesDBException#ERR_TXN_EXPIRED}.
     *
     * @param seconds seconds from now at which it expires, or &le; 0 to clear any
     *        timeout
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the transaction is already resolved
     */
    public void setTimeout(long seconds) throws TidesDBException {
        checkNotFreed();
        nativeSetTimeout(nativeHandle, seconds);
    }

    /**
     * Aborts a transaction that another thread is running, so its next operation
     * fails with {@link TidesDBException#ERR_TXN_ABORTED} rather than whatever it
     * would otherwise have done. This is the one method here that may be called
     * on a transaction owned by a different thread.
     *
     * <p>It exists for a caller that is itself the authority on whether a
     * transaction may proceed. The call stores a flag and does nothing else: the
     * transaction is not rolled back here, and the thread running it observes the
     * flag when it next enters an operation and frees the transaction as it
     * normally would.
     *
     * <p>A request that lands just after the running thread has checked lets the
     * operation in progress finish and stops the one after it.
     *
     * <p>A transaction that has already prepared is not affected: it has voted,
     * and only the coordinator's decision may resolve it.
     *
     * <p>Calling this on a freed transaction is a no-op.
     */
    public void requestAbort() {
        long handle = nativeHandle;
        if (freed || handle == 0) {
            return;
        }
        nativeRequestAbort(handle);
    }

    /**
     * Resets this transaction for reuse at a new isolation level, discarding its
     * buffered state. This avoids the cost of freeing and reallocating
     * transaction resources in hot loops.
     *
     * @param isolation the isolation level for the reset transaction; must not be
     *        {@code null}
     * @throws IllegalArgumentException if {@code isolation} is {@code null}
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the reset fails
     */
    public void reset(IsolationLevel isolation) throws TidesDBException {
        checkNotFreed();
        if (isolation == null) {
            throw new IllegalArgumentException("Isolation level cannot be null");
        }
        nativeReset(nativeHandle, isolation.getValue());
    }

    /**
     * Reports this transaction's lifecycle state, so a coordinator can tell a
     * prepared transaction from a resolved one.
     *
     * @return the current state
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native call fails
     */
    public TransactionState state() throws TidesDBException {
        checkNotFreed();
        return TransactionState.fromValue(nativeState(nativeHandle));
    }

    /* ===== two-phase commit ===== */

    /**
     * Two-phase-commit phase one: runs the same conflict checks as
     * {@link #commit()} and durably logs the write batch under the given
     * transaction id, but leaves the writes invisible and unapplied so a
     * coordinator can gather votes from every participant before deciding.
     *
     * <p>On success the transaction moves to {@link TransactionState#PREPARED}
     * and holds its snapshot and reservations until resolved with
     * {@link #commitPrepared()} or {@link #rollbackPrepared()}. A read-only
     * transaction prepares with nothing durable and needs no phase two. The xid
     * is copied.
     *
     * @param xid the transaction id to record durably; must not be {@code null}
     *        or empty
     * @throws IllegalArgumentException if {@code xid} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the prepare fails
     */
    public void prepare(byte[] xid) throws TidesDBException {
        checkNotFreed();
        if (xid == null || xid.length == 0) {
            throw new IllegalArgumentException("Transaction id cannot be null or empty");
        }
        nativePrepare(nativeHandle, xid);
    }

    /**
     * Two-phase-commit phase two: durably logs the decision to commit the
     * prepared transaction, then applies its batch and makes it visible. Valid
     * only on a prepared transaction. A transient I/O failure leaves it prepared
     * so the coordinator can retry.
     *
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException with {@link TidesDBException#ERR_INVALID_ARGS} if
     *         the transaction is not prepared
     */
    public void commitPrepared() throws TidesDBException {
        checkNotFreed();
        nativeCommitPrepared(nativeHandle);
    }

    /**
     * Two-phase-commit phase two: durably logs the decision to roll back the
     * prepared transaction and releases its reservations. Nothing was applied, so
     * nothing is undone. Valid only on a prepared transaction.
     *
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException with {@link TidesDBException#ERR_INVALID_ARGS} if
     *         the transaction is not prepared
     */
    public void rollbackPrepared() throws TidesDBException {
        checkNotFreed();
        nativeRollbackPrepared(nativeHandle);
    }

    /* ===== savepoints ===== */

    /**
     * Marks a named savepoint in this transaction to roll back to later.
     *
     * @param name the savepoint name; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException if the native savepoint creation fails
     */
    public void savepoint(String name) throws TidesDBException {
        checkNotFreed();
        requireName(name);
        nativeSavepoint(nativeHandle, name);
    }

    /**
     * Discards writes buffered since a named savepoint.
     *
     * @param name the savepoint name; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException with {@link TidesDBException#ERR_NOT_FOUND} when
     *         no savepoint carries that name
     */
    public void rollbackToSavepoint(String name) throws TidesDBException {
        checkNotFreed();
        requireName(name);
        nativeRollbackToSavepoint(nativeHandle, name);
    }

    /**
     * Releases a named savepoint without rolling back.
     *
     * @param name the savepoint name; must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code name} is {@code null} or empty
     * @throws IllegalStateException if this transaction is freed
     * @throws TidesDBException with {@link TidesDBException#ERR_NOT_FOUND} when
     *         no savepoint carries that name
     */
    public void releaseSavepoint(String name) throws TidesDBException {
        checkNotFreed();
        requireName(name);
        nativeReleaseSavepoint(nativeHandle, name);
    }

    /**
     * Frees the transaction, rolling it back if still open, and releases all
     * native resources.
     *
     * <p>This method is idempotent; subsequent calls are no-ops.
     */
    public void free() {
        if (!freed && nativeHandle != 0) {
            long handle = nativeHandle;
            freed = true;
            nativeHandle = 0;
            nativeFree(handle);
        }
    }

    /**
     * Closes this transaction. Equivalent to {@link #free()}.
     */
    @Override
    public void close() {
        free();
    }

    private void checkNotFreed() {
        if (freed) {
            throw new IllegalStateException("Transaction has been freed");
        }
    }

    private static void requireCf(ColumnFamily cf) {
        if (cf == null) {
            throw new IllegalArgumentException("Column family cannot be null");
        }
        cf.checkOwnerOpen();
    }

    private static void requireKey(byte[] key) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    private static void requireName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Savepoint name cannot be null or empty");
        }
    }

    private static void requireBoundSize(byte[] bound, String what) {
        if (bound.length > MAX_RANGE_BOUND_SIZE) {
            throw new IllegalArgumentException(
                what + " must be at most " + MAX_RANGE_BOUND_SIZE + " bytes, was: " + bound.length);
        }
    }

    long getNativeHandle() {
        return nativeHandle;
    }

    private static native void nativePut(long handle, long cfHandle, byte[] key, byte[] value,
                                         long ttlSeconds) throws TidesDBException;

    private static native byte[] nativeGet(long handle, long cfHandle, byte[] key) throws TidesDBException;

    private static native byte[] nativeGetNoTrack(long handle, long cfHandle, byte[] key) throws TidesDBException;

    private static native boolean nativeContains(long handle, long cfHandle, byte[] key) throws TidesDBException;

    private static native long nativeReadSnapshot(long handle);

    private static native void nativeDelete(long handle, long cfHandle, byte[] key) throws TidesDBException;

    private static native void nativeSingleDelete(long handle, long cfHandle, byte[] key) throws TidesDBException;

    private static native void nativeDeleteRange(long handle, long cfHandle, byte[] lo, byte[] hi) throws TidesDBException;

    private static native void nativeDeletePrefix(long handle, long cfHandle, byte[] prefix) throws TidesDBException;

    private static native long nativeNewIterator(long handle, long cfHandle) throws TidesDBException;

    private static native long nativeNewRangeIterator(long handle, long cfHandle, byte[] lower,
                                                      byte[] upper) throws TidesDBException;

    private static native void nativeCommit(long handle) throws TidesDBException;

    private static native void nativeRollback(long handle) throws TidesDBException;

    private static native void nativeSetTimeout(long handle, long seconds) throws TidesDBException;

    private static native void nativeRequestAbort(long handle);

    private static native void nativeReset(long handle, int isolationLevel) throws TidesDBException;

    private static native int nativeState(long handle) throws TidesDBException;

    private static native void nativePrepare(long handle, byte[] xid) throws TidesDBException;

    private static native void nativeCommitPrepared(long handle) throws TidesDBException;

    private static native void nativeRollbackPrepared(long handle) throws TidesDBException;

    private static native void nativeSavepoint(long handle, String name) throws TidesDBException;

    private static native void nativeRollbackToSavepoint(long handle, String name) throws TidesDBException;

    private static native void nativeReleaseSavepoint(long handle, String name) throws TidesDBException;

    private static native void nativeFree(long handle);
}
