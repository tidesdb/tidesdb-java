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
 * Names the database as it stood at a point in time, so it can be read again
 * later through {@link TidesDB#beginTransactionAtSnapshot(Snapshot)}.
 *
 * <p>A snapshot holds the reclamation floor at its own sequence for as long as
 * it lives, which is what keeps the versions it resolves to from being compacted
 * away, and is also its cost: the same cost a long-running transaction has.
 * Release it as soon as the point in time is no longer wanted.
 *
 * <p>{@code Snapshot} implements {@link java.io.Closeable} for use with
 * try-with-resources. Every {@link Transaction} opened against a snapshot must
 * be freed before the snapshot is released, since they read versions only it
 * keeps alive.
 *
 * <p>This class is not guaranteed to be thread-safe.
 */
public class Snapshot implements Closeable {

    static {
        NativeLibrary.load();
    }

    private long nativeHandle;
    private final long seq;
    private boolean released = false;

    Snapshot(long nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.seq = nativeSeq(nativeHandle);
    }

    /**
     * Returns the sequence this snapshot reads at, for reporting and for
     * comparing against {@link DbStats#getMinSnapshotSeq()}.
     *
     * <p>The sequence is read once at creation and stays available after the
     * snapshot is released.
     *
     * @return the snapshot sequence
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Releases the snapshot and the reclamation floor it was holding.
     *
     * <p>This method is idempotent; subsequent calls are no-ops.
     */
    public void release() {
        if (!released && nativeHandle != 0) {
            nativeRelease(nativeHandle);
            nativeHandle = 0;
            released = true;
        }
    }

    /**
     * Releases this snapshot. Equivalent to {@link #release()}.
     */
    @Override
    public void close() {
        release();
    }

    /**
     * Reports whether this snapshot has been released.
     *
     * @return {@code true} once released
     */
    public boolean isReleased() {
        return released;
    }

    long getNativeHandle() {
        if (released) {
            throw new IllegalStateException("Snapshot has been released");
        }
        return nativeHandle;
    }

    @Override
    public String toString() {
        return "Snapshot{seq=" + seq + ", released=" + released + '}';
    }

    private static native long nativeSeq(long handle);

    private static native void nativeRelease(long handle);
}
