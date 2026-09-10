---
title: TidesDB Java API Reference
description: Complete Java API reference for TidesDB
---

<div class="no-print">

If you want to download the source of this document, you can find it [here](https://github.com/tidesdb/tidesdb.github.io/blob/master/src/content/docs/reference/java.md).

<hr/>

</div>

## Getting Started

### Prerequisites

You **must** have the TidesDB shared C library installed on your system. You can find the installation instructions [here](/reference/building/#_top).

This binding targets **TidesDB 10.x**. It does not work against a 9.x library: the two have different public APIs and different on-disk formats.

## Requirements

- Java 11 or higher
- Maven 3.9.6+
- TidesDB 10.x native library installed on the system

### Building the JNI Library

```bash
cd src/main/c
cmake -S . -B build
cmake --build build
sudo cmake --install build
```

### Adding to Your Project

**Maven**

```xml
<dependency>
    <groupId>com.tidesdb</groupId>
    <artifactId>tidesdb-java</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

### Opening and Closing a Database

`TidesDB` implements `Closeable`, so use it with try-with-resources. Close every
`Transaction`, `TidesDBIterator`, and `Snapshot` derived from it first.

```java
import com.tidesdb.*;

Config config = Config.builder("/path/to/db")
    .logLevel(LogLevel.INFO)
    .memtableSyncMode(SyncMode.SYNC_FULL)
    .build();

try (TidesDB db = TidesDB.open(config)) {
    // ... use the database
}
```

Every field left at zero is resolved to the engine's own default, so a minimal
configuration is just a path:

```java
try (TidesDB db = TidesDB.open(Config.builder("/path/to/db").build())) {
    // ...
}
```

To see what the engine will actually pick, ask for the native defaults:

```java
Config defaults = Config.defaultConfig("/path/to/db");
System.out.println(defaults.getMemtableWriteBufferSize()); // e.g. 67108864
System.out.println(defaults.getValueSeparationThreshold()); // e.g. 1024
```

`defaultConfig` returns an immutable value; use `toBuilder()` to adjust it.

```java
Config config = Config.defaultConfig("/path/to/db")
    .toBuilder()
    .logToFile(true)
    .memtableL0QueueStallThreshold(16)
    .build();
```

A second handle on the same directory — from this process or any other — is
refused with `ERR_LOCKED`.

Closing reports no status: an I/O error while writing the last of the data is
logged rather than thrown. If you need your data on the device, ask for that
first with `syncWal()`, `checkpoint()`, or by running under `SYNC_FULL`.

### Raising the Open-File Limit

The engine reads the process open-file ceiling at open time and lowers
`maxOpenSSTables` to fit it, so an over-large setting costs descriptors it never
gets rather than failing opens. Raising the ceiling is an explicit, opt-in
action, and must happen **before** `open`.

```java
long ceiling = TidesDB.raiseOpenFileLimit(8192);
System.out.println("open-file ceiling is now " + ceiling);

try (TidesDB db = TidesDB.open(Config.builder("/path/to/db")
        .maxOpenSSTables(4096)
        .build())) {
    // ...
}
```

Passing a value of zero or less just reports the current ceiling.

### Creating and Dropping Column Families

A column family is an isolated key-value store with its own configuration. The
memtable, write-ahead log, block cache, and value log are database-level and
shared across all of them.

```java
ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
    .compression(CompressionAlgorithm.LZ4)
    .enableBloomFilter(true)
    .bloomFpr(0.01)
    .build();

db.createColumnFamily("users", cfConfig);

ColumnFamily users = db.getColumnFamily("users");

for (String name : db.listColumnFamilies()) {
    System.out.println(name);
}

db.dropColumnFamily("users");
```

`ColumnFamilyConfig.builder()` starts from the native defaults, so you only set
what you want to change. The `name` field on the config is ignored by
`createColumnFamily` — the name argument is authoritative.

Dropping destroys data and cannot be undone, and any `ColumnFamily` handle held
for that family is invalid afterwards.

#### Renaming and Cloning

```java
db.renameColumnFamily("users", "accounts");

// a point-in-time copy; later writes to the source do not appear in it
db.cloneColumnFamily("accounts", "accounts_snapshot");
```

Both flush the whole database memtable first — it is shared, so this is not just
that family's data — and claim the family against the compaction scheduler. Both
can report `ERR_LOCKED` if the family stays under compaction for the whole
quiesce window.

### Working with Transactions

Every read and write goes through a transaction.

#### Writing Data

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(cf, "key".getBytes(), "value".getBytes());
    txn.commit();
}
```

An empty value is allowed and stores the key present carrying nothing. That is a
distinct state from an absence: a read returns it with a zero length rather than
reporting the key missing.

#### Writing with TTL

The TTL is **how long the entry lives, in seconds from now**. Zero or negative
never expires. The engine converts it to an absolute deadline once, at the
boundary, so a long recovery cannot extend an entry's life.

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(cf, "session".getBytes(), "token".getBytes(), 3600); // one hour
    txn.commit();
}
```

> **Changed in 1.0.0.** In 0.8.x the TTL argument was an absolute Unix timestamp
> and `-1` meant no expiry. It is now a relative duration in seconds.

#### Reading Data

```java
try (Transaction txn = db.beginTransaction()) {
    byte[] value = txn.get(cf, "key".getBytes());
    if (value != null) {
        System.out.println(new String(value));
    }
    txn.rollback();
}
```

An absent key returns `null` rather than throwing.

For an existence probe that should not pollute the conflict footprint — a
primary-key uniqueness check, say — use the non-tracking forms:

```java
byte[] value = txn.getNoTrack(cf, key);   // reads without recording the read
boolean present = txn.contains(cf, key);  // existence only, no value read
```

#### Deleting Data

```java
try (Transaction txn = db.beginTransaction()) {
    txn.delete(cf, "key".getBytes());
    txn.commit();
}
```

#### Single-Delete

A tombstone the caller promises supersedes at most one put, so the two can be
reaped together at compaction.

**Caller contract:** between any two single-deletes on the same key, and from the
start of the key's history to its first single-delete, the key has been put at
most once. The engine cannot verify this; violating it can leave older puts
visible after the single-delete. When in doubt, prefer `delete`.

```java
txn.singleDelete(cf, "write-once-key".getBytes());
```

#### Range and Prefix Deletes

Both cost one entry however many keys they cover, and both delete keys written
*before* them as well as keys written after — so neither is the same as deleting
the keys that happen to be there when you call it. A write to a covered key
survives when it is newer: buffered after it in the same transaction, or
committed at a later sequence.

```java
try (Transaction txn = db.beginTransaction()) {
    // [lo, hi) — lower bound inclusive, upper bound exclusive
    txn.deleteRange(cf, "k05".getBytes(), "k10".getBytes());

    // null upper bound runs to the end of the family
    txn.deleteRange(cf, "archive:".getBytes(), null);

    // every key under a prefix
    txn.deletePrefix(cf, "session:".getBytes());

    txn.commit();
}
```

Each bound is at most `Transaction.MAX_RANGE_BOUND_SIZE` (256) bytes. An
interval delete holds its bounds in a fixed slot for the length of its commit,
which is what stops a concurrent write landing inside the range; a range too wide
to name in one call is expressed as several.

The delete is O(1) to write but not to reclaim: the keys it covers stay on disk
until a compaction rewrites the range, and reads pay a bounded interval lookup
until then.

At `SNAPSHOT` isolation and above the commit is refused with `ERR_CONFLICT` when
any key in the range was written after the transaction drew its snapshot.

#### Transaction Rollback

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(cf, "key".getBytes(), "value".getBytes());
    txn.rollback(); // nothing is written
}
```

#### Multi-Operation and Multi-Column-Family Transactions

A transaction spans column families, and the whole batch commits atomically.

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(users, "u1".getBytes(), "alice".getBytes());
    txn.put(orders, "o1".getBytes(), "u1:widget".getBytes());
    txn.delete(sessions, "s1".getBytes());
    txn.commit();
}
```

### Transaction Isolation Levels

| Level | Behaviour |
|---|---|
| `READ_UNCOMMITTED` | Every version is visible, including sequences still in progress. |
| `READ_COMMITTED` | The newest committed version, with the ceiling re-read on every operation. |
| `REPEATABLE_READ` | The ceiling is frozen at begin; a commit validates that every key it read still holds the version it read. |
| `SNAPSHOT` | Frozen ceiling, and a commit reserves each key it writes on a first-committer-wins basis. |
| `SERIALIZABLE` | Both of the above checks, plus the one that catches write skew. |

```java
try (Transaction txn = db.beginTransaction(IsolationLevel.SERIALIZABLE)) {
    // ...
    txn.commit();
}

// or take the column family's configured default
try (Transaction txn = db.beginTransaction(cf)) {
    // ...
    txn.commit();
}
```

You can ask which committed versions a transaction can see:

```java
long ceiling = txn.getReadSnapshot();
```

The engine's sequence is unsigned 64-bit, so a ceiling of `UINT64_MAX` — what
read-uncommitted filters at — arrives in Java as `-1`. Compare sequences with
`Long.compareUnsigned`.

### Savepoints

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(cf, "a".getBytes(), "1".getBytes());

    txn.savepoint("checkpoint");
    txn.put(cf, "b".getBytes(), "2".getBytes());
    txn.rollbackToSavepoint("checkpoint"); // "b" is discarded, "a" is kept

    txn.releaseSavepoint("checkpoint");    // or release without rolling back
    txn.commit();
}
```

Rolling back to or releasing a name that was never marked fails with
`ERR_NOT_FOUND`.

### Transaction Reset

Reuse a resolved transaction rather than freeing and reallocating one in a hot
loop.

```java
try (Transaction txn = db.beginTransaction()) {
    for (int batch = 0; batch < 1000; batch++) {
        txn.put(cf, key(batch), value(batch));
        txn.commit();
        txn.reset(IsolationLevel.READ_COMMITTED);
    }
}
```

### Timeouts and Aborts

An abandoned transaction holds its snapshot and its write reservations, which
keeps the reclamation floor down and stops compaction dropping old versions.
Bound one that may be left unresolved.

```java
Config config = Config.builder("/path/to/db")
    .txnTimeoutSeconds(300)   // database-wide default
    .build();

try (Transaction txn = db.beginTransaction()) {
    txn.setTimeout(30);       // override for this transaction
    // ...
    txn.setTimeout(0);        // clear it again
    txn.commit();
}
```

The transaction is not aborted in the background when the deadline passes: the
next operation on it notices, aborts it, and fails with `ERR_TXN_EXPIRED`. The
engine ages transactions against a clock refreshed once a second.

`requestAbort()` is the one method that may be called on a transaction owned by
a **different thread**. It exists for a caller that is itself the authority on
whether a transaction may proceed — a replication plugin whose cluster has
certified against it, say.

```java
txn.requestAbort();   // stores a flag and returns; nothing else changes here
```

The thread running the transaction observes the flag when it next enters an
operation and fails with `ERR_TXN_ABORTED` — a code of its own, so a caller can
tell an outside ruling from the engine's own `ERR_CONFLICT`. A transaction that
has already prepared is unaffected: it has voted, and only the coordinator's
decision may resolve it.

### Two-Phase Commit

`prepare` runs the same conflict checks as `commit` and durably logs the write
batch under a transaction id, but leaves the writes invisible and unapplied so a
coordinator can gather votes before deciding.

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(cf, "key".getBytes(), "value".getBytes());

    txn.prepare("xid-42".getBytes());
    assert txn.state() == TransactionState.PREPARED;

    // ... the coordinator gathers votes from every participant ...

    txn.commitPrepared();   // or txn.rollbackPrepared()
}
```

`TransactionState` reports `ACTIVE`, `PREPARED`, `COMMITTED`, or `ABORTED`. A
transient I/O failure in phase two leaves the transaction prepared so the
coordinator can retry.

After a restart, transactions that were prepared and never decided are listed
for the coordinator to finish:

```java
for (PreparedTransaction prepared : db.recoverPrepared()) {
    byte[] xid = prepared.getXid();
    try (Transaction txn = prepared.getTransaction()) {
        if (coordinatorSaysCommit(xid)) {
            txn.commitPrepared();
        } else {
            txn.rollbackPrepared();
        }
    }
}
```

One that was decided in the log is settled during open and never appears here.

### Snapshots and Point-in-Time Reads

A snapshot names the database as it stands now so it can be read again later. It
holds the reclamation floor at its own sequence for as long as it lives, which
is what keeps the versions it resolves to from being compacted away — and is
also its cost. Release it as soon as the point in time is no longer wanted.

```java
try (Snapshot snapshot = db.createSnapshot()) {
    System.out.println("reading as of " + snapshot.getSeq());

    // writes continue on the live database
    try (Transaction w = db.beginTransaction()) {
        w.put(cf, "key".getBytes(), "new".getBytes());
        w.commit();
    }

    // and this still sees the old value
    try (Transaction r = db.beginTransactionAtSnapshot(snapshot)) {
        byte[] asOfSnapshot = r.get(cf, "key".getBytes());
        r.rollback();
    }
}
```

Every transaction opened against a snapshot must be freed before the snapshot is
released, since they read versions only it keeps alive.

You can also read at an explicit sequence — one read back from
`getReadSnapshot()`, or recorded elsewhere:

```java
try (Transaction r = db.beginTransactionAtSeq(seq)) {
    // ...
    r.rollback();
}
```

This refuses rather than approximates. A sequence is readable only while
something holds the reclamation floor under it: an open transaction, or a
snapshot taken in advance. Once a collection has run past that sequence, the call
fails with `ERR_TOO_OLD` and reads nothing.

```java
long floor = db.getOldestReadableSeq(); // the oldest sequence still accepted
```

### Iterating Over Data

Iterators are created from a transaction and read at its snapshot. Close them
before freeing the transaction.

`isValid()` is the single way to ask whether the cursor is on an entry — every
positioning call leaves the iterator invalid rather than throwing when the merged
stream has nothing where it was asked to stand.

#### Forward Iteration

```java
try (Transaction txn = db.beginTransaction();
     TidesDBIterator it = txn.newIterator(cf)) {
    it.seekToFirst();
    while (it.isValid()) {
        System.out.println(new String(it.key()) + " = " + new String(it.value()));
        it.next();
    }
    txn.rollback();
}
```

Keys are ordered byte-wise (`memcmp`). A caller who wants a different order
encodes keys to be memcomparable — big-endian integers, sign-flipped signed
integers, inverted bytes for descending — since the engine carries no pluggable
comparator.

#### Backward Iteration

```java
it.seekToLast();
while (it.isValid()) {
    process(it.key(), it.value());
    it.prev();
}
```

#### Combined Key-Value Retrieval

One JNI crossing instead of two:

```java
KeyValue kv = it.keyValue();
byte[] key = kv.getKey();
byte[] value = kv.getValue();
```

#### Seeking

```java
it.seek("k100".getBytes());        // first key >= the target
it.seekForPrev("k100".getBytes()); // last key <= the target
```

#### Prefix Scans

```java
byte[] prefix = "user:".getBytes();
it.seek(prefix);
while (it.isValid() && startsWith(it.key(), prefix)) {
    process(it.key(), it.value());
    it.next();
}
```

#### Range Iterators

An iterator holds one open cursor per SSTable that could answer it, descends
each of them on every seek and compares each on every step. Telling it the range
up front lets it leave out the SSTables whose own key range cannot meet it, so a
scan of a narrow band costs what that band costs rather than what the whole
column family costs. On a family holding hundreds of SSTables that is the
difference between a scan that competes for the whole store and one that does
not.

```java
try (Transaction txn = db.beginTransaction();
     TidesDBIterator it = txn.newRangeIterator(cf, "k020".getBytes(), "k030".getBytes())) {
    it.seek("k020".getBytes());
    while (it.isValid() && compare(it.key(), "k030".getBytes()) < 0) {
        process(it.key(), it.value());
        it.next();
    }
    txn.rollback();
}
```

The range is a promise about what will be read, **not a fence the iterator
enforces**. Results are defined only inside it, because the SSTables that could
answer outside it were never opened: seeking or stepping past either end may
report absent a key that exists. Use `newIterator` for a scan whose extent is
not known in advance.

### Encoding Pipelines

A column family applies an ordered chain of encodings to its btree key-log
nodes, undone in reverse on read. The ids are recorded in the SSTable footer, so
a reader rebuilds the same chain from the file.

```java
// the common case: a single codec
ColumnFamilyConfig cfg = ColumnFamilyConfig.builder()
    .compression(CompressionAlgorithm.ZSTD)
    .build();

// or a chain, applied in order
ColumnFamilyConfig chained = ColumnFamilyConfig.builder()
    .encodingPipeline(CompressionAlgorithm.LZ4, CompressionAlgorithm.ZSTD)
    .build();

// or raw ids, for an encoding the enum does not name
ColumnFamilyConfig raw = ColumnFamilyConfig.builder()
    .encodingPipelineIds(2, 3)
    .build();
```

`compression(CompressionAlgorithm.NONE)` clears the pipeline so data is stored
verbatim. At most `ColumnFamilyConfig.MAX_ENCODING_PIPELINE` (8) entries are
allowed.

Every algorithm is always named by the enum, but a backend is only linked in
when its build option was set. Ask before choosing one, rather than discovering
it when a node fails to decode:

```java
if (TidesDB.isCompressionAvailable(CompressionAlgorithm.ZSTD)) {
    builder.compression(CompressionAlgorithm.ZSTD);
}
```

`CompressionAlgorithm.NONE` is always available.

### Commit Hook (Change Data Capture)

The hook fires synchronously after every transaction commit on that family,
receiving the full batch atomically. Keep the callback fast to avoid stalling
writers.

```java
cf.setCommitHook((ops, commitSeq) -> {
    for (CommitOp op : ops) {
        if (op.isDelete()) {
            replicateDelete(op.getKey());
        } else {
            replicatePut(op.getKey(), op.getValue(), op.getTtl());
        }
    }
    return 0; // non-zero is logged as a warning
});

// ... later
cf.clearCommitHook();
```

`CommitOp.getTtl()` is the **absolute expiry** the engine stored, not the
lifetime in seconds that `put` was given, so a hook forwarding the write
elsewhere reproduces the same expiry instant rather than restarting the clock. It
is `-1` when the entry never expires.

The hook fires after the WAL write, memtable apply, and commit-status marking
complete. A hook failure is logged but does not roll back the commit — the data
is already durable. An exception thrown out of the callback is caught and treated
as a failure.

Hooks are runtime-only and not persisted. After a restart, re-register them.
Setting a hook while one is installed replaces it; the old one is retired only
once every callback already inside it has returned.

### Maintenance

The memtable and write-ahead log are shared across every column family, so the
operations that act on them live on `TidesDB`. Compaction is per-family and lives
on `ColumnFamily`.

```java
db.flushMemtable();   // rotate and flush, waiting a bounded time for the queue to drain
db.isFlushing();      // whether an immutable is queued or flushing
db.syncWal();         // force an fsync of the write-ahead log
db.checkpoint();      // durability barrier: flush, then force vlog, WAL and manifest to disk

cf.compact();                                  // one forced pass over the family
cf.compactRange(startKey, endKey);             // every SSTable overlapping [start, end)
cf.isCompacting();                             // whether a merge is in progress
```

A `null` or empty endpoint on `compactRange` is unbounded on that side; both
unbounded is rejected in favour of `compact()`.

Every maintenance call can report `ERR_LOCKED`, and it always means the same
thing: the work was not done because something else held what it needed, and
asking again later is the remedy. It is never data loss and never a corrupt
database.

### Backup

```java
db.backup("/path/to/backup");
```

Flushes the memtable, then copies the manifest, the shared value log, and every
SSTable it references at a single manifest snapshot while compaction is held off,
so the copy references no file a merge could delete mid-copy. The result is
directly openable:

```java
try (TidesDB restored = TidesDB.open(Config.builder("/path/to/backup").build())) {
    // ...
}
```

### Updating Runtime Configuration

Every field of a column family configuration may change at runtime, since
byte-wise key ordering keeps all SSTables mergeable. The family name and id are
preserved; a rename is separate.

```java
ColumnFamilyConfig updated = ColumnFamilyConfig.builder()
    .compression(CompressionAlgorithm.ZSTD)
    .enableBloomFilter(true)
    .bloomFpr(0.05)
    .l1FileCountTrigger(8)
    .build();

cf.updateRuntimeConfig(updated, true); // true persists it in the manifest
```

Pass `false` to apply the change in memory only.

## Statistics

### Column Family Statistics

```java
CfStats stats = cf.getStats();

System.out.println("levels: " + stats.getNumLevels());
System.out.println("keys on disk: " + stats.getTotalKeys());
System.out.println("keys still in memory: " + stats.getUnflushedKeyCount());
System.out.println("on-disk size: " + stats.getTotalDataSize());
System.out.println("read amplification: " + stats.getReadAmp());
System.out.println("tombstone ratio: " + stats.getTombstoneRatio());
System.out.println("filter memory: " + stats.getFilterResidentBytes());

// per-level arrays, indexed by level - 1, valid for the first getNumLevels() entries
long[] sizes = stats.getLevelSizes();
int[] tables = stats.getLevelNumSstables();
long[] keys = stats.getLevelKeyCounts();
long[] tombstones = stats.getLevelTombstoneCounts();

// the configuration as the engine holds it, including the persisted name
ColumnFamilyConfig live = stats.getConfig();
```

`getTotalKeys()` plus `getUnflushedKeyCount()` is the live logical key count
including what is still in memory. `getTotalDataSize()` is the family's own
on-disk size, the sum of its key logs; what spilled to the shared value log is
database-level and reported as `DbStats.getVlogFileSize()`.

`getWalBytesWritten()` here is an attribution rather than a measurement — the
batch header and block framing belong to no single family, so these do not sum to
what the log wrote. The database-level figure is the measured one.

### Cardinality Estimate

```java
long distinct = cf.estimateCardinality();
```

### Database Statistics

```java
DbStats stats = db.getDbStats();

System.out.println("column families: " + stats.getNumColumnFamilies());
System.out.println("L0 queue depth: " + stats.getImmutableMemtableCount());
System.out.println("MVCC clock: " + stats.getGlobalSeq());
System.out.println("gc floor: " + stats.getMinSnapshotSeq());
System.out.println("live transactions: " + stats.getActiveTxnCount());
System.out.println("memtable bytes: " + stats.getMemtableBytes());

// write amplification terms
System.out.println("user bytes: " + stats.getUserBytesWritten());
System.out.println("wal bytes: " + stats.getWalBytesWritten());
System.out.println("flush bytes: " + stats.getFlushBytesWritten());
System.out.println("compaction bytes: " + stats.getCompactionBytesWritten());
System.out.println("vlog bytes: " + stats.getVlogBytesWritten());

// value log space
System.out.println("vlog live: " + stats.getVlogLiveBytes());
System.out.println("vlog dead: " + stats.getVlogDeadBytes());
System.out.println("drainable segments: " + stats.getVlogSegmentsDrainable());

// write admission
System.out.println("throttled: " + stats.getWritesThrottled());
System.out.println("blocked: " + stats.getWritesBlocked());
System.out.println("stall time (us): " + stats.getWriteStallUs());
System.out.println("ceiling hits: " + stats.getWriteStallCeilingHits());
```

`getMinSnapshotSeq()` is unsigned: when nothing holds the floor it sits at
`UINT64_MAX` and reads as `-1` in Java.

`getVlogLiveBytes()` is the figure space amplification is against — a store can
hold many gigabytes of values no tree can reach, and only this tells them apart
from the ones still worth keeping. A `getVlogSegmentsDrainable()` that stays high
is reclamation falling behind.

Any `getWriteStallCeilingHits()` at all means flush did not keep up with
ingestion.

### Cache Statistics

```java
CacheStats cache = db.getCacheStats();
System.out.println(cache.getHitRate());
System.out.println(cache.getTotalBytes() + " over " + cache.getNumPartitions() + " shards");
```

### Stall Statistics

Where writers have been made to wait. A write latency tail is answerable from
this alone: compare each reason's maximum against the tail you measured, and its
total against the others, rather than attaching a debugger to a stalled commit.

```java
StallStats stalls = db.getStallStats();

for (StallReason reason : StallReason.values()) {
    StallStat stat = stalls.get(reason);
    System.out.printf("%-16s count=%d total=%dus max=%dus%n",
        reason.getNativeName(), stat.getCount(), stat.getTotalUs(), stat.getMaxUs());
}
System.out.println("all waiting: " + stalls.getTotalUs() + "us");
```

| Reason | Meaning |
|---|---|
| `WAL_APPEND` | Waiting on the write-ahead log, for staging-ring space or for the record to reach the file. |
| `ROTATE_LOCK` | Waiting to take the rotation lock; another committer was rotating. |
| `ROTATE_WORK` | Performing the rotation, which this thread pays on everyone's behalf. |
| `ADMISSION` | Held by write admission because the unflushed backlog was too deep. |
| `MANIFEST_COMMIT` | Inside a manifest commit, which every flush install, compaction install and DDL serialises through. |

### I/O Statistics

The other half of the stall statistics: those say writers waited on the log, these
say whether the device was the reason. Bytes over total time is the throughput a
class actually achieved — compare it against what the storage can sustain,
because a saturated device and a stalled engine look identical from the
application.

```java
IoStats io = db.getIoStats();

for (IoClass cls : IoClass.values()) {
    IoStat stat = io.get(cls);
    System.out.printf("%-8s ops=%d bytes=%d %.1f MB/s max=%dus%n",
        cls.getNativeName(), stat.getOps(), stat.getBytes(),
        stat.getBytesPerSecond() / 1e6, stat.getMaxUs());
}
```

Only handles the engine opens through its descriptor manager are counted, which
is every key log and every write-ahead log. The value log and the manifest are
not, so this measures the two classes that compete for the device under load
rather than every byte the database writes.

### Encoding Statistics

What each encoding chain achieved, reported per chain rather than per column
family — a family can change its codec, and compaction rewrites data under
whichever pipeline is merging it, so a single figure per family would average
across settings that no longer apply.

```java
for (EncodingStats e : db.getKlogEncodingStats()) {
    System.out.printf("ids=%s %d -> %d bytes (%.2fx) over %d sstables%n",
        Arrays.toString(e.getIds()), e.getLogicalBytes(), e.getStoredBytes(),
        e.getRatio(), e.getItemCount());
}

for (EncodingStats e : db.getVlogEncodingStats()) {
    // same shape; itemCount counts values rather than sstables
}
```

At most `EncodingStats.MAX_CHAINS` (16) chains are reported.

### Range Statistics

What a query planner needs to know about a key range, both figures taken from one
layout snapshot so they describe the same instant.

```java
RangeStats range = cf.rangeStats("k000".getBytes(), "k500".getBytes());

System.out.println("sorted runs a scan would merge: " + range.getSstablesOverlapping());
System.out.println("live keys: " + range.getEstimatedKeys());
System.out.println("counted exactly: " + range.isKeysExact());
```

The count is memtable-aware, so a range whose data has not been flushed yet
reports a real cardinality rather than an SSTable overlap count. A range small
enough to walk is counted exactly and says so; a wider one is estimated from
SSTable metadata without walking, so the call stays cheap enough for plan time
whatever the range covers.

## Configuration Options

### Database Configuration

A field left at zero is resolved to the engine's default for the thread counts,
`blockCacheSize`, `maxOpenSSTables`, `vlogSegmentSize`,
`memtableWriteBufferSize`, `memtableSkipListMaxLevel` and
`memtableSkipListProbability`. The rest are used as given, `memtableSyncMode`
included, where zero is the meaningful value `SYNC_NONE`.

| Option | Type | Description |
|---|---|---|
| `dbPath` | `String` | Path to the database directory. |
| `numFlushThreads` | `int` | Flush worker threads. |
| `numCompactionThreads` | `int` | Compaction worker threads. |
| `logLevel` | `LogLevel` | Minimum severity to emit. |
| `blockCacheSize` | `long` | Bytes of database-level block cache for hot SSTable blocks. |
| `maxOpenSSTables` | `long` | Concurrently open SSTable handles; lowered at open to fit the process ceiling. |
| `logToFile` | `boolean` | Write the log to `LOG` inside the database directory rather than stderr. The sink is process-wide. |
| `logTruncationAt` | `long` | Bytes past which the log file is truncated and reopened; 0 for never. |
| `memtableWriteBufferSize` | `long` | Memory the active memtable may occupy before rotation. A budget, not a promise about flush size. |
| `memtableSkipListMaxLevel` | `int` | Skip list max level for the memtable. |
| `memtableSkipListProbability` | `float` | Skip list level probability, in [0.0, 1.0]. |
| `memtableSyncMode` | `SyncMode` | Durability mode for the write-ahead log. |
| `memtableSyncIntervalUs` | `long` | Fsync interval for `SYNC_INTERVAL`, in microseconds. |
| `valueSeparationThreshold` | `long` | Values at or above this size go to the shared value log. |
| `vlogSegmentSize` | `long` | Size at which the value log seals a segment. |
| `memtableL0QueueStallThreshold` | `int` | Immutable-queue depth at which writes stall; 0 never stalls. |
| `memtableIdleFlushSeconds` | `int` | How long the memtable may sit unwritten before the engine rotates it; 0 never does. |
| `txnTimeoutSeconds` | `long` | How long a transaction may stay active; 0 for no timeout. |

`memtableWriteBufferSize` is a memory budget rather than a promise about the size
of what a rotation flushes: an entry costs its key and value plus about a hundred
bytes of skip list node, pointer arrays and version struct. That overhead is
fixed per entry, so it is most of an entry holding a short value and almost none
of one holding a large one.

Keep `valueSeparationThreshold` at or under a quarter of a family's
`btreeKlogBlockSize` — an inlined value approaching the node size leaves a node
holding one entry and spends the btree fan-out that makes a lookup cheap. The
pairing is advisory, and a config that breaks it only logs a warning.

Left at 0, `memtableL0QueueStallThreshold` leaves the queue unbounded and a
writer outrunning the flush threads is never paced, so it is a value to set
deliberately rather than leave.

### Column Family Configuration

| Option | Type | Description |
|---|---|---|
| `levelSizeRatio` | `long` | Target size ratio between successive levels. |
| `minLevels` | `int` | Floor on the level count. |
| `dividingLevelOffset` | `int` | How far above the largest level the dividing level sits. |
| `keepValuesInline` | `boolean` | Hold every value in the key log whatever its size, ignoring the database threshold. |
| `btreeKlogBlockSize` | `long` | Target size of a btree key-log node; 0 leaves the choice to the btree. |
| `encodingPipeline` | `int[]` | Encoding ids applied in order, at most 8. |
| `enableBloomFilter` | `boolean` | Build a partition-range filter for point-get pruning. |
| `bloomFpr` | `double` | Target false-positive rate when the filter is enabled. |
| `defaultIsolationLevel` | `IsolationLevel` | Isolation for a transaction opened without an explicit level. |
| `l1FileCountTrigger` | `int` | L1 SSTable count that triggers compaction. |
| `tombstoneDensityTrigger` | `double` | Density above which an SSTable escalates compaction; 0 disables. |
| `tombstoneDensityMinEntries` | `long` | Minimum entry count to be judged by density; 0 imposes no minimum. |

A separated value costs a scan one value-log read per row, so a family scanned
far more than it is merged can be worth `keepValuesInline` even though its values
are large. The cost is the one the threshold exists to avoid: compaction rewrites
those bytes on every merge.

### Compression Algorithms

| Constant | Encoding id |
|---|---|
| `CompressionAlgorithm.NONE` | 0 |
| `CompressionAlgorithm.SNAPPY` | 1 |
| `CompressionAlgorithm.LZ4` | 2 |
| `CompressionAlgorithm.ZSTD` | 3 |
| `CompressionAlgorithm.LZ4_FAST` | 4 |

### Sync Modes

| Constant | Behaviour |
|---|---|
| `SyncMode.SYNC_NONE` | Nothing at commit. Fastest; a crash of either kind may lose commits. |
| `SyncMode.SYNC_FULL` | Fsync every commit. Slowest; no commit loss on crash. |
| `SyncMode.SYNC_INTERVAL` | Fsync on a background interval. Bounded loss window. |

A clean close loses nothing in any mode.

### Log Levels

| Constant | Value |
|---|---|
| `LogLevel.NONE` | 0 |
| `LogLevel.TRACE` | 1 |
| `LogLevel.INFO` | 2 |
| `LogLevel.WARN` | 3 |
| `LogLevel.ERROR` | 4 |

A larger value is more severe, so a higher threshold emits fewer lines.

## Error Handling

`TidesDBException` carries the native result code. `getErrorCode()` returns one
of the constants below and `getErrorMessage()` describes it without a native
call; `TidesDB.strerror(code)` asks the library for its own description.

| Constant | Value | Meaning |
|---|---|---|
| `ERR_SUCCESS` | 0 | Success. |
| `ERR_MEMORY` | -1 | Memory allocation failed. |
| `ERR_INVALID_ARGS` | -2 | Invalid arguments. |
| `ERR_NOT_FOUND` | -3 | Not found. |
| `ERR_IO` | -4 | I/O error. |
| `ERR_CORRUPTION` | -5 | Data corruption. |
| `ERR_EXISTS` | -6 | Already exists. |
| `ERR_CONFLICT` | -7 | The engine's first-committer-wins verdict. |
| `ERR_TOO_LARGE` | -8 | A value does not fit the space that must hold it. |
| `ERR_MEMORY_LIMIT` | -9 | Memory limit exceeded. |
| `ERR_INVALID_DB` | -10 | Invalid handle, or the database is closing. |
| `ERR_UNKNOWN` | -11 | Unknown error. |
| `ERR_LOCKED` | -12 | Transient contention. Retry. |
| `ERR_READONLY` | -13 | The database is read-only. |
| `ERR_TXN_EXPIRED` | -14 | The transaction's timeout passed. |
| `ERR_NO_SPACE` | -15 | The device or filesystem is out of space. |
| `ERR_TXN_ABORTED` | -16 | Aborted from outside through `requestAbort()`. |
| `ERR_TOO_OLD` | -17 | The point in time asked for is no longer reconstructable. |

### Retrying on Contention

`ERR_LOCKED` is transient contention, never a failed operation and never data
loss: something else held what the call needed, nothing was written, and the
remedy is always to try again. It is not confined to operations that take a
family exclusively — a read has to open SSTables and walk sources a compaction
may be moving, so any get, existence check or iterator step can report it too.
Treat it as retry, never as absence and never as an error to surface.

```java
for (int attempt = 0; ; attempt++) {
    try (Transaction txn = db.beginTransaction()) {
        byte[] value = txn.get(cf, key);
        txn.rollback();
        return value;
    } catch (TidesDBException e) {
        if (!e.isRetryable() || attempt >= 10) {
            throw e;
        }
        Thread.sleep(1L << attempt);
    }
}
```

`ERR_NO_SPACE` is distinct from `ERR_IO` because it says what to do about it:
the data is intact and the operation succeeds once space is freed, where an I/O
error carries no such promise.

## Migrating from 0.8.x

Version 1.0.0 targets TidesDB 10.x and is a breaking change throughout. The
on-disk format is also new: **a 9.x database is not readable by 10.x**, and moves
across by dumping and reloading through the API.

### Removed

| Removed | Why / what to use instead |
|---|---|
| Object store mode, S3 connector, `ObjectStoreConfig`, `S3Config` | Not part of the 10.x public API. |
| Replica mode, `promoteToPrimary()` | Removed with object store mode. |
| `registerComparator()` | Keys are ordered byte-wise; encode keys to be memcomparable instead. |
| `purge()`, `ColumnFamily.purge()` | Use `flushMemtable()` and `compact()`. |
| `cancelBackgroundWork()` | Removed; `close()` handles shutdown. |
| `deleteColumnFamily(ColumnFamily)` | Use `dropColumnFamily(String)`. |
| `ColumnFamilyConfig` INI save/load | Removed; configure in code. |
| `rangeCost()` | Replaced by `rangeStats()`, which reports real figures rather than an opaque score. |
| `Stats` | Replaced by `CfStats`. |

### Moved

The memtable and write-ahead log are now database-level, so the operations on
them moved from `ColumnFamily` to `TidesDB`:

| 0.8.x | 1.0.0 |
|---|---|
| `cf.flushMemtable()` | `db.flushMemtable()` |
| `cf.isFlushing()` | `db.isFlushing()` |
| `cf.syncWal()` | `db.syncWal()` |
| `cf.rangeCost(a, b)` | `cf.rangeStats(a, b)` |
| `cf.getStats()` returning `Stats` | `cf.getStats()` returning `CfStats` |
| `db.checkpoint(dir)` | `db.checkpoint()` — a durability barrier in the live database, not a directory copy |

Per-column-family memtable, WAL, sync and skip-list settings moved from
`ColumnFamilyConfig` to `Config`, and the `unifiedMemtable*` options are gone
because the memtable is always shared now.

### Changed

- **TTL is now relative.** `txn.put(cf, key, value, ttl)` takes seconds from now;
  zero or negative never expires. It was an absolute Unix timestamp with `-1`
  for no expiry.
- **Compression constants renamed** to match the C enum: `NO_COMPRESSION` →
  `NONE`, `SNAPPY_COMPRESSION` → `SNAPPY`, `LZ4_COMPRESSION` → `LZ4`,
  `ZSTD_COMPRESSION` → `ZSTD`, `LZ4_FAST_COMPRESSION` → `LZ4_FAST`. They are set
  through `compression(...)` or `encodingPipeline(...)` rather than a single
  `compressionAlgorithm` field.
- **`LogLevel` values changed** to `NONE(0)`, `TRACE(1)`, `INFO(2)`, `WARN(3)`,
  `ERROR(4)`. `DEBUG` and `FATAL` are gone.
- **Iterator positioning no longer throws at the end of a range.** `seek`,
  `seekForPrev`, `seekToFirst` and `seekToLast` leave the iterator invalid
  instead, matching `next` and `prev`. Check `isValid()`.
- **New error codes** `ERR_READONLY`, `ERR_TXN_EXPIRED`, `ERR_NO_SPACE`,
  `ERR_TXN_ABORTED` and `ERR_TOO_OLD`.

### Added

Snapshots and point-in-time reads, two-phase commit and prepared-transaction
recovery, range and prefix deletes, non-tracking reads and existence checks,
transaction timeouts and cross-thread aborts, range iterators, encoding
pipelines, cardinality estimates, and the stall, I/O, encoding and range
statistics families.

## Testing

```bash
# Run all tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=TidesDBTest

# Point the JVM at a locally built JNI library
./mvnw test -Dtest.jvm.args="-Djava.library.path=/usr/local/lib"
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/tidesdb/tidesdb-java.git
cd tidesdb-java

# Build and install the JNI library
cd src/main/c
cmake -S . -B build
cmake --build build
sudo cmake --install build
cd ../../..

# Build the Java package
./mvnw package

# Install to the local Maven repository
./mvnw install
```
