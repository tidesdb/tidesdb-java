# tidesdb-java

tidesdb-java is the official Java binding for TidesDB.

TidesDB is a fast and efficient key-value storage engine library. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This Java binding provides a safe, idiomatic Java interface to TidesDB with full support for all features.

## Features

- MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
- Snapshots and point-in-time reads at a named sequence
- Two-phase commit with recovery of transactions left in doubt
- Column families (isolated key-value stores with independent configuration)
- Bidirectional iterators with forward/backward traversal, seek, and range-scoped scans
- Range and prefix deletes that cost one entry however many keys they cover
- TTL (time to live) support with automatic key expiration
- Stacked encoding pipelines: LZ4, LZ4 Fast, ZSTD, Snappy, or none
- Key/value separation with a shared value log and background reclamation
- Partition range filters with configurable false positive rates
- Global block cache for hot blocks
- Savepoints for partial transaction rollback
- Transaction timeouts and cross-thread aborts
- Commit hooks for change data capture
- Statistics for column families, the database, the cache, write stalls, device I/O, encoding chains, and key ranges

## License

Multiple licenses apply:

- Mozilla Public License Version 2.0 (TidesDB)
- BSD 3-Clause (Snappy)
- BSD 2-Clause (LZ4)
- BSD 2-Clause (xxHash - Yann Collet)
- BSD (Zstandard)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
