---
title: TidesDB Java API Reference
description: Complete Java API reference for TidesDB
---

If you want to download the source of this document, you can find it [here](https://github.com/tidesdb/tidesdb.github.io/blob/master/src/content/docs/reference/java.md).

<hr/>

## Getting Started

### Prerequisites

You **must** have the TidesDB shared C library installed on your system.  You can find the installation instructions [here](/reference/building/#_top).

## Requirements

- Java 11 or higher
- Maven 3.6+
- TidesDB native library installed on the system

### Building the JNI Library

```bash
cd src/main/c
cmake -S . -B build
cmake --build build
sudo cmake --install build
```

### Adding to Your Project

**Maven:**
```xml
<dependency>
    <groupId>com.tidesdb</groupId>
    <artifactId>tidesdb-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Usage

### Opening and Closing a Database

```java
import com.tidesdb.*;

public class Example {
    public static void main(String[] args) throws TidesDBException {
        Config config = Config.builder("./mydb")
            .numFlushThreads(2)
            .numCompactionThreads(2)
            .logLevel(LogLevel.INFO)
            .blockCacheSize(64 * 1024 * 1024)
            .maxOpenSSTables(256)
            .build();
        
        try (TidesDB db = TidesDB.open(config)) {
            System.out.println("Database opened successfully");
        }
    }
}
```

### Creating and Dropping Column Families

Column families are isolated key-value stores with independent configuration.

```java
// Create with default configuration
ColumnFamilyConfig cfConfig = ColumnFamilyConfig.defaultConfig();
db.createColumnFamily("my_cf", cfConfig);

// Create with custom configuration
ColumnFamilyConfig customConfig = ColumnFamilyConfig.builder()
    .writeBufferSize(128 * 1024 * 1024)
    .levelSizeRatio(10)
    .minLevels(5)
    .compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)
    .enableBloomFilter(true)
    .bloomFPR(0.01)
    .enableBlockIndexes(true)
    .syncMode(SyncMode.SYNC_INTERVAL)
    .syncIntervalUs(128000)
    .defaultIsolationLevel(IsolationLevel.READ_COMMITTED)
    .build();

db.createColumnFamily("custom_cf", customConfig);

// Drop column family
db.dropColumnFamily("my_cf");
```

### Working with Transactions

#### Writing Data

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    // Put a key-value pair (no expiration)
    txn.put(cf, "key".getBytes(), "value".getBytes());
    txn.commit();
}
```

#### Writing with TTL

```java
import java.time.Instant;

ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    // Set expiration time (Unix timestamp in seconds)
    long ttl = Instant.now().getEpochSecond() + 10; // Expire in 10 seconds
    
    txn.put(cf, "temp_key".getBytes(), "temp_value".getBytes(), ttl);
    txn.commit();
}
```

**TTL Examples:**
```java
// No expiration
long ttl = -1;

// Expire in 5 minutes
long ttl = Instant.now().getEpochSecond() + 300;

// Expire in 1 hour
long ttl = Instant.now().getEpochSecond() + 3600;

// Expire at specific time
long ttl = Instant.parse("2026-12-31T23:59:59Z").getEpochSecond();
```

#### Reading Data

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    byte[] value = txn.get(cf, "key".getBytes());
    System.out.println("Value: " + new String(value));
}
```

#### Deleting Data

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    txn.delete(cf, "key".getBytes());
    txn.commit();
}
```

#### Multi-Operation Transactions

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    // Multiple operations in one transaction
    txn.put(cf, "key1".getBytes(), "value1".getBytes());
    txn.put(cf, "key2".getBytes(), "value2".getBytes());
    txn.delete(cf, "old_key".getBytes());
    
    // Commit atomically -- all or nothing
    txn.commit();
} catch (TidesDBException e) {
    // Transaction is automatically rolled back on exception
    throw e;
}
```

### Iterating Over Data

Iterators provide efficient bidirectional traversal over key-value pairs.

#### Forward Iteration

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    try (TidesDBIterator iter = txn.newIterator(cf)) {
        iter.seekToFirst();
        
        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] value = iter.value();
            
            System.out.printf("Key: %s, Value: %s%n", 
                new String(key), new String(value));
            
            iter.next();
        }
    }
}
```

#### Backward Iteration

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

try (Transaction txn = db.beginTransaction()) {
    try (TidesDBIterator iter = txn.newIterator(cf)) {
        iter.seekToLast();
        
        while (iter.isValid()) {
            byte[] key = iter.key();
            byte[] value = iter.value();
            
            System.out.printf("Key: %s, Value: %s%n", 
                new String(key), new String(value));
            
            iter.prev();
        }
    }
}
```

#### Seeking to a Specific Key

```java
try (TidesDBIterator iter = txn.newIterator(cf)) {
    // Seek to first key >= target
    iter.seek("prefix".getBytes());
    
    // Seek to last key <= target
    iter.seekForPrev("prefix".getBytes());
}
```

### Getting Column Family Statistics

```java
ColumnFamily cf = db.getColumnFamily("my_cf");
Stats stats = cf.getStats();

System.out.println("Number of levels: " + stats.getNumLevels());
System.out.println("Memtable size: " + stats.getMemtableSize());
```

### Getting Cache Statistics

```java
CacheStats cacheStats = db.getCacheStats();

System.out.println("Cache enabled: " + cacheStats.isEnabled());
System.out.println("Total entries: " + cacheStats.getTotalEntries());
System.out.println("Hit rate: " + cacheStats.getHitRate());
```

### Manual Compaction and Flush

```java
ColumnFamily cf = db.getColumnFamily("my_cf");

// Trigger compaction
cf.compact();

// Flush memtable to disk
cf.flushMemtable();
```

### Transaction Isolation Levels

```java
// Begin transaction with specific isolation level
try (Transaction txn = db.beginTransaction(IsolationLevel.SERIALIZABLE)) {
    // Operations with serializable isolation
    txn.commit();
}
```

**Available Isolation Levels:**
- `READ_UNCOMMITTED` - Sees all data including uncommitted changes
- `READ_COMMITTED` - Sees only committed data (default)
- `REPEATABLE_READ` - Consistent snapshot, phantom reads possible
- `SNAPSHOT` - Write-write conflict detection
- `SERIALIZABLE` - Full read-write conflict detection (SSI)

### Savepoints

Savepoints allow partial rollback within a transaction:

```java
try (Transaction txn = db.beginTransaction()) {
    txn.put(cf, "key1".getBytes(), "value1".getBytes());
    
    txn.savepoint("sp1");
    txn.put(cf, "key2".getBytes(), "value2".getBytes());
    
    // Rollback to savepoint -- key2 is discarded, key1 remains
    txn.rollbackToSavepoint("sp1");
    
    // Commit -- only key1 is written
    txn.commit();
}
```

## Configuration Options

### Database Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `dbPath` | String | - | Path to the database directory |
| `numFlushThreads` | int | 2 | Number of flush threads |
| `numCompactionThreads` | int | 2 | Number of compaction threads |
| `logLevel` | LogLevel | INFO | Logging level |
| `blockCacheSize` | long | 64MB | Block cache size in bytes |
| `maxOpenSSTables` | long | 256 | Maximum open SSTable files |

### Column Family Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `writeBufferSize` | long | 64MB | Write buffer size |
| `levelSizeRatio` | long | 10 | Level size ratio |
| `minLevels` | int | 4 | Minimum number of levels |
| `compressionAlgorithm` | CompressionAlgorithm | NO_COMPRESSION | Compression algorithm |
| `enableBloomFilter` | boolean | false | Enable bloom filter |
| `bloomFPR` | double | 0.01 | Bloom filter false positive rate |
| `enableBlockIndexes` | boolean | false | Enable block indexes |
| `syncMode` | SyncMode | SYNC_NONE | Sync mode for durability |
| `syncIntervalUs` | long | 0 | Sync interval in microseconds |
| `defaultIsolationLevel` | IsolationLevel | READ_COMMITTED | Default isolation level |

## Testing

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=TidesDBTest#testOpenClose

# Run with verbose output
mvn test -X
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/tidesdb/tidesdb-java.git
cd tidesdb-java

# Build the JNI library
cd src/main/c
cmake -S . -B build
cmake --build build
sudo cmake --install build
cd ../../..

# Build the Java package
mvn clean package

# Install to local Maven repository
mvn install
```
