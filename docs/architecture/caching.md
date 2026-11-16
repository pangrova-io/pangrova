---
title: "Caching Target Architecture"
description: "Target caching architecture plus the implemented metadata cache providers and configuration"
sidebar_label: "Caching (Target)"
---

# Pangrova — Caching Target Architecture

> **Status.** Draft target architecture. The implementation includes Velox Native caching, Worker eviction and locality, and a separate Coordinator cache for federation metadata. The RAM/SSD manager and prefetch controller below describe the proposed Native design.

> **Summary.** Target design for multi-tier caching and smart prefetch.

## 1. Cache Architecture Overview

![Cache architecture: metadata L1 and generation-checked L2 caches, Native async cache, proposed RAM/SSD manager, and remote object storage](/img/diagrams/caching-layers.svg)

**Design goals:** reduce remote I/O, keep cache state derived and rebuildable, and separate implemented caches from the proposed unified multi-tier manager.

## 2. Cache SPI Design (Extensible Interface)

```cpp
// CacheSpi.h - Cache layer SPI interface definition
#pragma once

#include <arrow/buffer.h>
#include <folly/futures/Future.h>
#include <string>
#include <memory>

namespace pangrova {
namespace cache {

/**
 * Cache Key - uniquely identifies cached data
 *
 * Design principles:
 * - fileId + offset + length locate the physical data block
 * - snapshotId binds the query snapshot version, ensuring snapshot isolation
 * - deleteFileSetHash is aware of delete file changes, preventing dirty reads
 */
struct CacheKey {
    std::string fileId;            // Unique file identifier (e.g., S3 path + ETag)
    int64_t offset;                // Offset within the file
    int64_t length;                // Data length
    int64_t snapshotId;            // Snapshot version bound to the query
    std::string deleteFileSetHash; // Hash of the associated delete files

    std::string toString() const {
        return fileId + ":" + std::to_string(offset) + ":" +
               std::to_string(length) + ":" + std::to_string(snapshotId) +
               ":" + deleteFileSetHash;
    }

    bool operator==(const CacheKey& other) const {
        return fileId == other.fileId &&
               offset == other.offset &&
               length == other.length &&
               snapshotId == other.snapshotId &&
               deleteFileSetHash == other.deleteFileSetHash;
    }
};

struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
        return std::hash<std::string>{}(key.toString());
    }
};

/**
 * Cache Entry - cache entry metadata
 */
struct CacheEntry {
    CacheKey key;
    int64_t size;
    int64_t accessCount;
    int64_t lastAccessTime;
    bool pinned;               // Whether pinned (not evictable)
};

/**
 * Cache Statistics - cache statistics
 */
struct CacheStats {
    int64_t hits = 0;
    int64_t misses = 0;
    int64_t evictions = 0;
    int64_t bytesRead = 0;
    int64_t bytesWritten = 0;
    int64_t currentSize = 0;
    int64_t maxSize = 0;

    double hitRate() const {
        int64_t total = hits + misses;
        return total > 0 ? static_cast<double>(hits) / total : 0.0;
    }
};


/**
 * Cache SPI - cache layer abstract interface
 *
 * Design principles:
 * 1. Extensible: supports multiple cache implementations (RAM, SSD, Redis, etc.)
 * 2. Asynchronous: all IO operations return a Future, supporting non-blocking
 * 3. Zero-copy: avoid data copying whenever possible
 */
class Cache {
public:
    virtual ~Cache() = default;

    /**
     * Get cached data
     * @param key cache key
     * @return Future<Buffer>; returns data on hit, nullptr on miss
     */
    virtual folly::Future<std::shared_ptr<arrow::Buffer>> get(const CacheKey& key) = 0;

    /**
     * Write data to the cache
     * @param key    cache key
     * @param buffer data buffer
     * @return Future<bool>; returns true on successful write
     */
    virtual folly::Future<bool> put(const CacheKey& key, std::shared_ptr<arrow::Buffer> buffer) = 0;

    /**
     * Remove cached data
     */
    virtual folly::Future<bool> remove(const CacheKey& key) = 0;

    /**
     * Check whether it exists
     */
    virtual bool contains(const CacheKey& key) = 0;

    /**
     * Pin a cache entry (prevent eviction)
     */
    virtual void pin(const CacheKey& key) = 0;
    virtual void unpin(const CacheKey& key) = 0;

    /**
     * Get statistics
     */
    virtual CacheStats getStats() const = 0;

    /**
     * Clear the cache
     */
    virtual void clear() = 0;

    /**
     * Get the cache name (used for logging and monitoring)
     */
    virtual std::string name() const = 0;
};

/**
 * Cache Factory - cache factory interface
 */
class CacheFactory {
public:
    virtual ~CacheFactory() = default;
    virtual std::string type() const = 0;
    virtual std::unique_ptr<Cache> create(const std::unordered_map<std::string, std::string>& config) = 0;
};

/**
 * Cache Registry - cache registry
 */
class CacheRegistry {
public:
    static CacheRegistry& instance();

    void registerFactory(std::unique_ptr<CacheFactory> factory);
    CacheFactory* getFactory(const std::string& type);
    std::vector<std::string> getRegisteredTypes() const;
};

// Auto-registration macro
#define REGISTER_CACHE_FACTORY(FactoryClass) \
    static bool _registered_##FactoryClass = []() { \
        CacheRegistry::instance().registerFactory(std::make_unique<FactoryClass>()); \
        return true; \
    }()

} // namespace cache
} // namespace pangrova
```

### 2.1 Java cache contracts

`pangrova-cache-api` publishes `CacheKey`, `CacheLookup`, `CacheStore` and `CacheStoreFactory` with a JDK-only runtime. Memory and Redis factories register in their respective provider JARs. A backend implements atomic group and entry invalidation, generation-checked fills and bounded-lifetime values. Metadata owns the serialization and catalog deployment identity. Server Coordinator discovers factories, applies configuration and opens one backend for its metadata subsystem. The Native engine retains ownership of its data-plane caches. [Metadata cache](./metadata.md#35-metadata-cache)

## 3. RAM Cache Implementation

`RamCache` implements the `Cache` SPI, providing a high-performance in-memory cache.

Core design:

| Feature | Detail |
|---------|--------|
| Storage | `mmap`/`madvise` allocation, zero fragmentation, supports arbitrary sizes |
| Concurrency | `folly::ConcurrentHashMap` + segmented LRU locks |
| Eviction | LRU (default), LFU, FIFO, TTL, Adaptive (LRU+LFU) |
| Config | `maxSize` (8GB default), `evictionThreshold` (90%), `asyncEviction` |

Eviction trigger: when `currentSize > maxSize × evictionThreshold`, eviction runs asynchronously, selecting victim entries per the strategy. The LRU list achieves O(1) updates via the `CacheItem.lruIter` doubly-linked iterator.



**Eviction Strategy Details**:

| Strategy | Algorithm | Key Data Structure | Use Case |
|----------|-----------|-------------------|----------|
| **LRU** (default) | Least Recently Used — evicts entry with oldest access time | Doubly-linked list (`CacheItem.lruIter`), O(1) access/update | Interactive queries with temporal locality |
| **LFU** | Least Frequently Used — evicts entry with lowest access count; uses frequency decay (halve all counts every `decayInterval=10min`) to prevent frequency accumulation | `std::priority_queue<CacheItem, freq>` + periodic decay sweep | Stable hot-set workloads |
| **FIFO** | First In First Out — evicts oldest inserted entry regardless of access | Ring buffer of insertion-ordered keys | ETL/batch sequential scans |
| **TTL** | Time-To-Live — evicts entries older than `ttlDuration` (default: 30min); checked lazily on access + periodic sweep (every 60s) | Sorted set by `expireTime = insertTime + ttlDuration`; `CacheEntry.expireTime` field | Time-sensitive data with known staleness window |
| **Adaptive** | Combines LRU + LFU using ARC (Adaptive Replacement Cache) algorithm; maintains two ghost lists (recently-evicted-by-LRU, recently-evicted-by-LFU) to dynamically adjust the LRU/LFU partition ratio | Two LRU lists (T1 recency, T2 frequency) + two ghost lists (B1, B2); partition parameter `p` adjusted on ghost hits | Mixed workloads with unknown access patterns |

**TTL Integration**: When TTL strategy is active, `Cache.put()` internally sets `CacheEntry.expireTime = now + config.ttlDuration`. The `CacheEntry` struct is extended with an optional `int64_t expireTime` field (0 = no TTL). A background sweep thread runs every 60s to remove expired entries. On `get()`, expired entries return `nullptr` (lazy expiry).


## 4. SSD Cache Implementation

`SsdCache` implements the `Cache` SPI, providing a large-capacity disk cache.

Core design:

| Feature | Detail |
|---------|--------|
| Storage | Sharded file storage (`numShards` independent files, each containing data + index) |
| I/O | Direct I/O (bypasses the OS Page Cache, avoiding double caching) |
| Write pattern | Log-structured append writes; periodic Compaction cleans up deleted data |
| Integrity | Optional checksum verification to detect data corruption |
| Config | `cacheDir`, `maxSize` (100GB default), `fileSize` (1GB/file), `numShards` (16) |

Shard routing: `shardIndex = hash(CacheKey) % numShards`; each shard is locked independently, reducing contention.

## 5. Multi-Level Cache Manager

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Multi-Level Cache Architecture                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Data flow:                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Read Request                                                           │   │
│  │       │                                                                  │   │
│  │       ▼                                                                  │   │
│  │  ┌─────────────┐  Hit                                                   │   │
│  │  │  RAM Cache  │ ────────────────────────────────────► Return Data     │   │
│  │  │    (L1)     │                                                        │   │
│  │  └──────┬──────┘                                                        │   │
│  │         │ Miss                                                          │   │
│  │         ▼                                                                │   │
│  │  ┌─────────────┐  Hit                                                   │   │
│  │  │  SSD Cache  │ ────► Promote to RAM ────────────────► Return Data    │   │
│  │  │    (L2)     │                                                        │   │
│  │  └──────┬──────┘                                                        │   │
│  │         │ Miss                                                          │   │
│  │         ▼                                                                │   │
│  │  ┌─────────────┐                                                        │   │
│  │  │   Remote    │ ────► Write to SSD ────► Write to RAM ► Return Data   │   │
│  │  │  Storage    │       (async)            (sync)                        │   │
│  │  └─────────────┘                                                        │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Eviction flow:                                                                  │
│  RAM full → evict to SSD (demote)                                              │
│  SSD full → evict directly (no write-back to Remote)                           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

`CacheManager` coordinates the two-tier RAM and SSD caches, managing data flow between tiers.

Core interfaces:

| Method | Description |
|--------|-------------|
| `read(key, loader) → Future<Buffer>` | Multi-level lookup: RAM → SSD → loader; auto-populate on miss |
| `readBatch(keys, loader) → Future<vector<Buffer>>` | Batch read with adjacent request coalescing |
| `prefetch(keys, loader)` | Async prefetch, non-blocking |
| `pin(key)` / `unpin(key)` | Pin entry to prevent eviction |
| `getStats() → AggregatedStats` | Aggregated RAM + SSD stats with overall hit rate |

Data flow rules:
- SSD hit + `promoteOnHit=true` → promote to RAM (async)
- RAM evict + `demoteOnEvict=true` → demote to SSD (async)
- SSD evict → discard (no write-back to Remote)

Config: `RamCacheConfig` + `SsdCacheConfig` + `enableSsdCache` + `promoteOnHit` + `demoteOnEvict` + `prefetchThreads` (4 default).



```cpp
// CacheManager.h — Multi-level cache coordinator
#pragma once

#include "pangrova/cache/CacheSpi.h"
#include <folly/futures/Future.h>
#include <functional>

namespace pangrova {
namespace cache {

enum class MemoryPressure; // Forward decl — see resource-management.md

/**
 * CacheManager — coordinates RAM and SSD cache tiers.
 *
 * Owns RamCache (L1) and SsdCache (L2) instances. Manages data flow
 * between tiers (promote on SSD hit, demote on RAM evict).
 * Integrates with ResourceManager via setMemoryPressureMode().
 */
class CacheManager {
public:
    CacheManager(
        std::unique_ptr<Cache> ramCache,
        std::unique_ptr<Cache> ssdCache,
        const CacheManagerConfig& config);

    /**
     * Multi-level read: RAM → SSD → loader.
     * Auto-populates cache on miss. Promotes SSD hits to RAM if configured.
     * @param key    cache key
     * @param loader fallback data loader (called on full miss)
     */
    folly::Future<std::shared_ptr<arrow::Buffer>> read(
        const CacheKey& key,
        std::function<folly::Future<std::shared_ptr<arrow::Buffer>>()> loader);

    /** Batch read with adjacent request coalescing. */
    folly::Future<std::vector<std::shared_ptr<arrow::Buffer>>> readBatch(
        const std::vector<CacheKey>& keys,
        std::function<folly::Future<std::shared_ptr<arrow::Buffer>>(const CacheKey&)> loader);

    /** Async prefetch — non-blocking, best-effort. */
    void prefetch(
        const std::vector<CacheKey>& keys,
        std::function<folly::Future<std::shared_ptr<arrow::Buffer>>(const CacheKey&)> loader);

    void pin(const CacheKey& key);
    void unpin(const CacheKey& key);

    /**
     * Adjust eviction aggressiveness based on memory pressure.
     * Called by ResourceManager (06 §3) when pressure level changes.
     * HIGH → aggressive eviction; CRITICAL → evict all unpinned entries.
     */
    void setMemoryPressureMode(MemoryPressure pressure);

    /** Mark entries for a given snapshot as stale (lowest eviction priority). */
    void markSnapshotStale(int64_t snapshotId);

    /** Aggregated stats across RAM + SSD tiers. */
    CacheStats getAggregatedStats() const;

private:
    std::unique_ptr<Cache> ramCache_;
    std::unique_ptr<Cache> ssdCache_;
    CacheManagerConfig config_;
};

struct CacheManagerConfig {
    bool enableSsdCache = true;
    bool promoteOnHit = true;   // Promote SSD hit to RAM
    bool demoteOnEvict = true;  // Demote RAM evict to SSD
    int prefetchThreads = 4;
};

} // namespace cache
} // namespace pangrova
```


## 6. Smart Prefetch Design

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Adaptive Prefetch Architecture                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                  │
│  • Hide IO latency: preload data before it is needed                            │
│  • Adaptive: dynamically adjust the prefetch strategy based on access patterns  │
│  • Resource-aware: adjust prefetch volume based on memory pressure and network bandwidth │
│                                                                                  │
│  Prefetch strategies:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Strategy          │ Trigger condition     │ Prefetch scope             │   │
│  │  ────────────────────────────────────────────────────────────────────── │   │
│  │  Sequential        │ Sequential-scan detection │ Next N Row Groups        │   │
│  │  Column-based      │ Column access frequency │ Subsequent data of hot columns │   │
│  │  Query-aware       │ Query plan analysis   │ All columns referenced in the plan │   │
│  │  Coalescing        │ Adjacent request detection │ Merge small requests into large ones │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Prefetch flow:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  TableScan Operator                                                     │   │
│  │       │                                                                  │   │
│  │       │ 1. Analyze the Split list                                        │   │
│  │       ▼                                                                  │   │
│  │  ┌─────────────────┐                                                    │   │
│  │  │ Prefetch Planner│                                                    │   │
│  │  │                 │                                                    │   │
│  │  │ • Identify hot columns │                                             │   │
│  │  │ • Compute prefetch window │                                          │   │
│  │  │ • Generate prefetch plan │                                           │   │
│  │  └────────┬────────┘                                                    │   │
│  │           │                                                              │   │
│  │           │ 2. Submit prefetch tasks                                    │   │
│  │           ▼                                                              │   │
│  │  ┌─────────────────┐     ┌─────────────────┐                           │   │
│  │  │ Prefetch Queue  │ ──► │ Prefetch Worker │ ──► Cache                 │   │
│  │  │ (priority queue) │     │ (background pool)│                           │   │
│  │  └─────────────────┘     └─────────────────┘                           │   │
│  │                                                                          │   │
│  │       │ 3. Execute the current Split concurrently                       │   │
│  │       ▼                                                                  │   │
│  │  Process Current Split (data already in Cache)                          │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```cpp
// PrefetchManager — core interface (implementation omitted for brevity)
namespace pangrova::cache {

class PrefetchManager {
public:
    explicit PrefetchManager(
        std::shared_ptr<CacheManager> cacheManager,
        const PrefetchConfig& config);

    /** Submit prioritized prefetch requests */
    void submitPrefetch(const std::vector<PrefetchRequest>& requests,
                        std::function<folly::Future<std::shared_ptr<arrow::Buffer>>(const CacheKey&)> loader);

    /** Query-plan-aware prefetch: analyze splits + required columns */
    void prefetchForQuery(
        const std::string& queryId,
        const std::vector<SplitInfo>& splits,
        const std::vector<std::string>& requiredColumns);

    /** Record column access for adaptive prefetch */
    void recordColumnAccess(const std::string& columnName, int64_t bytes, double selectivity);

    /** Get hot columns by access frequency */
    std::vector<std::string> getHotColumns(int topN = 10) const;

    /** Cancel prefetch on query cancellation */
    void cancelPrefetch(const std::string& queryId);

    /** Prefetch statistics */
    PrefetchStats getStats() const;
};

} // namespace pangrova::cache
```

Config: `prefetchThreads` (4), `prefetchWindowSize` (8 Row Groups), `maxPrefetchBytes` (256MB), `coalesceThreshold` (64KB gaps merged), `adaptivePrefetch` (true).

The implementation uses a priority queue (`std::priority_queue<PrefetchRequest>`) for scheduling; a smaller `PrefetchRequest.priority` means higher priority. The coalescing logic merges adjacent requests within `coalesceThreshold` into a single large IO.

### 6.1 Adaptive Prefetch Algorithm

Adaptive prefetch dynamically adjusts the prefetch strategy based on runtime access patterns, avoiding blind prefetch that wastes bandwidth and cache space.

**Adaptive decision model**:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Adaptive Prefetch Decision Model                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Input signals:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Signal              │ Source                │ Weight                    │   │
│  │  ────────────────────────────────────────────────────────────────────── │   │
│  │  prefetchHitRate     │ PrefetchStats         │ 0.4 (core metric)        │   │
│  │  cacheHitRate        │ CacheStats            │ 0.2                      │   │
│  │  memoryPressure      │ ResourceManager       │ 0.2 (inverse: high pressure → reduce) │   │
│  │  sequentialRatio     │ AccessPattern         │ 0.2 (sequential-scan ratio) │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Decision outputs:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  prefetchWindowSize  │ Dynamically adjust the prefetch window (1-32 Row Groups) │   │
│  │  maxPrefetchBytes    │ Dynamically adjust the prefetch byte cap          │   │
│  │  prefetchEnabled     │ Fully disable prefetch in extreme cases           │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Adjustment rules** (evaluated every 30 seconds):

| Condition | Action | Reason |
|------|------|------|
| prefetchHitRate > 80% | windowSize = min(windowSize × 2, 32) | Prefetch is effective, enlarge the window |
| prefetchHitRate 50-80% | windowSize unchanged | Prefetch is mediocre, keep as is |
| prefetchHitRate < 50% | windowSize = max(windowSize / 2, 1) | Prefetch is wasteful, shrink the window |
| prefetchHitRate < 20% for 3 consecutive cycles | prefetchEnabled = false (pause 60s) | Prefetch is ineffective, pause to avoid waste |

| memoryPressure >= HIGH | maxPrefetchBytes = configMax / 4 | Memory is tight, reduce prefetch volume; MemoryPressure levels defined in [06 §3](./resource-management.md): HIGH=85-95%, CRITICAL=95-100% |
| memoryPressure >= CRITICAL | prefetchEnabled = false | Memory is critical, fully disable prefetch; see [06 §3](./resource-management.md) |
| sequentialRatio > 90% | Use the Sequential strategy, double the window | Sequential scan, high prefetch benefit |
| sequentialRatio < 30% | Use the Column-based strategy, halve the window | Random access, low prefetch benefit |

`AdaptivePrefetchController` evaluates every 30 seconds, dynamically adjusting the three parameters `windowSize`, `maxPrefetchBytes`, and `enabled` according to the signals and rules above. It outputs a `PrefetchParameters` struct for `PrefetchManager` to use.


## 7. Cache and Connector Integration

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Cache Integration with Connector                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Integration points:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Native Connector (DataSourceReader)                                    │   │
│  │       │                                                                  │   │
│  │       │ 1. Build CacheKey (fileId + offset + length)                     │   │
│  │       ▼                                                                  │   │
│  │  ┌─────────────────┐                                                    │   │
│  │  │  CachedReader   │  ← Wraps the original Reader, adds caching transparently │   │
│  │  │                 │                                                    │   │
│  │  │  read() {       │                                                    │   │
│  │  │    key = buildKey(file, offset, len);                               │   │
│  │  │    data = cacheManager.read(key, () -> rawReader.read());           │   │
│  │  │    return data;                                                      │   │
│  │  │  }              │                                                    │   │
│  │  └─────────────────┘                                                    │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  CacheKey construction rules:                                                    │
│  • fileId = s3://bucket/path/file.parquet#etag=xxx                             │
│  • offset = Row Group start offset                                              │
│  • length = Row Group length                                                    │
│  • snapshotId = the Iceberg snapshot ID bound to the current query              │
│  • deleteFileSetHash = hash of the delete files set associated with this data file │
│  • Granularity: Row Group level (typically 64MB-128MB)                          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```cpp
// CachedDataSourceReader.h - cache-backed data source reader
#pragma once

#include "pangrova/connector/DataSource.h"
#include "pangrova/cache/CacheManager.h"

namespace pangrova {
namespace connector {

/**
 * Cache-backed data source reader
 * Decorator pattern; transparently adds caching to any DataSourceReader
 */
class CachedDataSourceReader : public DataSourceReader {
public:
    CachedDataSourceReader(
        std::unique_ptr<DataSourceReader> delegate,
        std::shared_ptr<cache::CacheManager> cacheManager,
        std::shared_ptr<cache::PrefetchManager> prefetchManager);

    std::shared_ptr<arrow::Schema> schema() const override {
        return delegate_->schema();
    }

    std::shared_ptr<arrow::RecordBatch> readNext() override {
        if (!hasNext()) return nullptr;

        // 1. Build CacheKey
        auto key = buildCacheKey(currentSplit_, currentRowGroup_);

        // 2. Read via CacheManager (automatically handles cache hit/miss)
        auto future = cacheManager_->read(key, [this]() {
            return readFromStorage();
        });

        // 3. Trigger the next batch of prefetch
        triggerPrefetch();

        // 4. Wait for the data and return
        auto buffer = future.get();
        return deserialize(buffer);
    }

    bool hasNext() const override {
        return delegate_->hasNext();
    }

    ReaderStats getStats() const override {
        auto stats = delegate_->getStats();
        stats.cacheHits = cacheHits_;
        stats.cacheMisses = cacheMisses_;
        return stats;
    }

    void close() override {
        delegate_->close();
    }

private:
    cache::CacheKey buildCacheKey(
        const SplitInfo& split, int rowGroupIndex);  // includes snapshotId + deleteFileSetHash
    folly::Future<std::shared_ptr<arrow::Buffer>> readFromStorage();
    std::shared_ptr<arrow::RecordBatch> deserialize(std::shared_ptr<arrow::Buffer> buffer);
    void triggerPrefetch();

    std::unique_ptr<DataSourceReader> delegate_;
    std::shared_ptr<cache::CacheManager> cacheManager_;
    std::shared_ptr<cache::PrefetchManager> prefetchManager_;


    SplitInfo currentSplit_;
    int currentRowGroup_ = 0;
    int64_t cacheHits_ = 0;
    int64_t cacheMisses_ = 0;
};

} // namespace connector
} // namespace pangrova
```

## 8. Cache Consistency with Snapshots and Delete Files

### 8.1 Snapshot-Bound CacheKey

Every cache lookup **must** include the `snapshotId` that the current query is bound to. This ensures that different queries reading different snapshots of the same table never share cache entries, even if the underlying data file is identical.

```
Query-A (snapshot 100) → CacheKey{file1, 0, 64MB, 100, "abc"} → Cache Entry A
Query-B (snapshot 102) → CacheKey{file1, 0, 64MB, 102, "def"} → Cache Entry B (different)
```

**Snapshot Expiry Eviction**: When Iceberg's `expire_snapshots` procedure removes old snapshots, the corresponding cache entries become unreachable (no future query will bind to that snapshot). These stale entries are marked and asynchronously evicted by a background cleanup thread:

```
1. Coordinator receives snapshot expiry event (via Iceberg listener)
2. Broadcasts expired snapshot IDs to all Workers
3. Each Worker's CacheManager scans index for matching snapshotId
4. Matching entries marked as stale (lowest eviction priority)
5. Background eviction thread reclaims stale entries during next cycle
```

**Integration with MaintenanceScheduler** ([streaming.md §2.10](./streaming.md)):

`MaintenanceScheduler.expireSnapshotsAsync()` is the trigger source for snapshot expiry. After MaintenanceScheduler completes snapshot expiration, it must notify the Cache layer to clean up the corresponding cache entries. Interaction flow (design target — `CacheEvictionNotifier` and the Worker-side `POST /v1/cache/expire-snapshots` endpoint are not yet implemented; until they land, snapshot-bound entries are reclaimed by the eviction policy):

```
MaintenanceScheduler.expireSnapshotsAsync(tableName)
  → TableMaintenance.expireSnapshotsAsync()
    → Iceberg Table.expireSnapshots().commit()
      → Returns List<Long> expiredSnapshotIds
  → CacheEvictionNotifier.notifySnapshotExpired(tableName, expiredSnapshotIds)
    → Coordinator broadcasts to all Workers (planned HTTP POST /v1/cache/expire-snapshots)
      → Each Worker's CacheManager marks matching entries as stale
```

`CacheEvictionNotifier` is the planned bridge interface between MaintenanceScheduler and CacheManager:

```java
/**
 * Bridge between MaintenanceScheduler and CacheManager.
 * Called after snapshot expiration completes.
 */
public interface CacheEvictionNotifier {
    void notifySnapshotExpired(String tableName, List<Long> expiredSnapshotIds);
    void notifyCompactionCompleted(String tableName, List<String> rewrittenFileIds);
}
```

### 8.2 Delete File Invalidation Strategy

Iceberg uses two types of delete files that affect cache validity:

| Delete Type | Mechanism | Cache Impact |
|-------------|-----------|--------------|
| **Position Delete** | Identifies rows by file path + row position | Cached data block contains deleted rows; must apply delete filter |
| **Equality Delete** | Identifies rows by column value predicate | Cached data block may contain deleted rows; must apply delete filter |

**`deleteFileSetHash` Computation**:

```
deleteFileSetHash = SHA-256(
    sort(
        [deleteFile.path + ":" + deleteFile.recordCount
         for deleteFile in associatedDeleteFiles]
    )
)
```

When new delete files are added (e.g., after a DELETE or MERGE operation), the `deleteFileSetHash` changes, causing cache misses for the new snapshot. This is the desired behavior — the new query reads fresh data with deletes applied.

**Invalidation Rules**:

1. **New delete files added** → `deleteFileSetHash` changes → automatic cache miss for new snapshot queries → fresh data loaded and cached
2. **Delete files compacted away** (after compaction merges deletes into data files) → new data files have new `fileId` → old cache entries become unreachable → background eviction reclaims
3. **No explicit invalidation needed** — the CacheKey design ensures correctness by construction; stale entries are naturally evicted via LRU/TTL

### 8.3 Concurrent Read/Write Cache Behavior

Pangrova guarantees **snapshot isolation at the cache level**: concurrent readers and writers to the same table never interfere with each other.

```
Timeline:
─────────────────────────────────────────────────────────────────────
  t0: Writer begins INSERT into table T
  t1: Reader starts query Q1, binds to snapshot S1
  t2: Writer commits → new snapshot S2 created
  t3: Reader Q1 continues reading with snapshot S1 (unaffected)
  t4: New reader Q2 starts, binds to snapshot S2
─────────────────────────────────────────────────────────────────────

Cache state:
  Q1 → CacheKey{..., snapshotId=S1, ...} → reads from / populates S1 entries
  Q2 → CacheKey{..., snapshotId=S2, ...} → reads from / populates S2 entries
  No conflict: different snapshotId → different cache entries
```

**Guarantees**:

| Scenario | Behavior |
|----------|----------|
| Read during concurrent write | Reader bound to old snapshot; cache entries isolated by snapshotId |
| Write commits while read in progress | No cache invalidation for in-flight reads; they continue with their snapshot |
| Multiple readers on different snapshots | Each reader has independent cache entries; no cross-contamination |
| Snapshot expiry during read | In-flight reads hold a reference; entries evicted only after all readers complete |

**Design Principle**: The cache layer does **not** perform active invalidation on write commits. Instead, correctness is achieved through the CacheKey's `snapshotId` dimension — each snapshot version occupies a separate key space, making concurrent read/write conflicts structurally impossible.

## 9. Cache Error Handling



Cache errors are internal system events — they are **never** propagated to clients. The cache layer degrades gracefully by falling back to the data source on any failure.

### 9.1 Error Scenarios and Recovery

| Scenario | Detection | Recovery Behavior | Error Code (internal) |
|----------|-----------|-------------------|----------------------|
| RAM `mmap` allocation failure | `mmap()` returns `MAP_FAILED` | Bypass RAM cache; read directly from SSD or data source. Log WARN. Disable RAM cache if failures persist (3 consecutive). | `CACHE_MISS` (1501) |
| SSD I/O error (read) | `pread()` returns error / checksum mismatch | Evict corrupted entry; fall back to data source read. Log ERROR. | `CACHE_INVALIDATION_FAILED` (1502) |
| SSD I/O error (write) | `pwrite()` returns error / disk full | Skip SSD write; data remains in RAM only. Log WARN. | — (silent degradation) |
| SSD checksum mismatch | Computed checksum ≠ stored checksum | Evict corrupted entry; reload from data source. Increment `cache.corruption_detected` metric. | `CACHE_INVALIDATION_FAILED` (1502) |
| Deserialization failure | `CachedDataSourceReader.deserialize()` throws | Evict corrupted entry; delegate to `rawReader.read()`. Log ERROR. | `CACHE_MISS` (1501) |
| Prefetch failure (network/storage) | `PrefetchManager` future completes exceptionally | Log WARN; skip prefetch for this key. Do not retry — next access triggers normal cache-miss path. | — (silent) |
| `CacheEvictionNotifier` broadcast failure | HTTP POST to Worker returns error / timeout | Retry once after 5s. If still fails, log WARN. Stale entries will be evicted naturally via LRU. | — (best-effort) |

### 9.2 Error Handling Pattern

All cache read paths follow the same fallback pattern:

```cpp
// Pseudocode — applied in CacheManager::read() and CachedDataSourceReader::readNext()
try {
    auto buffer = cacheManager->read(key, loader).get();
    return deserialize(buffer);
} catch (const CacheException& e) {
    // Cache failure is non-fatal: fall back to direct data source read
    metrics->increment("cache.fallback_reads");
    log::warn("Cache read failed for key={}, falling back to source: {}", key, e.what());
    return loader().get();  // Direct read from storage
}
```

> **Design Principle**: The cache layer is a **performance optimization**, not a correctness requirement. Any cache failure must degrade to a slower-but-correct path. Error codes `CACHE_MISS` (1501) and `CACHE_INVALIDATION_FAILED` (1502) from [08 §2.2](./resilience.md) are used for internal metrics only and are never returned to clients.

## 10. Distributed Cache Extension (Optional)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Distributed Cache Extension                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Scenario: multiple Workers share a cache, avoiding redundant loading           │
│                                                                                  │
│  Architecture:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Worker-1              Worker-2              Worker-3                   │   │
│  │  ┌─────────┐          ┌─────────┐          ┌─────────┐                 │   │
│  │  │Local    │          │Local    │          │Local    │                 │   │
│  │  │Cache    │          │Cache    │          │Cache    │                 │   │
│  │  │(RAM+SSD)│          │(RAM+SSD)│          │(RAM+SSD)│                 │   │
│  │  └────┬────┘          └────┬────┘          └────┬────┘                 │   │
│  │       │                    │                    │                       │   │
│  │       └────────────────────┼────────────────────┘                       │   │
│  │                            │                                            │   │
│  │                    ┌───────▼───────┐                                   │   │
│  │                    │ Cache Router  │                                   │   │
│  │                    │ (consistent hash) │                                 │   │
│  │                    └───────┬───────┘                                   │   │
│  │                            │                                            │   │
│  │       ┌────────────────────┼────────────────────┐                      │   │
│  │       ▼                    ▼                    ▼                      │   │
│  │  ┌─────────┐          ┌─────────┐          ┌─────────┐                │   │
│  │  │Remote   │          │Remote   │          │Remote   │                │   │
│  │  │Cache-1  │          │Cache-2  │          │Cache-3  │                │   │
│  │  │(Redis)  │          │(Redis)  │          │(Redis)  │                │   │
│  │  └─────────┘          └─────────┘          └─────────┘                │   │
│  │                                                                        │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Implementation approach:                                                        │
│  • Implement the Cache SPI interface                                            │
│  • Use Redis/Memcached as the backend                                           │
│  • Consistent-hash routing, supporting dynamic node scaling                     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```cpp
// RemoteCache.h - distributed cache implementation (optional extension)
#pragma once

#include "pangrova/cache/CacheSpi.h"

namespace pangrova {
namespace cache {

/**
 * Distributed cache configuration
 */
struct RemoteCacheConfig {
    std::vector<std::string> endpoints;  // Redis node list
    int connectionPoolSize = 10;
    int timeoutMs = 100;
    bool enableCompression = true;
};

/**
 * Distributed cache implementation
 * Based on Redis, supports cross-Worker sharing
 */
class RemoteCache : public Cache {
public:
    explicit RemoteCache(const RemoteCacheConfig& config);
    ~RemoteCache() override;

    folly::Future<std::shared_ptr<arrow::Buffer>> get(const CacheKey& key) override;
    folly::Future<bool> put(const CacheKey& key, std::shared_ptr<arrow::Buffer> buffer) override;
    folly::Future<bool> remove(const CacheKey& key) override;
    bool contains(const CacheKey& key) override;
    void pin(const CacheKey& key) override;
    void unpin(const CacheKey& key) override;
    CacheStats getStats() const override;
    void clear() override;
    std::string name() const override { return "RemoteCache"; }

private:
    int getShardIndex(const CacheKey& key) const;  // consistent hash

    RemoteCacheConfig config_;
    // Redis client pool
};

REGISTER_CACHE_FACTORY(RemoteCacheFactory);

} // namespace cache
} // namespace pangrova
```

## 11. Cache Monitoring and Tuning

```yaml
# cache-config.yaml - cache configuration example

cache:
  ram:
    enabled: true
    max-size: 8GB
    eviction-policy: LRU
    eviction-threshold: 0.9

  ssd:
    enabled: true
    directory: /data/cache
    max-size: 100GB
    num-shards: 16
    use-direct-io: true
    checksum-enabled: true

  prefetch:
    enabled: true
    threads: 4
    window-size: 8
    max-bytes: 256MB
    coalesce-threshold: 64KB
    adaptive: true

  metrics:
    enabled: true
    report-interval: 10s
    histogram-buckets: [1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s]
```

```java
// CacheMetrics.java - Cache monitoring metrics
package org.pangrova.cache;

/**
 * Cache monitoring metrics
 * Exposed to Prometheus/OpenTelemetry
 */
public class CacheMetrics {

    // RAM Cache metrics
    private final Counter ramHits;
    private final Counter ramMisses;
    private final Gauge ramSize;
    private final Histogram ramLatency;

    // SSD Cache metrics
    private final Counter ssdHits;
    private final Counter ssdMisses;
    private final Gauge ssdSize;
    private final Histogram ssdLatency;

    // Prefetch metrics
    private final Counter prefetchRequests;
    private final Counter prefetchHits;      // Prefetched data was actually used
    private final Counter prefetchWasted;    // Prefetched data evicted before being used

    // Overall metrics
    private final Gauge overallHitRate;
    private final Counter evictions;
    private final Counter promotions;        // SSD → RAM
    private final Counter demotions;         // RAM → SSD

    public void recordRamHit(long latencyNs) {
        ramHits.inc();
        ramLatency.observe(latencyNs / 1_000_000.0);  // Convert to ms
    }

    public void recordRamMiss() {
        ramMisses.inc();
    }

    // ... other methods
}
```

## 12. Cache Best Practices

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Cache Best Practices                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  1. Capacity planning:                                                           │
│     • RAM Cache: 10-20% of Worker memory (rest for compute)                      │
│     • SSD Cache: 1-2x the hot dataset size                                       │
│     • Monitor hit ratio; consider scaling up below 80%                           │
│                                                                                  │
│  2. Eviction strategy selection:                                                 │
│     • Interactive queries: LRU (strong temporal locality)                        │
│     • ETL batch: FIFO (mostly sequential scans)                                  │
│     • Mixed workloads: Adaptive (auto switching)                                 │
│                                                                                  │
│  3. Prefetch tuning:                                                             │
│     • Sequential scan: enlarge prefetch window (8-16 Row Groups)                 │
│     • Random access: shrink prefetch window to avoid waste                       │
│     • Monitor prefetch hit ratio; adjust strategy below 70%                      │
│                                                                                  │
│  4. SSD configuration:                                                           │
│     • Use NVMe SSD, avoid SATA SSD                                               │
│     • Enable Direct I/O to avoid double caching                                  │
│     • numShards = CPU core count to reduce lock contention                       │
│                                                                                  │
│  5. Monitoring & alerting:                                                       │
│     • Hit ratio < 70%: alert, check cache size                                   │
│     • Eviction rate > 10%/s: alert, cache thrashing                              │
│     • SSD latency > 10ms: alert, check disk health                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 13. Extension Points Summary

| Extension Point | Interface | Built-in Implementations | Extension Scenarios |
|--------|------|----------|----------|
| Cache backend | `Cache` | RamCache, SsdCache | Redis, Memcached, Alluxio |
| Eviction policy | `EvictionPolicy` | LRU, LFU, FIFO | Custom policies |
| Prefetch strategy | `PrefetchStrategy` | Sequential, Adaptive | ML-based prediction |
| Cache key | `CacheKey` | file+offset | Column-level cache, compressed-block cache |
| Serialization | `CacheSerializer` | Arrow IPC | Custom compression formats |

---



## 14. Observability Integration

This section defines the metrics and tracing integration points for the cache layer. For the full observability design, see [observability.md](./observability.md).

### 14.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.cache.hit_count` | CacheMetrics | Cache hits (by cache tier: RAM/SSD/Remote) |
| `pangrova.cache.miss_count` | CacheMetrics | Cache misses |
| `pangrova.cache.hit_rate` | CacheMetrics | Cache hit ratio |
| `pangrova.cache.eviction_count` | CacheMetrics | Eviction count |
| `pangrova.cache.size_bytes` | CacheMetrics | Current cache size in bytes |
| `pangrova.cache.load_latency` | CacheMetrics | Cache load latency |
| `pangrova.cache.prefetch.hit_rate` | PrefetchManager | Prefetch effectiveness |

### 14.2 Tracing Spans

Cache operations are children of `native.execute` or `task.execute` spans:

- `cache.lookup` — Cache lookup operation (INTERNAL)
- `cache.load` — Cache miss → load from storage (INTERNAL)
- `cache.prefetch` — Prefetch operation (INTERNAL)

### 14.3 Log Events

- Cache tier promotion/demotion events (DEBUG)
- Cache eviction events with eviction reason (DEBUG)
- Prefetch trigger events (DEBUG)
- Cache corruption detection events (ERROR)

See [14 §7](./observability.md) for structured logging format.


## 15. Plan Cache (Coordinator-Side)

Implementation status: the following compiled-plan cache is a design, not a
production capability. The unconnected Java RelNode cache and its unregistered
metadata listener were removed: copying a root node does not detach its cluster
or children. Current queries plan in a query-owned cluster. A future plan cache
must store immutable canonical plans, bind exact metadata/security/model versions,
and prove invalidation and concurrent isolation before wiring. The separately
wired query-result cache is not removed by this change.



While §1–§13 cover the **data cache** (Worker-side, caching file data blocks), this section describes the **plan cache** — a Coordinator-side optimization that caches compiled Substrait query plans to avoid repeated optimization of identical SQL patterns.

### 15.1 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Coordinator                               │
│                                                               │
│  SQL ──▶ Normalizer ──▶ CacheKey ──▶ L1 Lookup (JVM)        │
│                                         │                     │
│                              hit ◄──────┤                     │
│                                         │ miss                │
│                                         ▼                     │
│                                    L2 Lookup (Redis)          │
│                              hit ◄──────┤                     │
│                                         │ miss                │
│                                         ▼                     │
│                                    Full Optimization          │
│                                    (Calcite Pipeline)         │
│                                         │                     │
│                                         ▼                     │
│                                    Cache Put (L1 + L2)        │
└──────────────────────────────────────────────────────────────┘
```

**Design Rationale**: BI dashboards and reporting tools execute the same parameterized SQL patterns thousands of times per hour. Each execution pays the full 50–200ms Calcite optimization cost (ANTLR4 parsing → Calcite validation → HepPlanner → VolcanoPlanner → Substrait conversion). For queries completing in < 500ms, optimization overhead is 10–40% of total latency.

**Two-Level Cache**:

| Level | Backend | Capacity | Scope | Latency |
|-------|---------|----------|-------|---------|
| L1 | JVM `ConcurrentHashMap` | 5,000 entries (~50MB) | Single Coordinator | ~0.01ms |
| L2 | Redis (shared with Query Result Cache) | 50,000 entries, TTL 1h | Cross-Coordinator (Multi-Active) | ~1ms |

### 15.2 Pseudocode

**Cache Key Design**:

```pseudocode
class PlanCacheKey:
    function compute(sql: String, context: QueryContext):
        // Step 1: Normalize SQL — replace literals with placeholders
        normalizedSQL = SQLNormalizer.normalize(sql)
        // "SELECT * FROM t WHERE id = 42" → "SELECT * FROM t WHERE id = ?"

        // Step 2: Include schema version to invalidate on DDL
        schemaVersion = context.catalog.getSchemaVersion(extractTables(sql))

        // Step 3: Include statistics version to invalidate on stats change
        statsVersion = context.catalog.getStatsVersion(extractTables(sql))

        // Step 4: Include security context (tenant, roles) for RLS/column mask
        securityFingerprint = hash(context.tenantId, context.roles, context.rlsPolicies)

        return hash(normalizedSQL, schemaVersion, statsVersion, securityFingerprint)
```

**Cache Lookup and Population**:

```pseudocode
class PlanCacheManager:
    l1Cache: ConcurrentHashMap<PlanCacheKey, PlanCacheEntry>  // max 5000 entries
    l2Cache: RedisClient                                       // max 50000 entries, TTL 1h

    function get(key: PlanCacheKey):
        entry = l1Cache.get(key)
        if entry != null:
            entry.hitCount.increment()
            return entry
        entry = l2Cache.get(key)
        if entry != null:
            l1Cache.put(key, entry)  // promote to L1
            return entry
        return null  // cache miss — run full optimization

    function put(key: PlanCacheKey, plan: SubstraitPlan, params: List<Slot>):
        entry = PlanCacheEntry(plan, params, now(), 0,
            key.schemaVersion, key.statsVersion)
        l1Cache.put(key, entry)
        l2Cache.setex(key, entry, TTL = 3600)

    function invalidateForTable(tableName: String):
        // Called on DDL (ALTER TABLE, DROP TABLE) or ANALYZE TABLE
        keysToRemove = l1Cache.keys().filter(k => k.referencesTable(tableName))
        keysToRemove.forEach(k => l1Cache.remove(k))
        l2Cache.deletePattern("plan:" + tableName + ":*")
```

**Plan Reuse with Parameter Binding**:

```pseudocode
function executeWithPlanCache(sql: String, context: QueryContext):
    key = PlanCacheKey.compute(sql, context)
    cached = planCacheManager.get(key)

    if cached != null:
        plan = cached.substraitPlan.clone()
        literals = SQLNormalizer.extractLiterals(sql)
        for (slot, literal) in zip(cached.parameterSlots, literals):
            plan.bindLiteral(slot, literal)
        return plan  // skip entire Calcite pipeline

    plan = calciteOptimize(sql, context)
    params = extractParameterSlots(sql, plan)
    planCacheManager.put(key, plan, params)
    return plan
```

### 15.3 Trade-offs

| Aspect | Without Plan Cache | With Plan Cache |
|--------|-------------------|-----------------|
| Latency | 50–200ms optimization per query | ~0.1ms cache lookup for repeated patterns (100–2000x speedup) |
| Complexity | Stateless optimization pipeline | Cache key normalization, DDL/stats invalidation, parameter binding |
| Risk | None | Low — cache miss falls back to full optimization; stale plan mitigated by schema+stats versioning |
| Memory | None | L1: ~50MB JVM heap; L2: Redis (shared with existing result cache) |
| Correctness | Always fresh plan | Schema+stats version in key ensures invalidation on structural or statistical changes |

### 15.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | SQL Normalizer | Implement literal-to-placeholder normalization. Log normalized SQL and hit-rate potential (shadow mode). |
| 2 | L1 In-Memory Cache | JVM ConcurrentHashMap cache with schema+stats version key. Invalidation on DDL events. |
| 3 | L2 Redis Cache | Extend to Redis for cross-Coordinator sharing. TTL-based expiry. |
| 4 | Parameter Binding | Substrait plan parameter slot binding for literal substitution without re-optimization. |

> **Cross-reference**: Plan Cache is a Coordinator-side optimization. It complements the Worker-side data cache (§1–§13) and the Query Result Cache ([query-execution.md §14](./query-execution.md)). SQL normalization leverages the ANTLR4 parser defined in [query-optimization.md §2](./query-optimization.md). Invalidation on DDL events integrates with the FederatedCatalog SPI ([metadata.md §3.8](./metadata.md)).


> **Java CacheSpi Interface**: The Java-side cache SPI bridges the JVM layer with the Native C++ cache. The authoritative Java interface is defined in §2 (added by SPI-C05 fix). The C++ `CacheSpi.h` interface handles actual data caching; the Java `CacheSpi` interface handles cache metadata operations (lookup, invalidation, TTL management) and delegates data operations to the Native layer via JNI.


> **CacheKey Construction**: `CacheKey.deleteFileSetHash` is computed from the `delete_files` repeated field in [03 §5.3 IcebergSplitInfo](./native-execution.md). The hash input is: `sorted(delete_files.map(f -> f.path + ":" + f.record_count)).join(",").sha256()`. This ensures cache invalidation when delete file sets change (MOR table compaction).


> **Extension Point Interfaces (§12)**:
> - `EvictionPolicy`: `boolean shouldEvict(CacheEntry entry, CacheStats stats)` — determines if an entry should be evicted. Built-in implementations: LRU, LFU, ARC.
> - `PrefetchStrategy`: `List<CacheKey> predict(QueryPlan plan, CacheStats stats)` — predicts which data blocks to prefetch. Built-in implementations: ScanAhead, FrequencyBased.


---



## 16. Prefetch Algorithm Specification (OPT-018)

This section provides concrete algorithm specifications for the adaptive prefetch system described in §6, addressing underspecified areas: sequential ratio computation, SSD-first prefetch policy, prefetch cancellation, and per-operator prefetch context.

### 16.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Concrete Prefetch Algorithm Stack                    │
│                                                                  │
│  OperatorPrefetchContext                                         │
│    ├─ TableScan → SequentialPrefetch(window=4)                   │
│    ├─ HashJoinProbe → ColumnBasedPrefetch(probeKeys)             │
│    └─ Aggregation → QueryAwarePrefetch(inputPlan)                │
│              │                                                   │
│              ▼                                                   │
│  CancellablePrefetchManager                                      │
│    ├─ submitPrefetch(taskId, requests)                            │
│    └─ cancelPrefetch(taskId) ← query abort/completion            │
│              │                                                   │
│              ▼                                                   │
│  SSDFirstPrefetchPolicy                                          │
│    └─ prefetch → SSD (not RAM) → promoted on actual read         │
│                                                                  │
│  SequentialRatioTracker (sliding window, N=100)                  │
│    └─ feeds AdaptivePrefetchController (§6.1)                    │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: The adaptive prefetch controller (§6.1) lacks concrete implementation details for `sequentialRatio` computation, prefetch-to-SSD interaction, and cancellation. Prefetched data written to RAM cache may evict hot entries.

### 16.2 Pseudocode

**Sequential Ratio Computation**:

```pseudocode
class SequentialRatioTracker:
    windowSize = 100
    recentReads: CircularBuffer<ReadRequest>

    function recordRead(req: ReadRequest):
        recentReads.append(req)

    function computeSequentialRatio():
        if recentReads.size < 2: return 0.0
        sequentialCount = 0
        for i in range(1, recentReads.size):
            prev = recentReads[i - 1]
            curr = recentReads[i]
            if curr.fileId == prev.fileId
               AND curr.offset == prev.offset + prev.length:
                sequentialCount++
        return sequentialCount / (recentReads.size - 1)
```

**SSD-First Prefetch Policy**:

```pseudocode
class SSDFirstPrefetchPolicy:
    function prefetch(requests: List<PrefetchRequest>):
        for req in requests:
            if ramCache.contains(req.key): continue
            if ssdCache.contains(req.key): continue
            data = remoteStorage.read(req.fileId, req.offset, req.length)
            ssdCache.put(req.key, data)
            // Promoted to RAM on actual read (existing promotion logic)
```

**Prefetch Cancellation**:

```pseudocode
class CancellablePrefetchManager:
    activePrefetches: Map<TaskId, List<Future<void>>>

    function submitPrefetch(taskId: TaskId, requests: List<PrefetchRequest>):
        futures = requests.map(req => asyncExecutor.submit(
            () => ssdFirstPolicy.prefetch(req)))
        activePrefetches.put(taskId, futures)

    function cancelPrefetch(taskId: TaskId):
        futures = activePrefetches.remove(taskId)
        if futures != null:
            futures.forEach(f => f.cancel())
```

**Per-Operator Prefetch Context**:

```pseudocode
class OperatorPrefetchContext:
    function selectStrategy(operator: Operator):
        match operator:
            case TableScan:      return SequentialPrefetch(windowSize = 4)
            case HashJoinProbe:  return ColumnBasedPrefetch(columns = probeKeys)
            case Aggregation:    return QueryAwarePrefetch(plan = operator.inputPlan)
            default:             return NoPrefetch()
```

### 16.3 Trade-offs

| Aspect | Current (Underspecified) | Concrete Specification |
|--------|------------------------|----------------------|
| Specificity | High-level description; implementation ambiguous | Concrete algorithms with clear parameters |
| RAM cache quality | Prefetch may pollute RAM cache | SSD-first preserves RAM for hot data |
| Resource waste | No cancellation; prefetch continues after query completion | Cancellation stops unnecessary I/O on abort |
| Cross-Worker | Independent prefetch; potential redundancy | Mitigated by OPT-008 data locality (consistent hashing) |

### 16.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Sequential Ratio Tracker | Sliding-window sequential ratio computation. Gate sequential prefetch. |
| 2 | SSD-First Prefetch | Change prefetch target from RAM to SSD. Measure RAM cache hit rate impact. |
| 3 | Cancellation | `CancellablePrefetchManager` hooked into query abort/completion lifecycle. |
| 4 | Per-Operator Context | Assign prefetch strategies based on operator type. |

> **Cross-reference**: This section refines the adaptive prefetch design in §6.1. SSD-first policy integrates with SSD Cache (§4). Cancellation hooks into the query lifecycle managed by [query-execution.md](./query-execution.md). Data locality (OPT-008 in [01 §9](./distributed.md)) reduces cross-Worker prefetch redundancy via consistent hashing.
