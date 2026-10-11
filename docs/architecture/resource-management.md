---
title: "Resource Management"
sidebar_label: "Resource Management"
---

# Pangrova — Resource Management

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** Unified memory arbitration across the JVM and native layers, spill, and resource queues.

## 1. Resource Management Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Unified Resource Management Architecture                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • Unified control: the Java layer manages JVM Heap + Off-Heap; Native memory has an upper limit set by Java + independently managed by Native│
│  • Graceful degradation: automatically trigger Spill under memory pressure to avoid OOM│
│  • Fair scheduling: fair resource allocation across queries, supporting priority and preemption│
│  • Observability: monitor resource usage in real time, supporting alerts and auto-scaling│
│                                                                                  │
│  Resource types:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Resource Type    │ Layer   │ Limit method    │ Spill handling         │   │
│  │  ────────────────────────────────────────────────────────────────────── │   │
│  │  JVM Heap         │ Java    │ -Xmx            │ GC → OOM               │   │
│  │  JVM Off-Heap     │ Java    │ -XX:MaxDirect   │ Spill → OOM            │   │
│  │  Native Memory    │ Native  │ MemoryPool      │ Spill → reject allocation│   │
│  │  CPU (Threads)    │ Both    │ ThreadPool      │ Queue                  │   │
│  │  Disk (Spill)     │ Native  │ Quota           │ Clean up → reject      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 2. Memory Management Hierarchy



```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Memory Hierarchy Architecture                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Worker Total Memory: 64GB                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  ┌────────────────────────────────────────────────────────────────────┐ │   │
│  │  │                    Root Memory Arbitrator (Java)                       │ │   │
│  │  │                    Total Budget: 60GB                               │ │   │
│  │  │                                                                     │ │   │
│  │  │  Responsibilities:                                                  │ │   │
│  │  │  • Global memory budget management                                  │ │   │
│  │  │  • Cross-query memory arbitration                                   │ │   │
│  │  │  • Spill trigger decisions                                          │ │   │
│  │  │  • Native memory quota allocation                                   │ │   │
│  │  └────────────────────────────────────────────────────────────────────┘ │   │
│  │                              │                                          │   │
│  │         ┌────────────────────┼────────────────────┐                    │   │
│  │         ▼                    ▼                    ▼                    │   │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │   │
│  │  │ Query Pool  │     │ Query Pool  │     │ System Pool │              │   │
│  │  │  Query-1    │     │  Query-2    │     │  Reserved   │              │   │
│  │  │   25GB      │     │   25GB      │     │    10GB     │              │   │
│  │  └──────┬──────┘     └──────┬──────┘     └─────────────┘              │   │
│  │         │                   │                                          │   │
│  │    ┌────┴────┐         ┌────┴────┐                                    │   │
│  │    ▼         ▼         ▼         ▼                                    │   │
│  │ ┌──────┐ ┌──────┐   ┌──────┐ ┌──────┐                                │   │
│  │ │Task-1│ │Task-2│   │Task-1│ │Task-2│   (Task Pools)                 │   │
│  │ │ 10GB │ │ 10GB │   │ 10GB │ │ 10GB │                                │   │
│  │ └──┬───┘ └──────┘   └──────┘ └──────┘                                │   │
│  │    │                                                                   │   │
│  │    │  ┌─────────────────────────────────────────────────────────┐    │   │
│  │    └─►│              Native Memory Pool (C++)                    │    │   │
│  │       │  • Velox MemoryPool (compute buffers)                   │    │   │
│  │       │  • Arrow MemoryPool (data buffers)                      │    │   │
│  │       │  • Spill Buffer (spill buffers)                         │    │   │
│  │       └─────────────────────────────────────────────────────────┘    │   │
│  │                                                                       │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Reserved (4GB): JVM Heap, Metadata Cache, System Overhead                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```


## 3. Java-Layer Resource Manager

```java
// ResourceManager.java - Unified resource manager
package org.pangrova.worker.resource;

/**
 * Unified resource manager
 * Manages all resources on a Worker node: memory, CPU, disk
 */
public class ResourceManager implements Lifecycle {

    private final ResourceConfig config;
    private final MemoryArbitrator memoryArbitrator;
    private final ThreadPoolManager threadPoolManager;
    private final SpillManager spillManager;
    private final NativeRuntime nativeRuntime;

    // Memory pressure levels
    //
    public enum MemoryPressure {
        NONE(0.0, 0.5),      // < 50%: normal
        LOW(0.5, 0.7),       // 50-70%: begin monitoring
        MEDIUM(0.7, 0.85),   // 70-85%: trigger Spill
        HIGH(0.85, 0.95),    // 85-95%: aggressive Spill
        CRITICAL(0.95, 1.0); // > 95%: reject new tasks

        final double low, high;
        MemoryPressure(double low, double high) {
            this.low = low; this.high = high;
        }
    }

    /**
     * Allocate a resource quota for a query
     */
    public QueryResourceContext allocateForQuery(QueryId queryId, ResourceRequirement requirement) {
        // 1. Check whether global resources are sufficient
        if (memoryArbitrator.getPressure() == MemoryPressure.CRITICAL) {
            throw new ResourceExhaustedException("Memory pressure critical, rejecting new query");
        }

        //
        // HIGH pressure: only allow tasks with estimated memory < high-pressure-max-task-memory (default 1GB)
        // This aligns with 01 §5.2.1 scheduling rule: "HIGH → only schedule small-memory tasks (<1GB)"
        if (memoryArbitrator.getPressure() == MemoryPressure.HIGH
                && requirement.getEstimatedMemory() > config.getHighPressureMaxTaskMemory()) {
            throw new ResourceExhaustedException(
                "Memory pressure HIGH, rejecting large query (estimated "
                + requirement.getEstimatedMemory() + " > limit "
                + config.getHighPressureMaxTaskMemory() + ")");
        }

        // 2. Compute the allocatable memory quota
        long memoryQuota = memoryArbitrator.calculateQuota(requirement.getEstimatedMemory());

        // 3. Allocate the thread quota
        int threadQuota = threadPoolManager.allocateThreads(requirement.getParallelism());

        // 4. Create the Native memory pool
        long nativePoolHandle = nativeBridge.createMemoryPool(
            queryId.toString(), memoryQuota);

        return new QueryResourceContext(queryId, memoryQuota, threadQuota, nativePoolHandle);
    }

    /**
     * Release query resources
     */
    public void releaseForQuery(QueryResourceContext context) {
        memoryArbitrator.release(context.getMemoryQuota());
        threadPoolManager.releaseThreads(context.getThreadQuota());
        nativeBridge.destroyMemoryPool(context.getNativePoolHandle());
    }

    /**
     * Handle memory pressure (invoked by the monitoring thread)
     */
    public void handleMemoryPressure() {
        MemoryPressure pressure = memoryArbitrator.getPressure();


        switch (pressure) {
            case NONE:
            case LOW:
                // No action needed — normal operating range
                break;
            case MEDIUM:
                // Trigger Spill for low-priority tasks
                spillManager.spillLowPriorityTasks();
                break;
            case HIGH:
                // Aggressive Spill: all spillable tasks
                spillManager.spillAllSpillable();
                break;
            case CRITICAL:
                // Emergency measure: cancel low-priority queries
                cancelLowPriorityQueries();
                break;
            default:
                break;
        }
    }
}
```

## 4. Memory Arbitrator

```java
// MemoryArbitrator.java - Memory arbitrator
package org.pangrova.worker.resource;

/**
 * Memory arbitrator (naming aligned with Velox MemoryArbitrator)
 * Responsible for cross-query memory allocation and reclamation decisions
 */
public class MemoryArbitrator {

    private final long totalBudget;
    private final AtomicLong usedMemory = new AtomicLong(0);
    private final Map<QueryId, QueryMemoryContext> queryContexts = new ConcurrentHashMap<>();
    private final PriorityQueue<SpillCandidate> spillCandidates = new PriorityQueue<>();

    /**
     * Request a memory allocation
     * @return the number of bytes actually allocated (may be less than requested)
     */
    public long requestMemory(QueryId queryId, long bytes) {
        QueryMemoryContext context = queryContexts.get(queryId);

        // 1. Check the query quota
        if (context.getUsed() + bytes > context.getQuota()) {
            // Quota exceeded; try to borrow from other queries or trigger Spill
            bytes = handleOverQuota(queryId, bytes);
        }

        // 2. Check global memory
        long available = totalBudget - usedMemory.get();
        if (bytes > available) {
            // Insufficient global memory; trigger Spill
            long spilled = triggerSpill(bytes - available);
            available += spilled;
        }

        // 3. Allocate memory
        long allocated = Math.min(bytes, available);
        usedMemory.addAndGet(allocated);
        context.addUsed(allocated);

        return allocated;
    }

    /**
     * Trigger Spill to release memory
     */
    private long triggerSpill(long targetBytes) {
        long spilled = 0;

        while (spilled < targetBytes && !spillCandidates.isEmpty()) {
            SpillCandidate candidate = spillCandidates.poll();
            long freed = candidate.spill();
            spilled += freed;

            // Update statistics
            metrics.recordSpill(candidate.getQueryId(), freed);
        }

        return spilled;
    }

    /**
     * Register a spillable component
     */
    public void registerSpillable(QueryId queryId, Spillable spillable) {
        spillCandidates.add(new SpillCandidate(queryId, spillable));
    }
}
```


## 5. Native Memory Pool Integration

```cpp
// NativeMemoryPool.h - Native memory pool
#pragma once

#include <velox/common/memory/MemoryPool.h>
#include <arrow/memory_pool.h>
#include <atomic>
#include <functional>

namespace pangrova {
namespace memory {

/**
 * Native memory pool
 * Integrates the Velox memory pool; the Native layer manages it independently, while the Java layer sets an upper limit + polling monitoring
 */
class NativeMemoryPool : public facebook::velox::memory::MemoryPool {
public:
    NativeMemoryPool(
        const std::string& name,
        int64_t maxBytes);

    /**
     * Allocate memory (managed independently by the Native layer, no callback to Java)
     * On exceeding the limit, the Velox MemoryArbitrator arbitrates and Spills on its own
     *
     *
     * NOTE: This is a simplified example showing the allocation boundary check.
     * Production implementation delegates to Velox MemoryArbitrator's standard
     * growCapacity()/shrinkCapacity() flow, which attempts spill-before-fail:
     *   1. Operator requests memory via MemoryPool::allocate()
     *   2. MemoryArbitrator tries to grow pool capacity from other pools
     *   3. If insufficient, triggers spill on spillable operators
     *   4. Retries allocation after spill
     *   5. Only throws VeloxRuntimeError if spill cannot free enough memory
     */
    void* allocate(int64_t size) override {
        if (usedBytes_.load() + size > maxBytes_) {
            // Trigger Velox MemoryArbitrator arbitration/Spill
            throw facebook::velox::VeloxRuntimeError(
                "Memory allocation exceeds pool limit");
        }

        void* ptr = std::malloc(size);
        if (!ptr) {
            throw std::bad_alloc();
        }

        usedBytes_.fetch_add(size);
        return ptr;
    }

    void free(void* ptr, int64_t size) override {
        std::free(ptr);
        usedBytes_.fetch_sub(size);
    }

    int64_t currentBytes() const override {
        return usedBytes_.load();
    }

    int64_t peakBytes() const override {
        return peakBytes_.load();
    }

private:
    std::string name_;
    int64_t maxBytes_;
    std::atomic<int64_t> usedBytes_{0};
    std::atomic<int64_t> peakBytes_{0};
};

/**
 * Arrow memory pool adapter
 * Adapts NativeMemoryPool to the Arrow MemoryPool interface
 */
class ArrowMemoryPoolAdapter : public arrow::MemoryPool {
public:
    explicit ArrowMemoryPoolAdapter(std::shared_ptr<NativeMemoryPool> pool)
        : pool_(std::move(pool)) {}

    arrow::Status Allocate(int64_t size, uint8_t** out) override {
        try {
            *out = static_cast<uint8_t*>(pool_->allocate(size));
            return arrow::Status::OK();
        } catch (const std::exception& e) {
            return arrow::Status::OutOfMemory(e.what());
        }
    }

    void Free(uint8_t* buffer, int64_t size) override {
        pool_->free(buffer, size);
    }

    int64_t bytes_allocated() const override {
        return pool_->currentBytes();
    }

private:
    std::shared_ptr<NativeMemoryPool> pool_;
};

} // namespace memory
} // namespace pangrova
```

## 6. Spill Manager

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Spill Manager Architecture                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Spill trigger conditions:                                                       │
│  1. Memory pressure reaches MEDIUM (70%) → begin Spilling low-priority data     │
│  2. Memory pressure reaches HIGH (85%) → aggressively Spill all spillable data  │
│  3. A single operator exceeds its memory limit → operator-level Spill           │
│                                                                                  │
│  Spillable components:                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Component          │ Spill priority│ Spill strategy                     │   │
│  │  ────────────────────────────────────────────────────────────────────── │   │
│  │  Hash Join Build    │ High         │ Spill by partition, keep hot partitions│   │
│  │  Aggregation        │ High         │ Spill by Group partition           │   │
│  │  Sort               │ Medium       │ External sort, multi-way merge     │   │
│  │  Shuffle Buffer     │ Medium       │ Spill by target partition          │   │
│  │  Scan Buffer        │ Low          │ Reduce read-ahead, read on demand  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Spill storage hierarchy:                                                        │
│                 │
│  1. NVMe SSD (local) → fastest, preferred                                       │
│  2. HDD (local) → fallback                                                      │
│  3. S3/HDFS (remote) → cross-Worker fault-tolerance scenarios                   │
│                                                                                  │
│  Current scope: single-tier local spill (SpillConfig.spillDir).                  │
│  Multi-tier storage is a future enhancement. When implemented,                   │
│  SpillConfig.spillDir will be extended to a list of paths with tier              │
│  annotations. Tier selection: try paths in order; if current tier's              │
│  free space < spillFileTargetSize, fall through to next tier.                    │
│  S3/HDFS remote spill triggers only for cross-Worker fault tolerance             │
│  (stage-level retry with OPT-012).                                               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.1 Spill Trigger Decision Algorithm

Spill triggering is performed cooperatively by the Java-layer `MemoryArbitrator` (§4) and the Native-layer Velox `MemoryArbitrator`. This section defines the complete decision algorithm.

**Decision flow**:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Spill Trigger Decision Flow                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Trigger source 1: Java MemoryArbitrator global memory pressure                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  MemoryPressure = MEDIUM (70-85%):                                       │   │
│  │    1. Collect the SpillCandidate list for all Tasks                      │   │
│  │    2. Sort by (spillPriority ASC, reclaimableBytes DESC)                 │   │
│  │    3. Starting from low-priority Tasks, trigger Spill one by one         │   │
│  │    4. After each Spill, check whether pressure drops to LOW; if so, stop │   │
│  │                                                                          │   │
│  │  MemoryPressure = HIGH (85-95%):                                         │   │
│  │    1. Trigger Spill in parallel for all spillable Tasks                  │   │
│  │    2. Do not distinguish priority; Spill everything                      │   │
│  │    3. Still HIGH after Spill → cancel the lowest-priority query (BACKGROUND queue first)│   │
│  │                                                                          │   │
│  │  MemoryPressure = CRITICAL (>95%):                                       │   │
│  │    1. Reject all new queries                                             │   │
│  │    2. Cancel all BACKGROUND queries                                      │   │
│  │    3. Cancel the BATCH query with the largest memory footprint           │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Trigger source 2: Native Velox MemoryArbitrator operator-level memory overrun   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Operator requests memory → exceeds Task Pool limit:                     │   │
│  │    1. The Velox MemoryArbitrator tries to reclaim from other Operators in the same Task│   │
│  │    2. Insufficient reclamation → select a Spillable Operator (by reclaimableBytes DESC)│   │
│  │    3. Trigger Operator.spill(targetBytes)                                │   │
│  │    4. Retry allocation after Spill                                       │   │
│  │    5. Still insufficient → throw VeloxRuntimeError → Task FAILED        │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**SpillCandidate selection algorithm**:

```java
/**
 * Spill candidate selection — determines which operator to spill first.
 *
 * Selection criteria (ordered by priority):
 * 1. Query queue type: BACKGROUND > BATCH > INTERACTIVE (spill low-priority first)
 * 2. Operator spill priority: Scan Buffer > Shuffle Buffer > Sort > Agg > Hash Join
 * 3. Reclaimable bytes: larger reclaimable → spill first (maximize freed memory)
 */
public class SpillCandidateSelector {

    public List<SpillCandidate> selectCandidates(long targetBytes) {
        List<SpillCandidate> candidates = collectAllSpillable();

        // Sort by composite key: (queuePriority DESC, spillPriority ASC, reclaimable DESC)
        candidates.sort(Comparator
            .comparing((SpillCandidate c) -> c.queueType().priority).reversed()
            .thenComparing(SpillCandidate::spillPriority)
            .thenComparing(SpillCandidate::reclaimableBytes, Comparator.reverseOrder()));

        // Select minimum set of candidates to reach targetBytes
        List<SpillCandidate> selected = new ArrayList<>();
        long accumulated = 0;
        for (SpillCandidate c : candidates) {
            if (accumulated >= targetBytes) break;
            selected.add(c);
            accumulated += c.reclaimableBytes();
        }
        return selected;
    }
}
```

**Operator Spill priority definition** (the smaller the value, the earlier it is Spilled):

| Spill Priority | Operator | Reason |
|---|---|---|
| 1 (Spilled first) | Scan Buffer | Reducing read-ahead is enough; re-reading has low cost |
| 2 | Shuffle Buffer | Spill by partition; restore by partition on read-back |
| 3 | Sort | External sort is mature; multi-way merge performance is acceptable |
| 4 | Aggregation | Spill by Group partition; requires rehash |
| 5 (Spilled last) | Hash Join Build | Highest Spill cost; requires rebuilding the hash table |

```cpp

```cpp
// SpillManager.h - Spill manager
#pragma once

#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>

namespace pangrova {
namespace memory {

struct SpillConfig {
    std::string spillDir = "/tmp/pangrova/spill";
    int64_t maxSpillBytes = 100L * 1024 * 1024 * 1024;  // 100GB
    arrow::Compression::type compression = arrow::Compression::LZ4_FRAME;
    int spillFileTargetSize = 64 * 1024 * 1024;  // 64MB per file
};

struct SpillFileInfo {
    std::string path;
    int64_t size;
    int64_t rowCount;
    std::shared_ptr<arrow::Schema> schema;
};

/**
 * Spill manager
 * Responsible for spilling data to disk and reading it back
 */
class SpillManager {
public:
    explicit SpillManager(const SpillConfig& config);
    ~SpillManager();

    /**
     * Spill a RecordBatch to disk
     * @param taskId   task ID (used for isolation and cleanup)
     * @param batch    the data to spill
     * @return Spill file information
     */
    SpillFileInfo spill(
        const std::string& taskId,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Spill multiple RecordBatches (merged write)
     */
    SpillFileInfo spillBatches(
        const std::string& taskId,
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

    /**
     * Read a Spill file
     * @return a RecordBatch iterator
     */
    std::shared_ptr<arrow::RecordBatchReader> readSpillFile(
        const SpillFileInfo& fileInfo);

    /**
     * Delete a Spill file
     */
    void deleteSpillFile(const SpillFileInfo& fileInfo);

    /**
     * Clean up all Spill files for a task
     */
    void cleanupTask(const std::string& taskId);

    /**
     * Get Spill statistics
     */
    int64_t getTotalSpilledBytes() const { return totalSpilledBytes_.load(); }
    int64_t getTotalSpilledFiles() const { return totalSpilledFiles_.load(); }
    int64_t getCurrentSpillDiskUsage() const;

private:
    std::string generateSpillPath(const std::string& taskId);

    SpillConfig config_;
    std::atomic<int64_t> totalSpilledBytes_{0};
    std::atomic<int64_t> totalSpilledFiles_{0};
    std::atomic<int64_t> spillFileCounter_{0};
    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<SpillFileInfo>> taskSpillFiles_;
};

} // namespace memory
} // namespace pangrova
```


## 7. Thread Pool Management

```java
// ThreadPoolManager.java - Thread pool manager
package org.pangrova.worker.resource;

/**
 * Thread pool manager
 * Manages all thread pools on a Worker
 */
public class ThreadPoolManager {

    private final ExecutorService queryExecutor;      // Query execution thread pool
    private final ExecutorService ioExecutor;         // I/O thread pool
    private final ScheduledExecutorService scheduler; // Scheduling thread pool

    private final int maxQueryThreads;
    private final AtomicInteger allocatedThreads = new AtomicInteger(0);

    public ThreadPoolManager(ThreadPoolConfig config) {
        this.maxQueryThreads = config.getMaxQueryThreads();

        // Query execution thread pool: CPU-intensive, thread count = CPU core count
        this.queryExecutor = new ThreadPoolExecutor(
            config.getCoreQueryThreads(),
            config.getMaxQueryThreads(),
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(config.getQueueCapacity()),
            new NamedThreadFactory("pangrova-query"),
            new CallerRunsPolicy()
        );

        // I/O thread pool: I/O-intensive, thread count = 2 * CPU core count
        this.ioExecutor = Executors.newFixedThreadPool(
            config.getIoThreads(),
            new NamedThreadFactory("pangrova-io")
        );

        // Scheduling thread pool
        this.scheduler = Executors.newScheduledThreadPool(
            config.getSchedulerThreads(),
            new NamedThreadFactory("pangrova-scheduler")
        );
    }

    /**
     * Allocate a thread quota for a query
     */
    public int allocateThreads(int requested) {
        int available = maxQueryThreads - allocatedThreads.get();
        int allocated = Math.min(requested, available);
        allocatedThreads.addAndGet(allocated);
        return allocated;
    }

    /**
     * Release the thread quota
     */
    public void releaseThreads(int count) {
        allocatedThreads.addAndGet(-count);
    }

    /**
     * Submit a query task
     */
    public Future<?> submitQueryTask(Runnable task) {
        return queryExecutor.submit(task);
    }

    /**
     * Submit an I/O task
     */
    public Future<?> submitIoTask(Runnable task) {
        return ioExecutor.submit(task);
    }
}
```

## 8. Resource Queue and Scheduling

> **Relationship with the multi-tenant ResourcePool ([multi-tenancy.md §3](./multi-tenancy.md))**:
>
> ResourceQueue (this section) and ResourcePool (13) are two resource management mechanisms at different levels and are easily confused. The division of responsibilities is clarified as follows:
>
> | Dimension | ResourceQueue (this section) | ResourcePool (multi-tenancy.md) |
> |---|---|---|
> | Abstraction level | Query scheduling layer — queues by query type | Tenant isolation layer — resource pools by tenant |
> | Classification dimension | Query type: Interactive / Batch / System | Tenant/business line: default / premium / batch |
> | Managed objects | Query queuing, priority, timeout | Division of Worker CPU/Memory shares |
> | Effective location | Coordinator (at query submission) | Worker (at task execution) |
> | Relationship | A query first enters the ResourceQueue to wait, and is allocated to the corresponding ResourcePool Worker resources at scheduling time |
>
> **Unified scheduling entry rule**: the full path at query submission is:
> ```
> Client → Coordinator → QuotaChecker (tenant quota) → ResourceQueue (queuing) → ResourceScheduler (scheduling)
>   → When selecting a Worker, consider the ResourcePool resource share → Worker executes
> ```

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Resource Queue Architecture                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Multi-level queue design:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  ┌─────────────────┐                                                    │   │
│  │  │ Interactive     │  Priority: highest                                 │   │
│  │  │ Queue           │  Resource quota: 30%                               │   │
│  │  │ (Ad-hoc queries)│  Timeout: 5min                                     │   │
│  │  └────────┬────────┘                                                    │   │
│  │           │                                                              │   │
│  │  ┌────────▼────────┐                                                    │   │
│  │  │ Batch           │  Priority: medium                                  │   │
│  │  │ Queue           │  Resource quota: 50%                               │   │
│  │  │ (ETL tasks)     │  Timeout: 2h                                       │   │
│  │  └────────┬────────┘                                                    │   │
│  │           │                                                              │   │
│  │  ┌────────▼────────┐                                                    │   │
│  │  │ Background      │  Priority: low                                     │   │
│  │  │ Queue           │  Resource quota: 20%                               │   │
│  │  │ (Compaction, etc.)│  Timeout: unlimited                                │   │
│  │  └─────────────────┘                                                    │   │
│  │                                                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Scheduling strategies:                                                          │
│  • Fair Scheduling: fair allocation among queues of the same priority           │
│  • Preemption: high priority can preempt low-priority resources                 │
│  • Reservation: each queue is guaranteed a minimum resource quota               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
// ResourceScheduler.java - Resource scheduler
package org.pangrova.worker.resource;

/**
 * Resource scheduler
 * Implements multi-level queues and fair scheduling
 */
public class ResourceScheduler {

    private final Map<QueueType, ResourceQueue> queues;
    private final ResourceManager resourceManager;

    public enum QueueType {
        INTERACTIVE(1, 0.3, Duration.ofMinutes(5)),
        BATCH(2, 0.5, Duration.ofHours(2)),
        BACKGROUND(3, 0.2, Duration.ofDays(1));

        final int priority;
        final double resourceShare;
        final Duration timeout;

        QueueType(int priority, double resourceShare, Duration timeout) {
            this.priority = priority;
            this.resourceShare = resourceShare;
            this.timeout = timeout;
        }
    }

    /**
     * Submit a query to a queue
     */
    public QueryHandle submit(Query query, QueueType queueType) {
        ResourceQueue queue = queues.get(queueType);

        // 1. Check queue capacity
        if (queue.isFull()) {
            throw new QueueFullException("Queue " + queueType + " is full");
        }

        // 2. Estimate resource requirements
        ResourceRequirement requirement = estimateRequirement(query);

        // 3. Enqueue and wait for scheduling
        return queue.enqueue(query, requirement);
    }

    /**
     * Scheduling loop (executed by a background thread)
     */
    public void scheduleLoop() {
        while (!shutdown) {
            // Traverse queues by priority
            for (QueueType queueType : QueueType.values()) {
                ResourceQueue queue = queues.get(queueType);

                // Check whether the queue has queries waiting to be scheduled
                Optional<PendingQuery> pending = queue.peek();
                if (pending.isEmpty()) continue;

                // Check whether resources are available
                if (canAllocate(pending.get().getRequirement(), queueType)) {
                    PendingQuery query = queue.poll();
                    allocateAndExecute(query, queueType);
                }
            }

            Thread.sleep(100);  // Scheduling interval
        }
    }

    /**
     * Preempt low-priority queries
     */
    private void preemptIfNeeded(QueueType highPriority, ResourceRequirement requirement) {
        // Only INTERACTIVE can preempt
        if (highPriority != QueueType.INTERACTIVE) return;

        // Find preemptible low-priority queries
        List<RunningQuery> victims = findPreemptionVictims(requirement);

        for (RunningQuery victim : victims) {
            // Cancel after triggering a Checkpoint
            victim.checkpoint();
            victim.cancel();

            // Release resources
            resourceManager.releaseForQuery(victim.getResourceContext());
        }
    }
}
```


## 9. Resource Monitoring and Alerting

```java
// ResourceMonitor.java - Resource monitor
package org.pangrova.worker.resource;

/**
 * Resource monitor
 * Monitors resource usage in real time, triggering alerts and automatic handling
 */
public class ResourceMonitor implements Runnable {

    private final ResourceManager resourceManager;
    private final MetricsRegistry metrics;
    private final AlertManager alertManager;

    private static final Duration MONITOR_INTERVAL = Duration.ofSeconds(1);

    @Override
    public void run() {
        while (!shutdown) {
            try {
                collectMetrics();
                checkThresholds();
                Thread.sleep(MONITOR_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void collectMetrics() {
        // Memory metrics
        metrics.gauge("worker.memory.used", resourceManager.getUsedMemory());
        metrics.gauge("worker.memory.total", resourceManager.getTotalMemory());
        metrics.gauge("worker.memory.pressure", resourceManager.getPressure().ordinal());

        // Native memory metrics
        metrics.gauge("worker.native.memory.used", resourceManager.getNativeMemoryUsed());
        metrics.gauge("worker.native.memory.peak", resourceManager.getNativeMemoryPeak());

        // Spill metrics
        metrics.gauge("worker.spill.bytes", resourceManager.getSpilledBytes());
        metrics.gauge("worker.spill.files", resourceManager.getSpilledFiles());

        // Thread metrics
        metrics.gauge("worker.threads.active", resourceManager.getActiveThreads());
        metrics.gauge("worker.threads.queued", resourceManager.getQueuedTasks());

        // Query metrics
        metrics.gauge("worker.queries.running", resourceManager.getRunningQueries());
        metrics.gauge("worker.queries.queued", resourceManager.getQueuedQueries());
    }

    private void checkThresholds() {
        MemoryPressure pressure = resourceManager.getPressure();

        // Memory pressure alert
        if (pressure == MemoryPressure.HIGH) {
            alertManager.warn("Memory pressure HIGH", Map.of(
                "used", resourceManager.getUsedMemory(),
                "total", resourceManager.getTotalMemory()
            ));
        } else if (pressure == MemoryPressure.CRITICAL) {
            alertManager.critical("Memory pressure CRITICAL", Map.of(
                "used", resourceManager.getUsedMemory(),
                "total", resourceManager.getTotalMemory()
            ));

            // Trigger emergency handling
            resourceManager.handleMemoryPressure();
        }

        // Spill disk space alert
        double spillDiskUsage = resourceManager.getSpillDiskUsageRatio();
        if (spillDiskUsage > 0.9) {
            alertManager.warn("Spill disk usage > 90%", Map.of(
                "usage", spillDiskUsage
            ));
        }
    }
}
```

## 10. Resource Configuration

```yaml
# resource-config.yaml - Resource configuration example

worker:
  memory:
    total: 64GB                    # Total Worker memory
    reserved: 4GB                  # System reserved
    query-pool-ratio: 0.9          # Query-available ratio

  pressure-thresholds:
    low: 0.5
    medium: 0.7
    high: 0.85
    critical: 0.95

  spill:
    directory: /data/spill
    max-size: 100GB
    compression: lz4
    file-target-size: 64MB

  threads:
    query-core: 16                 # Query core thread count
    query-max: 32                  # Query max thread count
    io: 64                         # I/O thread count
    scheduler: 4                   # Scheduler thread count

  queues:
    interactive:
      share: 0.3
      max-queries: 100
      timeout: 5m
    batch:
      share: 0.5
      max-queries: 50
      timeout: 2h
    background:
      share: 0.2
      max-queries: 20
      timeout: 24h
```

## 11. Resource Management Best Practices

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Resource Management Best Practices                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  1. Memory configuration principles:                                             │
│     │
│     • Total Worker memory = Reserved (JVM Heap + Off-Heap, ~4-12GB) + Managed Memory│
│     • Managed Memory = Root Memory Arbitrator + RAM Cache Pool (see §12.4)      │
│     • Native memory is uniformly controlled by the Java layer to avoid OOM      │
│     • Reserve 10% of memory for the system and metadata cache                   │
│                                                                                  │
│  2. Spill configuration principles:                                              │
│     • Use NVMe SSD for the Spill directory to avoid becoming a bottleneck       │
│     • Spill space = 2x memory size (to handle extreme cases)                    │
│     • Enable compression (LZ4) to reduce I/O                                    │
│                                                                                  │
│  3. Thread configuration principles:                                             │
│     • Query thread count = CPU core count (CPU-intensive)                       │
│     • I/O thread count = 2x CPU core count (I/O-intensive)                      │
│     • Avoid excessive threads causing context-switch overhead                   │
│                                                                                  │
│  4. Queue configuration principles:                                              │
│     • The Interactive queue guarantees low latency and limits resource usage    │
│     • The Batch queue allows large resource usage but can be preempted          │
│     • The Background queue uses remaining resources without affecting foreground queries│
│                                                                                  │
│  5. Monitoring and alerting principles:                                          │
│     • Memory pressure > 70% starts alerting                                     │
│     • Memory pressure > 85% triggers automatic Spill                            │
│     • Memory pressure > 95% rejects new queries                                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 12. Java-Native Memory Coordination Protocol

The Java-layer MemoryArbitrator (§4) and the Native-layer Velox MemoryArbitrator (native-execution.md §5.6) each manage memory independently. This section defines the coordination mechanism between the two layers to avoid resource waste or OOM caused by fragmented memory management.

### 12.1 Coordination Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Java-Native Memory Coordination Protocol                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Model: Java sets the upper limit + Native manages independently + Java polling monitoring + dynamic adjustment│
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Java MemoryArbitrator                                                   │   │
│  │  • Global budget: 60GB                                                  │   │
│  │  • Allocate a Native memory quota (max_memory_bytes) to each Task       │   │
│  │  • Poll actual Native usage via nativeGetTaskStats()                    │   │
│  │  • Dynamically adjust the quota based on actual usage                   │   │
│  └──────────────────────────────┬──────────────────────────────────────────┘   │
│                                 │ TaskRequest.resource_limit.max_memory_bytes   │
│                                 │ (initial quota)                               │
│                                 ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Native Velox MemoryArbitrator                                           │   │
│  │  • Arbitrate independently within the max_memory_bytes upper limit      │   │
│  │  • Trigger Spill on its own when exceeding the limit (OrderBy/HashBuild/HashAgg/Window)│   │
│  │  • Still insufficient after Spill: return an error code; the Java layer detects it via│   │
│  │    a return value < 0 from nativeGetNextBatch()                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Key constraints:                                                                │
│  • The Native layer does not call back into Java to request more memory (Pull-model principle)│
│  • The Java layer does not directly interfere with Native-internal memory allocation/reclamation│
│  • Coordination is achieved in three steps: "quota setting + polling monitoring + dynamic adjustment"│
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 12.2 Native OOM Handling Flow

When the Native layer still cannot satisfy the memory requirement after Spilling within its quota:

```
Native Velox MemoryArbitrator:
  1. Operator requests memory → exceeds the Task Pool limit
  2. Try to reclaim from other Operators in the same Task → insufficient
  3. Trigger Spill (OrderBy/HashBuild/HashAgg) → release memory
  4. Still insufficient after Spill → throw VeloxRuntimeError("Memory allocation exceeds pool limit")
  5. TaskExecutor catches the exception → sets state = TASK_STATE_FAILED, error_message = OOM details

Java-layer awareness:
  6. nativeGetNextBatch() returns < 0 (error)
  7. Java calls nativeGetTaskStatus() → obtains FAILED + OOM error
  8. The Java MemoryArbitrator decides:
     a. Global memory has spare capacity → can expand the quota for this Task and retry (RESOURCE-class error, retry once)
     b. Global memory is also tight → mark the Task FAILED and report to the Coordinator
  9. The Coordinator decides based on error classification (see §3.3.6 Error Classification and Retry Strategy):
     a. Reschedule to a Worker with lower memory pressure (double the quota)
     b. Retry count exceeded → Query FAILED
```

### 12.3 Dynamic Quota Adjustment

The Java layer monitors Native memory usage by polling `nativeGetTaskStats()` and dynamically adjusts the quota:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Dynamic quota adjustment strategy                                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Polling interval: 1 second (synchronized with ResourceMonitor §9)              │
│                                                                                  │
│  Adjustment rules:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Actual Native usage < 50% of quota (for 30s)                            │   │
│  │  → Reclaim excess quota and return it to the global pool (scale down)   │   │
│  │  → New quota = max(actual usage * 1.5, minimum guaranteed quota)        │   │
│  │                                                                          │   │
│  │  Actual Native usage > 80% of quota and Spill volume > 0                │   │
│  │  → If the global pool has spare capacity, expand the quota (scale up)   │   │
│  │  → New quota = min(current quota * 1.5, global pool allocatable limit)  │   │
│  │  → Notify Native via nativeUpdateMemoryLimit(handle, newLimitBytes)     │   │
│  │                                                                          │   │
│  │  Global memory pressure >= HIGH (85%)                                    │   │
│  │  → Shrink every Task's Native quota to actual usage * 1.2               │   │
│  │  → Trigger Native-layer Spill                                           │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  JNI interface (added to NativeRuntime):                                         │
│  /**                                                                             │
│   * Dynamically update a Task's memory upper limit                              │
│   * After receiving it, the Native layer adjusts the Velox MemoryPool maxBytes  │
│   * If the new limit < current usage, the Native layer triggers Spill until the new limit is satisfied│
│   */                                                                             │
│  public static native void nativeUpdateMemoryLimit(                              │
│      long executorHandle, long newMaxBytes);                                     │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 12.4 Memory Pool Boundary Definition

```
Worker Total Memory: 64GB
├── Reserved (4GB): JVM Heap, Metadata Cache, System Overhead
└── Managed Memory (60GB)
    ├── Root Memory Arbitrator (variable, default 52GB)
    │   ├── Query Memory Pool (variable): query execution + Native compute
    │   │   └── Task Native Pool: quota allocated by Java, managed independently by Native
    │   └── System Pool (10GB): metadata, connections, scheduling, and other system overhead
    └── RAM Cache Pool (fixed, independent, default 8GB)
        └── Does not participate in MemoryArbitrator arbitration
```

#### Boundary Contract Between the Cache Memory Pool and the Query Memory Pool

The RAM Cache ([caching.md](./caching.md)) uses an independent memory pool and does not participate in Root Memory Arbitrator arbitration.

**Design rationale**: The Cache eviction policy (LRU/LFU) and the query Spill policy have different goals — the Cache pursues maximum hit rate, while Spill pursues query completability. Mixed management would cause the Cache to be evicted by query Spill (lowering the hit rate), or Cache usage to block query allocation (causing query failures).

**Boundary rules**:

| Rule | Description |
|------|------|
| Memory isolation | The Cache Pool and Query Pool use independent mmap regions, invisible to each other |
| Static quota division | The Cache Pool size is determined at startup by `pangrova.cache.ram.max-size` and cannot be dynamically adjusted at runtime |
| OOM independence | Query Pool OOM does not trigger Cache eviction; a full Cache Pool only triggers LRU eviction and does not affect the Query Pool |
| Global memory pressure coordination | When the overall Worker memory pressure >= HIGH (85%), the Cache Pool proactively lowers its eviction threshold (from 90% to 70%) to accelerate releasing cache space |
| Startup validation | On Worker startup, validate: `cache.ram.max-size + query.pool.min-size + system.pool.size <= total.managed.memory`; startup fails if not satisfied |

**Cache coordination under global memory pressure**:

```
Worker MemoryPressure = HIGH (85%):
  1. ResourceMonitor detects global memory pressure
  2. Notifies the CacheManager to enter "pressure mode"
  3. The CacheManager lowers the eviction threshold from 90% to 70%
  4. Accelerates eviction of low-priority cache entries (unpinned, low access count)
  5. Released memory is returned to the OS (munmap), not transferred to the Query Pool
  6. Indirectly relieves global memory pressure (the OS can allocate the released physical pages to the Query Pool mmap)

Worker MemoryPressure = CRITICAL (95%):
  1. The CacheManager enters "emergency mode"
  2. Clears all unpinned cache entries
  3. Retains only pinned entries (data actively used by running queries)
```

**Configuration example**:

```yaml
# Memory pool configuration
memory:
  total-managed: 60GB          # Manageable memory beyond Reserved
  cache:
    ram:
      max-size: 8GB            # Cache Pool fixed size
  query:
    min-size: 32GB             # Query Pool minimum guarantee
  system:
    size: 10GB                 # System Pool fixed size
  # Startup validation: 8GB + 32GB + 10GB = 50GB <= 60GB ✓
  # Query Pool actually available: 60GB - 8GB - 10GB = 42GB
```

**Relationship with caching.md**: 09 defines the Cache SPI, eviction policy, and prefetch mechanism; this section defines the source and boundary of Cache memory and its coordination rules with query memory. The two are linked via the `CacheManager.setMemoryPressureMode()` interface.

---

## 13. Memory Overcommit Model (OPT-007)

This section introduces a soft-limit / hard-limit memory model with a global overflow pool, enabling tasks to temporarily borrow idle memory from other tasks before resorting to spill. This reduces unnecessary spill I/O by 30–50% for mixed workloads where memory-intensive and I/O-bound tasks coexist.

> **Relationship to §4 (MemoryArbitrator) and §12 (Java-Native Coordination):**
> The existing MemoryArbitrator (§4) manages hard per-task quotas. The Java-Native coordination protocol (§12) uses `nativeUpdateMemoryLimit()` to set a single hard ceiling. This section extends both by splitting the single limit into a soft limit (guaranteed quota) and a hard limit (maximum borrowable), with an automated reclaim protocol.

### 13.1 Architecture

```
┌──────────────────────────────────────────────────────────┐
│                   Worker Memory Model                     │
│                                                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │              Root Memory Arbitrator (52GB)          │  │
│  │                                                    │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐          │  │
│  │  │ Task A   │ │ Task B   │ │ Task C   │          │  │
│  │  │ soft: 8GB│ │ soft: 8GB│ │ soft: 8GB│          │  │
│  │  │ hard:16GB│ │ hard:16GB│ │ hard:16GB│          │  │
│  │  │ actual:  │ │ actual:  │ │ actual:  │          │  │
│  │  │   12GB   │ │    3GB   │ │    5GB   │          │  │
│  │  └──────────┘ └──────────┘ └──────────┘          │  │
│  │                                                    │  │
│  │  ┌──────────────────────────────────────────────┐ │  │
│  │  │ Global Overflow Pool                          │ │  │
│  │  │ (sum of unused soft limits from idle tasks)   │ │  │
│  │  │ Available: (8-3) + (8-5) = 8GB borrowable    │ │  │
│  │  └──────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │              RAM Cache Pool (8GB, isolated)         │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

**Key concepts:**

| Concept | Description |
|---------|-------------|
| Soft Limit | Guaranteed memory quota per task (from queue config or CBO estimate). Allocations within soft limit are always granted. |
| Hard Limit | Maximum memory a task may use, including borrowed memory. `hard = min(soft × overcommitRatio, workerManaged − reservedForOtherTasks)`. |
| Global Overflow Pool | Virtual pool representing the sum of unused soft-limit headroom across all tasks. Tasks borrowing beyond their soft limit draw from this pool. |
| Reclaim | When Worker memory pressure rises, borrowed memory is reclaimed by forcing spill on borrowing tasks, starting with the least recently active. |

### 13.2 Pseudocode

**Soft/Hard Limit Computation and Memory Request Handling:**

```pseudocode
class MemoryOvercommitManager:
    config:
        overcommitRatio = 2.0     // hard limit = soft limit × overcommitRatio
        reclaimThreshold = 0.85   // reclaim borrowed memory when Worker usage > 85%
        reclaimBatchSize = 256MB  // reclaim in 256MB increments

    function computeLimits(task: Task, workerState: WorkerMemoryState):
        softLimit = task.requestedMemory  // from queue config or CBO estimate
        hardLimit = min(
            softLimit * overcommitRatio,
            workerState.totalManaged - workerState.reservedForOtherTasks
        )
        return (softLimit, hardLimit)

    function onMemoryRequest(task: Task, requestedBytes: long):
        currentUsage = task.getCurrentMemoryUsage()
        softLimit = task.softLimit
        hardLimit = task.hardLimit

        if currentUsage + requestedBytes <= softLimit:
            return GRANT  // within soft limit — always grant

        if currentUsage + requestedBytes <= hardLimit:
            // Borrowing from overflow pool
            availableOverflow = computeOverflowPool()
            if requestedBytes <= availableOverflow:
                return GRANT_BORROWED
            else:
                // Trigger reclaim from other tasks before granting
                reclaimed = reclaimFromIdleTasks(requestedBytes)
                if reclaimed >= requestedBytes:
                    return GRANT_BORROWED
                else:
                    return SPILL  // cannot borrow enough — spill this task

        return SPILL  // exceeds hard limit — must spill
```

**Reclaim Protocol:**

```pseudocode
class MemoryReclaimer:
    function reclaimFromIdleTasks(neededBytes: long):
        totalReclaimed = 0

        // Sort tasks by reclaimability: most idle first
        candidates = getAllTasks()
            .filter(t => t.currentUsage > t.softLimit)  // only reclaim borrowed memory
            .sortBy(t => t.lastActivityTime ASC)         // least recently active first

        for task in candidates:
            if totalReclaimed >= neededBytes:
                break

            reclaimable = task.currentUsage - task.softLimit
            toReclaim = min(reclaimable, neededBytes - totalReclaimed)

            // Notify Velox to spill this amount from the task
            nativeReclaimMemory(task.nativeHandle, toReclaim)
            totalReclaimed += toReclaim

        return totalReclaimed

    function periodicRebalance():
        // Run every 1s (aligned with ResourceMonitor §9 polling interval)
        workerUsage = getTotalManagedUsage()

        if workerUsage > reclaimThreshold * totalManagedMemory:
            // Worker under pressure — reclaim all borrowed memory
            for task in getAllTasks():
                borrowed = max(0, task.currentUsage - task.softLimit)
                if borrowed > 0:
                    nativeReclaimMemory(task.nativeHandle, borrowed)
```

**JNI Interface Changes (extends §12.3):**

```pseudocode
// Existing (§12):  nativeUpdateMemoryLimit(taskHandle, newLimitBytes)
// Enhanced:        nativeUpdateMemoryLimits(taskHandle, softLimitBytes, hardLimitBytes)
// New:             nativeReclaimMemory(taskHandle, bytesToReclaim) -> actualReclaimed
// New:             nativeGetBorrowedMemory(taskHandle) -> borrowedBytes
```

**Affected SPI Interfaces:**

| Interface | Change |
|-----------|--------|
| `TaskRequest` Protobuf | New `soft_memory_limit` and `hard_memory_limit` fields (replacing single `max_memory_bytes`) |
| JNI Bridge | New `nativeReclaimMemory()` and `nativeGetBorrowedMemory()` methods |
| `MemoryArbitrator` (Java, §4) | Enhanced with `MemoryOvercommitManager` integration |
| Worker heartbeat (§14) | New `borrowed_memory_bytes` field for Coordinator visibility |
| Configuration | Proposed keys: `pangrova.memory.overcommit-ratio`, `pangrova.memory.reclaim-threshold` (not in the current typed options) |

### 13.3 Trade-offs

| Aspect | Current (Hard Limit Only) | Proposed (Soft/Hard + Overflow) |
|--------|---------------------------|----------------------------------|
| Performance | Premature spill when Worker has free memory; 30–50% unnecessary spill I/O for mixed workloads | Tasks borrow idle memory; spill only when Worker is truly under pressure; 30–50% spill reduction |
| Complexity | Simple — hard limit per task | Medium — soft/hard limits, overflow pool accounting, reclaim protocol |
| Risk | Low — hard limits prevent OOM | Medium — overcommit can cause cascading spill if many tasks borrow simultaneously; hard limit remains as OOM guard |
| Memory utilization | Low for mixed workloads (idle tasks hold unused memory) | High — unused memory is available to active tasks |
| Cross-language coordination | Minimal (set limit once via §12) | More frequent (reclaim calls, limit updates via enhanced JNI) |

### 13.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| Phase 1 | Soft/Hard Limit Infrastructure | Add `soft_memory_limit` and `hard_memory_limit` to `TaskRequest`. Java sets both; Native respects hard limit. No borrowing yet — soft = hard initially. |
| Phase 2 | Overflow Pool + Borrowing | Implement `MemoryOvercommitManager`. Tasks can borrow up to hard limit from overflow pool. No reclaim yet — borrowed memory is released when task completes. |
| Phase 3 | Active Reclaim | Implement `MemoryReclaimer` with periodic rebalancing. Borrowed memory can be reclaimed via `nativeReclaimMemory()` when Worker is under pressure. |
| Phase 4 | Coordinator-Aware Scheduling | Coordinator considers Worker borrowed memory in scheduling decisions. Avoid scheduling memory-intensive tasks to Workers with high borrowed memory. |

---

## 14. Worker Heartbeat Resource Feedback

The Worker reports real-time resource state to the Coordinator via heartbeats, forming a scheduling-resource closed loop. The Coordinator makes scheduling decisions based on this feedback, avoiding assigning new tasks to high-pressure Workers.

### 14.1 HeartbeatRequest Extension

```protobuf
message HeartbeatRequest {
    string worker_id = 1;
    WorkerStatus status = 2;
    // Resource feedback fields
    MemoryPressureLevel memory_pressure = 3;   // Current memory pressure level
    int32 running_task_count = 4;              // Number of tasks currently executing
    int64 available_memory_bytes = 5;          // Available memory quota in bytes
    double cpu_usage = 6;                      // CPU utilization ratio (0.0 - 1.0)
    int64 spill_disk_available_bytes = 7;      // Remaining spill disk space in bytes
}

enum MemoryPressureLevel {
    NONE = 0;       // < 50% usage, normal operation — maps to Java MemoryPressure.NONE (§3)
    LOW = 1;        // 50-70% usage, monitoring — maps to Java MemoryPressure.LOW (§3)
    MEDIUM = 2;     // 70-85% usage, spill triggered — maps to Java MemoryPressure.MEDIUM (§3)
    HIGH = 3;       // 85-95% usage, aggressive spill — maps to Java MemoryPressure.HIGH (§3)
    CRITICAL = 4;   // > 95% usage, rejecting new tasks — maps to Java MemoryPressure.CRITICAL (§3)
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `memory_pressure` | MemoryPressureLevel | Maps to `ResourceManager.MemoryPressure` (Section 3), reflects real-time memory utilization tier |
| `running_task_count` | int32 | Number of active tasks on this Worker, used by Coordinator to balance load |
| `available_memory_bytes` | int64 | Remaining allocatable memory from Root Memory Arbitrator (Section 2), represents actual scheduling capacity |
| `cpu_usage` | double | CPU utilization ratio (0.0–1.0), sampled at heartbeat time |
| `spill_disk_available_bytes` | int64 | Remaining spill disk space, critical for tasks that may require spill (Section 6) |

### 14.2 Feedback Latency Budget

The feedback loop between Worker state changes and Coordinator awareness must operate within a bounded latency to ensure scheduling decisions are based on reasonably fresh data.

| Parameter | Value | Description |
|-----------|-------|-------------|
| Heartbeat interval | **10 seconds** | Worker sends heartbeat to Coordinator every 10s |
| Max state staleness | **20 seconds** | Coordinator tolerates at most 20s delay between Worker state change and awareness |
| Suspect threshold | **20 seconds** | Worker not heard from for >20s is marked as `SUSPECT` |
| Dead threshold | **60 seconds** | Worker not heard from for >60s is marked as `DEAD` and its tasks are rescheduled |

**Staleness Guarantee:** Under normal network conditions, the Coordinator's view of any Worker's resource state is at most **20 seconds** stale. This means:
- A Worker entering `CRITICAL` memory pressure will stop receiving new tasks within at most 20s.
- A Worker recovering from `HIGH` to `NONE` pressure will resume receiving tasks within at most 20s.

**Worker State Transitions Based on Heartbeat:**

```
                    heartbeat received
    ┌──────────┐   (within 20s)    ┌──────────┐
    │  ACTIVE  │◄──────────────────│  ACTIVE  │
    └────┬─────┘                   └──────────┘
         │ no heartbeat > 20s
         ▼
    ┌──────────┐
    │ SUSPECT  │  (stop scheduling new tasks)
    └────┬─────┘
         │ no heartbeat > 60s
         ▼
    ┌──────────┐
    │   DEAD   │  (reschedule all tasks to other Workers)
    └──────────┘
```

> **Cross-Reference:** Coordinator scheduling rules based on these resource feedback fields are defined in `distributed.md` (Scheduling-Resource Closed Loop section).




## 15. Multi-Tenant Resource Isolation

The multi-tenant resource isolation strategy works together with [multi-tenancy.md](./multi-tenancy.md) to implement tenant-level quotas and isolation at the resource management layer.

### 15.1 Tenant Resource Quota Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Tenant Resource Isolation                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Root Memory Arbitrator (60GB)                                                   │
│  ├── Tenant-A Pool (30GB, 50%)                                                  │
│  │   ├── Query-1 Pool (15GB)                                                    │
│  │   └── Query-2 Pool (15GB)                                                    │
│  ├── Tenant-B Pool (18GB, 30%)                                                  │
│  │   └── Query-3 Pool (18GB)                                                    │
│  └── Shared Pool (12GB, 20%)  ← queries of unassigned tenants + system tasks   │
│                                                                                  │
│  Isolation strategy:                                                             │
│  • Hard Limit: a tenant cannot exceed its quota upper limit; exceeding triggers Spill or rejection│
│  • Soft Limit: a tenant can temporarily borrow the Shared Pool and return it when idle│
│  • Priority: queries within a tenant are scheduled by QueueType priority (see §8)│
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 15.2 Tenant Quota Configuration

```yaml
# tenant-resource-config.yaml
tenants:
  tenant-a:
    memory-share: 0.5          # 50% of query pool
    max-concurrent-queries: 50
    max-query-memory: 20GB     # Single-query upper limit
    queue-priority: INTERACTIVE
  tenant-b:
    memory-share: 0.3
    max-concurrent-queries: 20
    max-query-memory: 10GB
    queue-priority: BATCH
  shared:
    memory-share: 0.2          # Default pool for unbound tenants
```

### 15.3 Tenant Resource Allocation

```java
/**
 * Tenant resource allocator
 * Add a tenant dimension to ResourceManager.allocateForQuery()
 */
public class TenantResourceAllocator {

    private final Map<String, TenantResourcePool> tenantPools;

    public QueryResourceContext allocateForTenant(
            String tenantId, QueryId queryId, ResourceRequirement requirement) {
        TenantResourcePool pool = tenantPools.getOrDefault(tenantId, sharedPool);

        if (pool.getUsedMemory() + requirement.getEstimatedMemory() > pool.getHardLimit()) {
            throw new TenantQuotaExceededException(tenantId);
        }

        return pool.allocate(queryId, requirement);
    }
}
```

---



## 16. Observability Integration

This section defines the metrics and tracing integration points for the resource management layer. For the full observability design, see [observability.md](./observability.md).

### 16.1 Metrics


Metrics use the `pangrova.worker.*` prefix per [14 §3.2](./observability.md) naming convention:

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.worker.memory.used` | MemoryArbitrator | Current memory usage |
| `pangrova.worker.memory.reserved` | MemoryArbitrator | Reserved memory |
| `pangrova.worker.memory.pressure` | ResourceMonitor | Current MemoryPressure level |
| `pangrova.worker.spill.bytes` | SpillManager | Bytes spilled to disk |
| `pangrova.worker.spill.count` | SpillManager | Spill operation count |
| `pangrova.worker.threads.active` | ThreadPoolManager | Active thread count |
| `pangrova.worker.resource.queue.pending` | ResourceQueue | Pending resource requests |

### 16.2 Tracing Spans

Resource management operations are typically children of `task.execute` spans:

- `memory.arbitrate` — Memory arbitration decision (child of `task.execute`)
- `spill.execute` — Spill operation (child of `task.execute`)

### 16.3 Log Events

- Memory pressure level transitions (INFO)
- Spill trigger events with operator and bytes (INFO)
- Memory arbitration failures (WARN)
- Resource queue admission/rejection events (DEBUG)

See [14 §7](./observability.md) for structured logging format.



> **Spillable Operators Cross-Reference**: The authoritative list of spillable operators is maintained in this document (06 §5). The Native layer (03) uses Velox-specific operator names. Mapping: HashBuild (06) = HashBuild (03), Sort (06) = OrderBy (03), HashAgg (06) = HashAggregation (03), WindowSort (06) = Window (03). Any operator added to the spill framework must be registered in both documents.


> **ResourceMonitor.run() (§9)**: The monitoring loop collects metrics from all active query pools via `MemoryArbitrator.getPoolStats()`, evaluates pressure level per §3 thresholds, and emits `pangrova.resource.memory.pressure_level` gauge. If pressure >= HIGH, it triggers preemptive spill on the largest consumer via `triggerSpill(targetBytes)`. The loop interval is configured by `resource.monitor.interval` (default: 1s).

---



## 17. Dynamic Cache-Query Memory Boundary (OPT-014)

This section introduces a dynamic boundary between the Cache Memory Pool and the Query Memory Pool, allowing memory to flow bidirectionally based on workload demands. This complements the Memory Overcommit Model (§13 OPT-007) for holistic memory management.

### 17.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Dynamic Memory Boundary                             │
│                                                                  │
│  Total Managed: 60GB                                             │
│  ┌──────────────────────┬───────────────────────────────────┐   │
│  │   Cache Pool          │         Query Pool                │   │
│  │   (min 4GB)           │         (min 40GB)                │   │
│  │                       │                                   │   │
│  │   initial: 8GB        │         initial: 52GB             │   │
│  │                       │                                   │   │
│  │   ◄── shrink ─────── │ ── grow ──►                       │   │
│  │   ── grow ──►         │ ◄── shrink ───                    │   │
│  └──────────────────────┴───────────────────────────────────┘   │
│                                                                  │
│  Rebalance every 10s:                                            │
│    Query pressure HIGH + cache > min → shrink cache              │
│    Cache util > 90% + query pressure LOW → grow cache            │
│    Cache util < 50% + cache > min → return to query              │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: RAM Cache Pool (8GB default) and Query Pool (~52GB) are statically partitioned at Worker startup. Scan-heavy workloads underutilize cache while query memory is under pressure. Compute-heavy workloads waste cache memory that operators need.

### 17.2 Pseudocode

```pseudocode
class DynamicMemoryBoundary:
    totalManaged = 60GB
    cacheMinGuarantee = 4GB
    queryMinGuarantee = 40GB
    rebalanceInterval = 10s
    cacheGrowThreshold = 0.95   // don't grow if hit rate > 95%
    cacheShrinkThreshold = 0.5  // shrink if utilization < 50%

    currentCacheSize = 8GB
    currentQuerySize = 52GB

    function rebalance():
        cacheUtil = cacheManager.utilization
        cacheHitRate = cacheManager.recentHitRate
        queryPressure = queryMemoryArbitrator.pressure

        // Case 1: Query needs more — shrink cache
        if queryPressure >= HIGH AND currentCacheSize > cacheMinGuarantee:
            shrinkAmount = min(currentCacheSize - cacheMinGuarantee, 2GB)
            cacheManager.evictUnpinned(shrinkAmount)
            currentCacheSize -= shrinkAmount
            currentQuerySize += shrinkAmount

        // Case 2: Cache needs more — grow cache
        elif cacheUtil > 0.9 AND cacheHitRate < cacheGrowThreshold
             AND queryPressure <= LOW:
            growAmount = min(currentQuerySize - queryMinGuarantee, 2GB)
            currentCacheSize += growAmount
            currentQuerySize -= growAmount
            cacheManager.setMaxSize(currentCacheSize)

        // Case 3: Cache underutilized — return to query
        elif cacheUtil < cacheShrinkThreshold
             AND currentCacheSize > cacheMinGuarantee:
            shrinkAmount = min(
                currentCacheSize - cacheMinGuarantee,
                (1.0 - cacheUtil) * currentCacheSize * 0.5)
            currentCacheSize -= shrinkAmount
            currentQuerySize += shrinkAmount

        nativeUpdateQueryPoolSize(currentQuerySize)
```

### 17.3 Trade-offs

| Aspect | Static Partition (Current) | Dynamic Boundary |
|--------|---------------------------|------------------|
| Performance | Suboptimal for mixed workloads | Near-optimal — memory flows to where needed |
| Complexity | Simple — fixed at startup | Medium — periodic rebalancing, eviction coordination |
| Risk | None | Low — guaranteed minimums prevent starvation; 2GB increments |
| Memory utilization | Waste under varying workloads | Adaptive to workload pattern changes |

### 17.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Metrics Collection | Track cache utilization, hit rate, query memory pressure. Log rebalancing decisions without acting. |
| 2 | Query→Cache Shrink | Allow query pool to reclaim from cache under HIGH pressure. One-directional. |
| 3 | Bidirectional Rebalancing | Allow cache to grow into unused query memory. Full dynamic boundary. |

> **Cross-reference**: Dynamic boundary complements Memory Overcommit (§13 OPT-007) — overcommit handles intra-query-pool borrowing, while dynamic boundary handles inter-pool rebalancing. Cache pool integration with [caching.md §5](./caching.md) CacheManager. The `nativeUpdateQueryPoolSize()` JNI method updates the Velox MemoryArbitrator (§12).


### Java spill ownership and actual reclaimed bytes

`SpillManager` retains a live `SpillableBuffer` owner for each candidate. Candidate
byte counts order work; `handleMemoryPressure` reports only the actual return from
`spillToDisk(remainingTarget)`. A partial spill retains the owner and its live byte
count. A drained candidate is removed by exact candidate identity. Each buffer
serializes its own operations; the manager does not hold a global lock during IO.

`DiskSpillableBuffer` owns unique files under its configured directory, removes
heap batches only after successful writes, and publishes restored batches only
after all pending files are read. It restores the original order. Cleanup failures
remain visible and their paths remain owned for a later `close()` retry. Its byte
count describes retained payloads, not JVM garbage collection or an arbitrator
reservation. An implementation charged to an arbitrator must release its actual
reservation in the same successful spill operation.

The current Worker configures a Java spill manager, but its pressure callback
serves descriptor-only task admission. Canonical task admission bypasses that
branch, and there is no production registration of `DiskSpillableBuffer` candidates.
The Java shuffle classes have a separate optional `ShuffleDiskSpiller` implementation;
no production construction of those classes is wired to this manager. Native
operators use their task grant and Velox spill lifecycle. Java component IO tests
do not qualify those production execution paths.
