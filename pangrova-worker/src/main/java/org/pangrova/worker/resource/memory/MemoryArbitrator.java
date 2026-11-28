package org.pangrova.worker.resource.memory;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.worker.resource.tenant.TenantResourceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Single source of truth for all memory accounting in the resource SPI layer.
 *
 * <p>All memory pools created via {@link #subPool(String, MemoryPool)} delegate their
 * reserve/release operations through this arbitrator. This eliminates dual-ledger inconsistency by
 * ensuring exactly one authoritative counter for global memory usage.
 *
 * <p>Concurrency strategy: all mutable state is managed via {@link AtomicLong} with CAS loops,
 * ensuring thread-safe allocation and release without locks.
 */
@ThreadSafe
public class MemoryArbitrator {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryArbitrator.class);

    private final long totalBudget;
    private final long systemPoolReserved;
    private final long allocatableBudget;
    private final AtomicLong usedMemory = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> queryAllocations =
            new ConcurrentHashMap<>();

    /** Per-pool tracking for sub-pools created via {@link #subPool}. */
    private final ConcurrentHashMap<String, AtomicLong> poolAllocations = new ConcurrentHashMap<>();

    /** Tracks when each query allocation was last updated, for stale allocation detection. */
    private final ConcurrentHashMap<String, Instant> queryAllocationTimestamps =
            new ConcurrentHashMap<>();

    /** Maximum age for a query allocation before it is considered stale. */
    private static final Duration STALE_ALLOCATION_THRESHOLD = Duration.ofMinutes(60);

    /** Per-tenant resource tracker; null means no tenant enforcement. */
    private volatile TenantResourceTracker tenantTracker;

    /** Maps queryId → tenantId for tenant-aware release. */
    private final ConcurrentHashMap<String, String> queryTenantMap = new ConcurrentHashMap<>();

    /** Registered shrinkable consumers that can release memory on demand. */
    private final ConcurrentHashMap<String, ShrinkableMemoryConsumer> consumers =
            new ConcurrentHashMap<>();

    public MemoryArbitrator(long totalBudget, long systemPoolReserved) {
        this(totalBudget, systemPoolReserved, null);
    }

    /**
     * Creates a memory arbitrator with optional tenant quota enforcement.
     *
     * @param totalBudget total memory budget in bytes
     * @param systemPoolReserved bytes reserved for system pool
     * @param tenantTracker per-tenant resource tracker (null disables tenant enforcement)
     */
    public MemoryArbitrator(
            long totalBudget, long systemPoolReserved, TenantResourceTracker tenantTracker) {
        this.totalBudget = totalBudget;
        this.systemPoolReserved = systemPoolReserved;
        this.allocatableBudget = totalBudget - systemPoolReserved;
        this.tenantTracker = tenantTracker;
        LOG.info(
                "MemoryArbitrator initialized: totalBudget={}, reserved={}, allocatable={},"
                    + " tenantAware={}",
                totalBudget,
                systemPoolReserved,
                allocatableBudget,
                tenantTracker != null);
    }

    // --- Sub-pool management (single source of truth API) ---

    /**
     * Creates a sub-pool backed by this arbitrator. The returned pool delegates all reserve/release
     * operations through this arbitrator, ensuring a single authoritative ledger.
     *
     * @param name human-readable pool name
     * @param parent parent pool (used for hierarchy tracking; may be null for root-level)
     * @return a new ArbitratedMemoryPool that delegates to this arbitrator
     */
    public ArbitratedMemoryPool subPool(String name, MemoryPool parent) {
        long parentMax = parent != null ? parent.getMaxBytes() : allocatableBudget;
        ArbitratedMemoryPool pool = new ArbitratedMemoryPool(name, parentMax, this);
        poolAllocations.putIfAbsent(name, new AtomicLong(0));
        LOG.info("event=sub_pool_created name={} maxBytes={}", name, parentMax);
        return pool;
    }

    /**
     * Reserves bytes for a specific pool, enforcing both the pool's local cap and the global
     * budget. This is the unified entry point for pool-based memory accounting.
     *
     * @param pool the pool requesting memory
     * @param bytes requested byte count (positive)
     * @return actual bytes reserved (0 if denied)
     */
    public long reserve(MemoryPool pool, long bytes) {
        if (bytes <= 0 || pool == null) {
            return 0;
        }
        long allocated = tryAllocate(bytes);
        if (allocated > 0) {
            poolAllocations
                    .computeIfAbsent(pool.getName(), k -> new AtomicLong(0))
                    .addAndGet(allocated);
        }
        return allocated;
    }

    /**
     * Releases bytes from a specific pool back to the global budget.
     *
     * @param pool the pool releasing memory
     * @param bytes byte count to release (positive)
     */
    public void release(MemoryPool pool, long bytes) {
        if (bytes <= 0 || pool == null) {
            return;
        }
        long released = releaseGlobalMemory(bytes);
        if (released > 0) {
            AtomicLong poolUsage = poolAllocations.get(pool.getName());
            if (poolUsage != null) {
                decrementAtomicLong(poolUsage, released);
            }
        }
    }

    /**
     * Returns a snapshot of per-pool memory usage. Keys are pool names, values are currently
     * allocated bytes.
     *
     * @return unmodifiable map of pool name to allocated bytes
     */
    public Map<String, Long> snapshot() {
        return poolAllocations.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().get()));
    }

    /**
     * Returns an immutable snapshot of actual query-key allocations, including serialized buffers.
     */
    public Map<String, Long> queryAllocationsSnapshot() {
        return queryAllocations.entrySet().stream()
                .collect(
                        Collectors.toUnmodifiableMap(
                                Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    // --- Query-based allocation API (existing) ---

    public MemoryPressure getPressure() {
        double ratio = (double) usedMemory.get() / allocatableBudget;
        return MemoryPressure.fromRatio(ratio);
    }

    /** Requests memory for a query against the global allocatable budget. */
    public long requestMemory(String queryId, long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        long allocated = tryAllocate(bytes);
        if (allocated > 0) {
            trackQueryAllocation(queryId, allocated);
        }
        return allocated;
    }

    /**
     * Requests memory for a query with per-tenant quota enforcement. Checks tenant budget before
     * global allocation.
     *
     * @return allocated bytes (0 if rejected by tenant quota or global budget)
     */
    public long requestMemory(String queryId, String tenantId, long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        TenantResourceTracker tracker = this.tenantTracker;
        if (tracker != null && tenantId != null) {
            return allocateWithTenantQuota(queryId, tenantId, bytes, tracker);
        }
        return requestMemory(queryId, bytes);
    }

    private long allocateWithTenantQuota(
            String queryId, String tenantId, long bytes, TenantResourceTracker tracker) {
        if (!tracker.tryAllocateMemory(tenantId, bytes)) {
            return 0;
        }
        long allocated = tryAllocate(bytes);
        adjustTenantAfterAllocation(queryId, tenantId, bytes, allocated, tracker);
        return allocated;
    }

    private void adjustTenantAfterAllocation(
            String queryId,
            String tenantId,
            long requested,
            long allocated,
            TenantResourceTracker tracker) {
        if (allocated > 0) {
            queryTenantMap.putIfAbsent(queryId, tenantId);
            trackQueryAllocation(queryId, allocated);
        }
        long overReserved = requested - Math.max(allocated, 0);
        if (overReserved > 0) {
            tracker.releaseMemory(tenantId, overReserved);
        }
    }

    /** Releases memory for a query. Uses CAS loop to ensure usedMemory never goes below zero. */
    public void release(String queryId, long bytes) {
        if (bytes <= 0) {
            return;
        }
        long released = releaseGlobalMemory(bytes);
        if (released > 0) {
            releaseTenantMemory(queryId, released);
            decrementQueryAllocation(queryId, released);
        }
    }

    private void releaseTenantMemory(String queryId, long bytes) {
        TenantResourceTracker tracker = this.tenantTracker;
        String tenantId = queryTenantMap.get(queryId);
        if (tracker != null && tenantId != null) {
            tracker.releaseMemory(tenantId, bytes);
        }
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    public long getAvailableMemory() {
        return allocatableBudget - usedMemory.get();
    }

    public long getTotalBudget() {
        return totalBudget;
    }

    public long getSystemReserved() {
        return systemPoolReserved;
    }

    /**
     * Clears all query and pool allocations and resets used memory to zero. Called during shutdown
     * to release all tracked resources.
     */
    public void clearAllocations() {
        int cleared = queryAllocations.size() + poolAllocations.size();
        queryAllocations.clear();
        queryTenantMap.clear();
        queryAllocationTimestamps.clear();
        poolAllocations.clear();
        usedMemory.set(0);
        LOG.info("Cleared {} allocations during shutdown", cleared);
    }

    /**
     * Detects and releases query allocations that have not been updated within {@link
     * #STALE_ALLOCATION_THRESHOLD}. This acts as a watchdog against leaked allocations from queries
     * that terminated abnormally without calling {@link #release(String, long)}.
     *
     * <p>Intended to be called periodically by an external scheduler (e.g., every 10 minutes).
     *
     * @return the number of stale query allocations evicted
     */
    public int evictStaleAllocations() {
        Instant cutoff = Instant.now().minus(STALE_ALLOCATION_THRESHOLD);
        int evicted = 0;
        for (Map.Entry<String, Instant> entry : queryAllocationTimestamps.entrySet()) {
            if (entry.getValue().isBefore(cutoff)) {
                String queryId = entry.getKey();
                AtomicLong allocation = queryAllocations.remove(queryId);
                if (allocation != null) {
                    long leaked = allocation.get();
                    if (leaked > 0) {
                        releaseGlobalMemory(leaked);
                        releaseTenantMemory(queryId, leaked);
                    }
                    LOG.warn(
                            "event=stale_allocation_evicted queryId={} leakedBytes={}",
                            queryId,
                            leaked);
                    evicted++;
                }
                queryTenantMap.remove(queryId);
                queryAllocationTimestamps.remove(queryId);
            }
        }
        return evicted;
    }

    // --- Shrinkable consumer registration and global reserve API ---

    /**
     * Registers a shrinkable memory consumer. When memory pressure is high and a new reservation
     * cannot be satisfied, the arbitrator will invoke {@link ShrinkableMemoryConsumer#shrink(long)}
     * on registered consumers.
     */
    public void registerConsumer(ShrinkableMemoryConsumer consumer) {
        if (consumer == null) {
            return;
        }
        consumers.put(consumer.consumerId(), consumer);
        LOG.info("event=consumer_registered consumerId={}", consumer.consumerId());
    }

    /** Unregisters a previously registered consumer. */
    public void unregisterConsumer(String consumerId) {
        if (consumerId == null) {
            return;
        }
        consumers.remove(consumerId);
        LOG.info("event=consumer_unregistered consumerId={}", consumerId);
    }

    /**
     * Reserves memory from the global budget. If insufficient space is available, triggers shrink
     * callbacks on registered consumers to reclaim memory.
     *
     * <p>This method is the unified entry point for all memory consumers (native off-heap, Java
     * heap buffers) to participate in a single global memory cap, preventing OS-level OOM.
     *
     * @param bytes requested byte count (positive)
     * @return actual bytes reserved (0 if denied even after shrink attempts)
     */
    public long reserve(long bytes) {
        if (bytes <= 0) {
            return 0;
        }
        long available = allocatableBudget - usedMemory.get();
        if (available >= bytes) {
            long allocated = tryAllocate(bytes);
            if (allocated == bytes) {
                return allocated;
            }
            if (allocated > 0) {
                releaseGlobalMemory(allocated);
            }
        }
        long deficit = bytes - Math.max(0, available);
        long reclaimed = triggerShrink(deficit);
        if (reclaimed > 0 || available > 0) {
            return tryAllocate(bytes);
        }
        return 0;
    }

    /**
     * Releases a previously reserved amount back to the global budget. Used by non-query consumers
     * (e.g., native memory coordinator).
     *
     * @param bytes byte count to release (positive)
     */
    public void releaseReservation(long bytes) {
        if (bytes <= 0) {
            return;
        }
        releaseGlobalMemory(bytes);
    }

    /**
     * Triggers shrink callbacks on all registered consumers to reclaim at least {@code
     * targetBytes}. Consumers are invoked in order of descending current usage (largest consumer
     * first).
     *
     * @param targetBytes desired bytes to reclaim
     * @return total bytes actually reclaimed across all consumers
     */
    long triggerShrink(long targetBytes) {
        List<ShrinkableMemoryConsumer> sorted = new ArrayList<>(consumers.values());
        sorted.sort((a, b) -> Long.compare(b.currentUsageBytes(), a.currentUsageBytes()));

        long totalReclaimed = 0;
        for (ShrinkableMemoryConsumer consumer : sorted) {
            long remaining = targetBytes - totalReclaimed;
            if (remaining <= 0) {
                break;
            }
            long reclaimed = consumer.shrink(remaining);
            if (reclaimed > 0) {
                totalReclaimed += reclaimed;
                LOG.info(
                        "event=consumer_shrink consumerId={} reclaimed={} target={}",
                        consumer.consumerId(),
                        reclaimed,
                        remaining);
            }
        }
        return totalReclaimed;
    }

    // --- Internal helpers ---

    private long tryAllocate(long requested) {
        while (true) {
            long current = usedMemory.get();
            long available = allocatableBudget - current;
            long toAllocate = Math.min(requested, available);
            if (toAllocate <= 0) {
                return 0;
            }
            if (usedMemory.compareAndSet(current, current + toAllocate)) {
                return toAllocate;
            }
        }
    }

    private long releaseGlobalMemory(long bytes) {
        while (true) {
            long current = usedMemory.get();
            long toRelease = Math.min(bytes, current);
            if (toRelease <= 0) {
                return 0;
            }
            if (usedMemory.compareAndSet(current, current - toRelease)) {
                return toRelease;
            }
        }
    }

    private void trackQueryAllocation(String queryId, long bytes) {
        queryAllocations.computeIfAbsent(queryId, k -> new AtomicLong(0)).addAndGet(bytes);
        queryAllocationTimestamps.put(queryId, Instant.now());
    }

    private void decrementQueryAllocation(String queryId, long bytes) {
        AtomicLong allocation = queryAllocations.get(queryId);
        if (allocation == null) {
            return;
        }
        long remaining = decrementAtomicLong(allocation, bytes);
        if (remaining <= 0) {
            queryAllocations.remove(queryId);
            queryTenantMap.remove(queryId);
            queryAllocationTimestamps.remove(queryId);
        }
    }

    private long decrementAtomicLong(AtomicLong counter, long bytes) {
        while (true) {
            long current = counter.get();
            long toRelease = Math.min(bytes, current);
            if (toRelease <= 0) {
                return current;
            }
            if (counter.compareAndSet(current, current - toRelease)) {
                return current - toRelease;
            }
        }
    }
}
