package org.pangrova.worker.resource.memory;

import org.pangrova.worker.enums.MemoryDomain;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single unified ledger tracking memory usage across all domains:
 * Java heap, native (Velox) off-heap, and additional off-heap pools.
 *
 * <p>Task 203 fix (G-INTEROP-04): previously the system had 4+ independent
 * ledgers (MemoryArbitrator, NativeMemoryCoordinator standalone arbitrator,
 * WorkerResourceBootstrap OS tracking, per-task allocators) with no unified
 * Java+native budget. The admission controller was native-RSS-blind.</p>
 *
 * <p>This ledger is the single source of truth. It:</p>
 * <ul>
 *   <li>Tracks per-domain usage via atomic counters</li>
 *   <li>Enforces a single total budget across all domains</li>
 *   <li>Feeds the admission controller with native-RSS-aware data</li>
 *   <li>Implements {@link MemoryBudgetEnforcer} for cross-domain decisions</li>
 * </ul>
 *
 * <p>Concurrency: all counters are {@link AtomicLong}; no locks needed.</p>
 */
@ThreadSafe
public final class UnifiedMemoryLedger implements MemoryBudgetEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedMemoryLedger.class);

    private final long totalPhysicalBudget;
    private final AtomicLong javaHeapUsed = new AtomicLong(0);
    private final AtomicLong nativeUsed = new AtomicLong(0);
    private final AtomicLong offHeapUsed = new AtomicLong(0);
    /**
     * A-MEM-01: authoritative aggregate-usage gate. All reservations are admitted
     * by an atomic compare-and-set on this counter, so the shared physical budget
     * can never be exceeded — not even transiently — under concurrency. Kept exactly
     * in sync with the sum of the per-domain counters (which serve the reporting
     * breakdown) by every mutation path (reserve / release / updateUsage).
     */
    private final AtomicLong totalUsed = new AtomicLong(0);

    /** Threshold ratios for enforcement decisions. */
    private static final double SPILL_THRESHOLD = 0.75;
    private static final double SHRINK_THRESHOLD = 0.85;
    private static final double KILL_THRESHOLD = 0.95;
    /** Maximum tolerated drift between snapshot usage and ledger accounting. */
    private static final double MAX_ACCOUNTING_DRIFT_RATIO = 0.1;

    /**
     * Creates a unified memory ledger.
     *
     * @param totalPhysicalBudget total memory budget in bytes across all domains
     */
    public UnifiedMemoryLedger(long totalPhysicalBudget) {
        if (totalPhysicalBudget <= 0) {
            throw new IllegalArgumentException(
                    "totalPhysicalBudget must be positive, got " + totalPhysicalBudget);
        }
        this.totalPhysicalBudget = totalPhysicalBudget;
        LOG.info("event=unified_memory_ledger_init totalBudget={}", totalPhysicalBudget);
    }

    // --- Domain-level reserve/release API ---

    /**
     * Reserves memory in the specified domain.
     *
     * @param domain the memory domain
     * @param bytes  requested byte count (positive)
     * @return actual bytes reserved (0 if total budget exhausted)
     */
    public long reserve(MemoryDomain domain, long bytes) {
        if (bytes <= 0 || domain == null) {
            return 0;
        }
        // A-MEM-01: admit against the shared physical budget via a single atomic gate
        // (totalUsed). The previous check-then-act on the summed domain counters
        // allowed concurrent reservations to collectively exceed the budget. The CAS
        // below makes the budget check-and-increment atomic; the per-domain counter is
        // then updated for the reporting breakdown.
        while (true) {
            long current = totalUsed.get();
            long available = totalPhysicalBudget - current;
            if (available <= 0) {
                return 0;
            }
            long toAllocate = Math.min(bytes, available);
            if (totalUsed.compareAndSet(current, current + toAllocate)) {
                counterFor(domain).addAndGet(toAllocate);
                return toAllocate;
            }
        }
    }

    /**
     * Releases memory from the specified domain.
     *
     * @param domain the memory domain
     * @param bytes  byte count to release (positive)
     */
    public void release(MemoryDomain domain, long bytes) {
        if (bytes <= 0 || domain == null) {
            return;
        }
        AtomicLong counter = counterFor(domain);
        while (true) {
            long current = counter.get();
            long toRelease = Math.min(bytes, current);
            if (toRelease <= 0) {
                return;
            }
            if (counter.compareAndSet(current, current - toRelease)) {
                totalUsed.addAndGet(-toRelease);
                return;
            }
        }
    }

    /**
     * Updates the accounting for a domain directly (e.g., from native RSS sampling).
     * This is a set operation, not additive.
     *
     * @param domain the memory domain
     * @param usedBytes current used bytes in this domain
     */
    public void updateUsage(MemoryDomain domain, long usedBytes) {
        if (domain == null || usedBytes < 0) {
            return;
        }
        // Keep the authoritative aggregate gate in sync with the domain reset.
        long previous = counterFor(domain).getAndSet(usedBytes);
        totalUsed.addAndGet(usedBytes - previous);
    }

    // --- Query API ---

    /** Returns total used bytes across all domains (authoritative budget gate). */
    public long totalUsedBytes() {
        return totalUsed.get();
    }

    /** Returns total available bytes across all domains. */
    public long totalAvailableBytes() {
        return Math.max(0, totalPhysicalBudget - totalUsedBytes());
    }

    /** Returns the total physical memory budget. */
    public long totalBudget() {
        return totalPhysicalBudget;
    }

    /** Returns current usage for a specific domain. */
    public long usedBytes(MemoryDomain domain) {
        return counterFor(domain).get();
    }

    /** Returns overall utilisation ratio. */
    public double utilisation() {
        return (double) totalUsedBytes() / totalPhysicalBudget;
    }

    /**
     * Returns a snapshot of all domains for the {@link MemoryBudgetEnforcer} API.
     */
    public Map<MemoryDomain, MemoryDomainSnapshot> snapshot() {
        long jh = javaHeapUsed.get();
        long nv = nativeUsed.get();
        long oh = offHeapUsed.get();
        // Budget split: proportional to current usage or equal if no usage
        long total = jh + nv + oh;
        long jBudget = total > 0 ? totalPhysicalBudget * jh / total : totalPhysicalBudget / 3;
        long nBudget = total > 0 ? totalPhysicalBudget * nv / total : totalPhysicalBudget / 3;
        long oBudget = total > 0 ? totalPhysicalBudget * oh / total : totalPhysicalBudget / 3;
        // Ensure at least 1 byte budget for non-zero domains
        jBudget = Math.max(jBudget, 1);
        nBudget = Math.max(nBudget, 1);
        oBudget = Math.max(oBudget, 1);
        return Map.of(
                MemoryDomain.JAVA_HEAP, new MemoryDomainSnapshot(MemoryDomain.JAVA_HEAP, jh, jBudget),
                MemoryDomain.NATIVE, new MemoryDomainSnapshot(MemoryDomain.NATIVE, nv, nBudget),
                MemoryDomain.OFF_HEAP, new MemoryDomainSnapshot(MemoryDomain.OFF_HEAP, oh, oBudget));
    }

    // --- MemoryBudgetEnforcer implementation ---

    @Override
    public EnforcementDecision enforce(Map<MemoryDomain, MemoryDomainSnapshot> snapshots) {
        double ratio = mergedUtilisation(snapshots);
        if (ratio >= KILL_THRESHOLD) {
            MemoryDomain largest = findLargestDomain(snapshots);
            return EnforcementDecision.killLargest(largest,
                    String.format("unified utilisation %.1f%% >= kill threshold %.1f%%",
                            ratio * 100, KILL_THRESHOLD * 100));
        }
        if (ratio >= SHRINK_THRESHOLD) {
            return EnforcementDecision.shrinkNative(
                    String.format("unified utilisation %.1f%% >= shrink threshold %.1f%%",
                            ratio * 100, SHRINK_THRESHOLD * 100));
        }
        if (ratio >= SPILL_THRESHOLD) {
            MemoryDomain largest = findLargestDomain(snapshots);
            return EnforcementDecision.spill(largest,
                    String.format("unified utilisation %.1f%% >= spill threshold %.1f%%",
                            ratio * 100, SPILL_THRESHOLD * 100));
        }
        return EnforcementDecision.none();
    }

    @Override
    public double mergedUtilisation(Map<MemoryDomain, MemoryDomainSnapshot> snapshots) {
        long snapshotUsedBytes = snapshots.values().stream()
                .mapToLong(MemoryDomainSnapshot::usedBytes)
                .sum();
        return (double) snapshotUsedBytes / totalPhysicalBudget;
    }

    @Override
    public String detectDrift(Map<MemoryDomain, MemoryDomainSnapshot> snapshots,
                              long physicalMemoryBytes) {
        long totalBudget = snapshots.values().stream()
                .mapToLong(MemoryDomainSnapshot::budgetBytes)
                .sum();
        if (totalBudget > physicalMemoryBytes) {
            return String.format("sum of domain budgets (%d) exceeds physical memory (%d)",
                    totalBudget, physicalMemoryBytes);
        }
        long snapshotUsedBytes = snapshots.values().stream()
                .mapToLong(MemoryDomainSnapshot::usedBytes)
                .sum();
        long trackedUsed = totalUsedBytes();
        double drift = Math.abs((double) (snapshotUsedBytes - trackedUsed) / Math.max(trackedUsed, 1));
        if (drift > MAX_ACCOUNTING_DRIFT_RATIO) {
            return String.format("accounting drift %.1f%% between snapshot (%d) and ledger (%d)",
                    drift * 100, snapshotUsedBytes, trackedUsed);
        }
        return "";
    }

    @Override
    public String name() {
        return "unified-ledger";
    }

    // --- Internal helpers ---

    private AtomicLong counterFor(MemoryDomain domain) {
        return switch (domain) {
            case JAVA_HEAP -> javaHeapUsed;
            case NATIVE -> nativeUsed;
            case OFF_HEAP -> offHeapUsed;
        };
    }

    private static MemoryDomain findLargestDomain(
            Map<MemoryDomain, MemoryDomainSnapshot> snapshots) {
        return snapshots.entrySet().stream()
                .max(java.util.Comparator.comparingLong(e -> e.getValue().usedBytes()))
                .map(Map.Entry::getKey)
                .orElse(MemoryDomain.JAVA_HEAP);
    }
}
