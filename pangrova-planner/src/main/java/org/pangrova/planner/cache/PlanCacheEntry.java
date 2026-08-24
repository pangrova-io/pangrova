package org.pangrova.planner.cache;

import org.pangrova.planner.PlanResult;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Internal cache entry: an optimized {@link PlanResult} together with the set
 * of table names it references, so the cache can perform table-scoped
 * invalidation without walking the RelNode tree on every event.
 *
 * <p>{@code referencedTables} is captured at insertion time
 * ({@link PlanCache#put(PlanCacheKey, PlanResult)}) and is unmodifiable.</p>
 *
 * <p>A-PLAN-01: {@code tableVersions} records the monotonic version of each referenced table at
 * plan time. {@link PlanCache#get} validates these on every hit and evicts the entry if any
 * referenced table's version has advanced since — closing the put/invalidate race and the
 * cross-coordinator invalidation-lag window that pure event-driven removal leaves open.</p>
 */
final class PlanCacheEntry {

    private final PlanResult plan;
    private final Set<String> referencedTables;
    private final Map<String, Long> tableVersions;
    private final long createdAtNanos;

    PlanCacheEntry(PlanResult plan, Set<String> referencedTables, Map<String, Long> tableVersions) {
        this.plan = Objects.requireNonNull(plan, "plan");
        this.referencedTables = Objects.requireNonNull(referencedTables, "referencedTables");
        this.tableVersions = Objects.requireNonNull(tableVersions, "tableVersions");
        this.createdAtNanos = System.nanoTime();
    }

    PlanResult plan() {
        return plan;
    }

    Set<String> referencedTables() {
        return referencedTables;
    }

    /** A-PLAN-01: version of each referenced table captured at plan time. */
    Map<String, Long> tableVersions() {
        return tableVersions;
    }

    long createdAtNanos() {
        return createdAtNanos;
    }
}
