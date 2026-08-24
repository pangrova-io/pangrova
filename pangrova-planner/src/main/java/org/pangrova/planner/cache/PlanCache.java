package org.pangrova.planner.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.pangrova.planner.PlanResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded LRU cache for optimized {@link PlanResult}s, keyed by normalized SQL +
 * session-properties digest ({@link PlanCacheKey}).
 *
 * <p>Goal: skip the parse → validate → optimize pipeline for queries that have
 * already been planned. The optimized RelNode tree is reused as-is for Substrait
 * conversion. RelNodes in Calcite are immutable once produced by rules, so reading
 * the cached tree from multiple threads is safe.</p>
 *
 * <h2>Invalidation</h2>
 * <ul>
 *   <li>{@link #invalidateByTable(String)} — drop entries whose plan references
 *       the named table. Called when DDL/DML mutates the table's schema or
 *       its statistics change materially. Matching is exact on the
 *       {@code catalog.schema.table} qualified name (lowercase).</li>
 *   <li>{@link #invalidateAll()} — clear everything. Used as a conservative
 *       fallback for events whose impact cannot be scoped (e.g. catalog dropped,
 *       full metadata sync).</li>
 * </ul>
 *
 * <p>Thread safety: backed by Caffeine which uses internal segmented locking
 * for near-zero contention on reads. All public methods are safe for
 * concurrent access without external synchronization.</p>
 *
 * <p>Memory safety: bounded by {@link PlanCacheConfig#maxEntries()}; the LRU
 * evicts the least-recently-accessed entry when the cap is exceeded. There is
 * no TTL — staleness is driven by the invalidation listeners, not the clock,
 * because the cost of a stale plan (potentially incorrect schema) outweighs
 * the cost of re-planning.</p>
 */
public class PlanCache {

    private static final Logger LOG = LoggerFactory.getLogger(PlanCache.class);

    private final PlanCacheConfig config;
    private final Cache<PlanCacheKey, PlanCacheEntry> entries;
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong invalidationCount = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();

    /**
     * A-PLAN-01: monotonic per-table version, bumped by {@link #invalidateByTable}. Entries capture
     * these at {@link #put} time; {@link #get} evicts any entry whose referenced table's version has
     * advanced since — a hard staleness gate independent of the eager entry removal.
     */
    private final ConcurrentHashMap<String, Long> tableVersions = new ConcurrentHashMap<>();

    public PlanCache() {
        this(PlanCacheConfig.DEFAULT);
    }

    /**
     * Creates a plan cache backed by Caffeine with the given configuration.
     *
     * @param config cache sizing and eviction settings
     */
    // PERF-T7: Replaced Collections.synchronizedMap(LinkedHashMap) with Caffeine.
    // Caffeine uses lock-free reads and segmented writes — eliminates the global
    // synchronized bottleneck under high concurrent query planning load.
    public PlanCache(PlanCacheConfig config) {
        this.config = config;
        this.entries = Caffeine.newBuilder()
                .maximumSize(config.maxEntries())
                .evictionListener((PlanCacheKey key, PlanCacheEntry value, RemovalCause cause) -> {
                    if (cause.wasEvicted()) {
                        evictionCount.incrementAndGet();
                    }
                })
                .build();
    }

    /** Look up a cached plan. Increments hit/miss counters. */
    public Optional<PlanResult> get(PlanCacheKey key) {
        if (!config.enabled()) {
            return Optional.empty();
        }
        PlanCacheEntry entry = entries.getIfPresent(key);
        if (entry == null) {
            missCount.incrementAndGet();
            return Optional.empty();
        }
        // A-PLAN-01: validate-on-hit. Evict if any referenced table's version advanced since the
        // plan was cached (catches the put/invalidate race and any entry that survived eager
        // invalidation), so a stale-schema plan is never returned.
        if (isStale(entry)) {
            entries.invalidate(key);
            invalidationCount.incrementAndGet();
            missCount.incrementAndGet();
            LOG.info("event=plan_cache_stale_evict_on_hit tables={}", entry.referencedTables());
            return Optional.empty();
        }
        hitCount.incrementAndGet();
        return Optional.of(entry.plan());
    }

    /** A-PLAN-01: true if any referenced table's current version differs from the plan-time version. */
    private boolean isStale(PlanCacheEntry entry) {
        for (Map.Entry<String, Long> v : entry.tableVersions().entrySet()) {
            if (currentVersion(v.getKey()) != v.getValue()) {
                return true;
            }
        }
        return false;
    }

    private long currentVersion(String table) {
        return tableVersions.getOrDefault(table, 0L);
    }

    /**
     * Cache a freshly-optimized plan. The plan is detached from the planner's
     * cluster before caching to prevent shared mutable state races (F-PLANNER-10).
     * The set of referenced tables is extracted from the RelNode tree's
     * {@link TableScan} nodes for fine-grained invalidation.
     */
    public void put(PlanCacheKey key, PlanResult plan) {
        if (!config.enabled() || config.maxEntries() == 0) {
            return;
        }
        Set<String> referencedTables = extractReferencedTables(plan.plan());
        // A-PLAN-01: snapshot each referenced table's current version at plan time so a later hit
        // can detect an intervening invalidation (validate-on-hit).
        Map<String, Long> versionsAtPlanTime = new HashMap<>();
        for (String table : referencedTables) {
            versionsAtPlanTime.put(table, currentVersion(table));
        }
        // F-PLANNER-10: Create a deep copy of the plan to detach it from the
        // VolcanoPlanner's shared cluster state. This prevents data races when
        // a cached plan's cluster is reused by a subsequent query on the same thread.
        PlanResult detachedPlan = plan.withDetachedPlan();
        entries.put(key, new PlanCacheEntry(detachedPlan, referencedTables, versionsAtPlanTime));
    }

    /**
     * Drop entries whose plan references {@code tableName} (case-insensitive,
     * matched against the qualified {@code catalog.schema.table} string).
     */
    public void invalidateByTable(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return;
        }
        String lower = tableName.toLowerCase();
        // A-PLAN-01: bump the monotonic version BEFORE the scan below, so a concurrent put() that
        // captured the old version is caught by validate-on-hit even if it stores its entry after
        // this scan has already passed (the put/invalidate TOCTOU race).
        markTableChanged(lower);
        // PERF-T7: Caffeine's asMap() is a concurrent view — no external sync needed.
        List<PlanCacheKey> toRemove = new ArrayList<>();
        for (var e : entries.asMap().entrySet()) {
            if (e.getValue().referencedTables().contains(lower)
                    || matchesAnyTableSuffix(e.getValue().referencedTables(), lower)) {
                toRemove.add(e.getKey());
            }
        }
        if (!toRemove.isEmpty()) {
            entries.invalidateAll(toRemove);
            invalidationCount.addAndGet(toRemove.size());
            LOG.info("event=plan_cache_invalidate_by_table table={} entriesRemoved={}", tableName, toRemove.size());
        }
    }

    /**
     * A-PLAN-01: marks a table changed by bumping its monotonic version WITHOUT scanning entries.
     * Cached plans referencing it are then lazily evicted on their next {@link #get}
     * (validate-on-hit) — an O(1) alternative to {@link #invalidateByTable} suitable for
     * high-frequency or cross-coordinator version-propagation signals where an immediate scan is
     * undesirable.
     */
    public void markTableChanged(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return;
        }
        tableVersions.merge(tableName.toLowerCase(), 1L, Long::sum);
    }

    /** Clear all entries (used for catalog drop, full metadata sync, etc.). */
    public void invalidateAll() {
        long size = entries.estimatedSize();
        entries.invalidateAll();
        if (size > 0) {
            invalidationCount.addAndGet(size);
            LOG.info("event=plan_cache_invalidate_all entriesRemoved={}", size);
        }
    }

    public long hitCount() {
        return hitCount.get();
    }

    public long missCount() {
        return missCount.get();
    }

    public long invalidationCount() {
        return invalidationCount.get();
    }

    public long evictionCount() {
        return evictionCount.get();
    }

    public int size() {
        entries.cleanUp();
        return (int) entries.estimatedSize();
    }

    public PlanCacheConfig config() {
        return config;
    }

    /** Walks the RelNode tree and collects qualified table names from TableScan nodes. */
    static Set<String> extractReferencedTables(RelNode root) {
        Set<String> tables = new HashSet<>();
        collectTables(root, tables);
        return Collections.unmodifiableSet(tables);
    }

    private static void collectTables(RelNode node, Set<String> tables) {
        if (node instanceof TableScan scan) {
            List<String> qualifiedName = scan.getTable().getQualifiedName();
            if (!qualifiedName.isEmpty()) {
                String full = String.join(".", qualifiedName).toLowerCase();
                tables.add(full);
                // Also add the bare table name (last segment) so invalidations that
                // arrive without a fully-qualified name still match.
                tables.add(qualifiedName.get(qualifiedName.size() - 1).toLowerCase());
            }
        }
        for (RelNode input : node.getInputs()) {
            collectTables(input, tables);
        }
    }

    /**
     * Allows invalidation by a partial qualifier. For example, an event that says
     * "table 'orders' changed" should invalidate plans referencing
     * "iceberg_catalog.default.orders". We treat the lookup as matching when any
     * referenced table ends with ".{tableName}" (suffix match on dotted segments).
     */
    private static boolean matchesAnyTableSuffix(Set<String> referencedTables, String tableName) {
        String suffix = "." + tableName;
        for (String t : referencedTables) {
            if (t.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }
}
