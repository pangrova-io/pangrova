package org.pangrova.coordinator.query.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.query.statement.TableWarmupExecutor;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.types.value.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator-side service for proactive cache warmup and hot-table pinning
 * (G-CACHE-06).
 *
 * <p>Provides two complementary capabilities:
 * <ol>
 *   <li><b>Warmup:</b> enumerates a table's data files (splits) through the
 *       connector metadata path and reads each split through the page-source
 *       pipeline, populating connector-side metadata caches and the OS page
 *       cache. Delegates to {@link TableWarmupExecutor} for the actual I/O.</li>
 *   <li><b>Pin:</b> registers a {@link CachePinPolicy} for a table so that
 *       subsequent scheduling decisions and cache eviction policies on workers
 *       can give preferential treatment to pinned tables.</li>
 * </ol>
 *
 * <h3>Session property integration</h3>
 * <p>Users trigger warmup via the session property
 * {@code cache_warmup_tables}:
 * <pre>
 *   SET SESSION cache_warmup_tables = 'catalog.schema.table1,catalog.schema.table2'
 * </pre>
 * When the coordinator processes this property (at session start or via a SET
 * command), it calls {@link #warmupTables(String, QuerySession)} which parses the
 * comma-separated list and warms each table in sequence. Tables that fail
 * warmup are logged but do not prevent subsequent tables from being warmed.</p>
 *
 * <h3>Pin registry</h3>
 * <p>Pinned tables are tracked in an in-memory concurrent map. The pin
 * registry is coordinator-local (not replicated across coordinators) since
 * pin state is advisory and loss on coordinator restart is acceptable — the
 * user simply re-issues the SET SESSION.</p>
 *
 * <p>Equivalent to Spark's {@code CACHE TABLE} / Trino's {@code CACHE SELECT}.</p>
 *
 * @see TableWarmupExecutor
 * @see CachePinPolicy
 */
public final class CacheWarmupService {

    /** Session property name for declaring tables to warm up. */
    public static final String SESSION_PROPERTY = "cache_warmup_tables";

    private static final Logger LOG = LoggerFactory.getLogger(CacheWarmupService.class);

    private final TableWarmupExecutor warmupExecutor;
    private final ConcurrentHashMap<String, CachePinPolicy> pinnedTables;

    public CacheWarmupService(CatalogRegistry catalogRegistry) {
        this(new TableWarmupExecutor(catalogRegistry));
    }

    /** Test-friendly constructor accepting a pre-built executor. */
    public CacheWarmupService(TableWarmupExecutor warmupExecutor) {
        this.warmupExecutor = warmupExecutor;
        this.pinnedTables = new ConcurrentHashMap<>();
    }

    /**
     * Parses a comma-separated list of fully-qualified table references
     * ({@code catalog.schema.table}) and warms each table.
     *
     * <p>Each table is warmed independently. Failures on individual tables
     * are logged and counted but do not abort the remaining warmups.</p>
     *
     * @param tableList comma-separated FQN table references
     * @return summary of warmup results (one entry per table attempted)
     */
    public List<WarmupResult> warmupTables(String tableList, QuerySession context) {
        if (tableList == null || tableList.isBlank()) {
            return List.of();
        }
        String[] tables = tableList.split(",");
        List<WarmupResult> results = new ArrayList<>(tables.length);
        for (String raw : tables) {
            String fqn = raw.strip();
            if (fqn.isEmpty()) {
                continue;
            }
            results.add(warmupSingleTable(fqn, context));
        }
        return Collections.unmodifiableList(results);
    }

    /**
     * Warms a single table identified by its fully-qualified name
     * ({@code catalog.schema.table}).
     *
     * @param fullyQualifiedName e.g. "iceberg_catalog.analytics.orders"
     * @return the warmup result
     */
    public WarmupResult warmupSingleTable(String fullyQualifiedName, QuerySession context) {
        String[] parts = parseFullyQualifiedName(fullyQualifiedName);
        if (parts == null) {
            LOG.warn("event=cache_warmup_invalid_fqn fqn={}", fullyQualifiedName);
            return WarmupResult.failure(fullyQualifiedName,
                    "Invalid table reference — expected 'catalog.schema.table'");
        }
        String catalog = parts[0];
        String schema = parts[1];
        String table = parts[2];

        try {
            List<TypedRow> rows = warmupExecutor.execute(
                    new org.pangrova.connector.api.metadata.table.QualifiedTableName(catalog, schema, table), context);
            TypedRow summary = rows.get(0);
            long splitsProcessed = summary.getLong("splits_processed");
            long splitsFailed = summary.getLong("splits_failed");
            long rowsTouched = summary.getLong("rows_touched");
            long bytesPrefetched = summary.getLong("bytes_prefetched");
            long durationMs = summary.getLong("duration_ms");

            LOG.info("event=cache_warmup_completed table={} splits={} rows={} "
                            + "bytes={} duration_ms={}",
                    fullyQualifiedName, splitsProcessed, rowsTouched,
                    bytesPrefetched, durationMs);

            return new WarmupResult(fullyQualifiedName, true, null,
                    splitsProcessed, splitsFailed, rowsTouched,
                    bytesPrefetched, durationMs);
        } catch (Exception e) {
            LOG.warn("event=cache_warmup_failed table={} error={}",
                    fullyQualifiedName, e.getMessage());
            return WarmupResult.failure(fullyQualifiedName, e.getMessage());
        }
    }

    /**
     * Registers a pin policy for a table. Replaces any existing pin.
     *
     * @param fullyQualifiedName e.g. "iceberg_catalog.analytics.orders"
     * @param policy the pin policy to apply
     */
    public void pinTable(String fullyQualifiedName, CachePinPolicy policy) {
        if (fullyQualifiedName == null || fullyQualifiedName.isBlank()) {
            return;
        }
        String normalized = fullyQualifiedName.strip().toLowerCase();
        if (policy == CachePinPolicy.NONE) {
            pinnedTables.remove(normalized);
            LOG.info("event=cache_unpin table={}", normalized);
        } else {
            pinnedTables.put(normalized, policy);
            LOG.info("event=cache_pin table={} policy={}", normalized, policy);
        }
    }

    /**
     * Removes the pin for a table (equivalent to
     * {@code pinTable(name, CachePinPolicy.NONE)}).
     */
    public void unpinTable(String fullyQualifiedName) {
        pinTable(fullyQualifiedName, CachePinPolicy.NONE);
    }

    /**
     * Returns the current pin policy for a table, or {@link CachePinPolicy#NONE}
     * if the table is not pinned.
     */
    public CachePinPolicy getPinPolicy(String fullyQualifiedName) {
        if (fullyQualifiedName == null) {
            return CachePinPolicy.NONE;
        }
        return pinnedTables.getOrDefault(
                fullyQualifiedName.strip().toLowerCase(), CachePinPolicy.NONE);
    }

    /**
     * Returns an unmodifiable snapshot of all currently pinned tables and
     * their policies.
     */
    public Map<String, CachePinPolicy> pinnedTableSnapshot() {
        return Map.copyOf(pinnedTables);
    }

    /**
     * Returns the set of pinned table names (lower-cased FQN).
     * Useful for the scheduler to check whether a split belongs to a
     * pinned table.
     */
    public Set<String> pinnedTableNames() {
        return Collections.unmodifiableSet(pinnedTables.keySet());
    }

    /**
     * Returns true if any table is currently pinned.
     */
    public boolean hasPinnedTables() {
        return !pinnedTables.isEmpty();
    }

    /**
     * Parses a warmup session property value and also pins each referenced
     * table with {@link CachePinPolicy#HOT_TABLE}.
     *
     * <p>This is the main entry point for session-property-driven warmup:
     * the coordinator calls this when it sees
     * {@code SET SESSION cache_warmup_tables = '...'}.
     *
     * @param tableList comma-separated FQN table references
     * @return warmup results
     */
    public List<WarmupResult> warmupAndPinTables(String tableList, QuerySession context) {
        List<WarmupResult> results = warmupTables(tableList, context);
        // Pin all successfully warmed tables.
        for (WarmupResult r : results) {
            if (r.success()) {
                pinTable(r.tableName(), CachePinPolicy.HOT_TABLE);
            }
        }
        return results;
    }

    /**
     * Parses "catalog.schema.table" into [catalog, schema, table].
     * Returns null if the format is invalid.
     */
    static String[] parseFullyQualifiedName(String fqn) {
        if (fqn == null || fqn.isBlank()) {
            return null;
        }
        String trimmed = fqn.strip();
        int firstDot = trimmed.indexOf('.');
        if (firstDot <= 0) {
            return null;
        }
        int secondDot = trimmed.indexOf('.', firstDot + 1);
        if (secondDot <= firstDot + 1 || secondDot >= trimmed.length() - 1) {
            return null;
        }
        // Ensure no third dot (only catalog.schema.table).
        if (trimmed.indexOf('.', secondDot + 1) >= 0) {
            return null;
        }
        return new String[]{
                trimmed.substring(0, firstDot),
                trimmed.substring(firstDot + 1, secondDot),
                trimmed.substring(secondDot + 1)
        };
    }

    /**
     * Immutable result of a single table warmup attempt.
     */
    public record WarmupResult(
            String tableName,
            boolean success,
            String errorMessage,
            long splitsProcessed,
            long splitsFailed,
            long rowsTouched,
            long bytesPrefetched,
            long durationMs
    ) {
        /** Factory for a failed warmup. */
        public static WarmupResult failure(String tableName, String errorMessage) {
            return new WarmupResult(tableName, false, errorMessage, 0, 0, 0, 0, 0);
        }
    }
}
