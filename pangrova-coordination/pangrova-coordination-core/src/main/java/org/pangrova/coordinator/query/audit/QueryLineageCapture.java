/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.audit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.lineage.ColumnDerivation;
import org.pangrova.spi.observability.event.QueryLineageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Captures query lineage info (source/target tables) and column names from compiled plans.
 *
 * <p>Concurrency strategy: bounded ConcurrentHashMap (max 10000 entries) with
 * TTL-based eviction via ScheduledExecutorService. Entries not removed within
 * the TTL (e.g. query failure skipping cleanup) are automatically evicted to
 * prevent unbounded memory growth.</p>
 */
@ThreadSafe
public final class QueryLineageCapture {

    private static final Logger LOG = LoggerFactory.getLogger(QueryLineageCapture.class);

    static final int MAX_ENTRIES = 10_000;
    static final long DEFAULT_TTL_MS = 600_000L; // 10 minutes
    private static final long EVICTION_INTERVAL_MS = 60_000L; // 1 minute

    private final ConcurrentHashMap<String, TimestampedEntry<QueryLineageInfo>> lineageInfoMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TimestampedEntry<List<String>>> columnNamesMap =
            new ConcurrentHashMap<>();
    private final long ttlMs;

    private record TimestampedEntry<T>(T value, long createdAtMs) {}

    public QueryLineageCapture() {
        this.ttlMs = DEFAULT_TTL_MS;
    }

    /** Creates a bounded lineage cache with the specified entry time to live. */
    public QueryLineageCapture(long ttlMs) {
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("ttlMs must be positive, got: " + ttlMs);
        }
        this.ttlMs = ttlMs;
    }

    /**
     * Starts periodic eviction of expired entries.
     * Must be called once during bootstrap with a shared scheduler.
     */
    public void startEviction(ScheduledExecutorService scheduler) {
        scheduler.scheduleWithFixedDelay(
                this::evictExpired,
                EVICTION_INTERVAL_MS, EVICTION_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Captures column names for a query. Uses pre-optimization names when available
     * because HEP rules (e.g. PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW) replace SQL
     * aliases with synthetic names like w0$o0.
     */
    public void captureColumnNames(String queryId, List<String> preOptNames,
                                   PlannedQuery plannedQuery) {
        if (!checkCapacity("columnNames")) {
            return;
        }
        List<String> names = (preOptNames != null && !preOptNames.isEmpty())
                ? preOptNames
                : plannedQuery.columnNames();
        columnNamesMap.put(queryId, new TimestampedEntry<>(names, System.currentTimeMillis()));
    }

    /**
     * Captures lineage info from a compiled query plan.
     * Source tables come from table references (scans); write targets come from
     * the DML write target if present.
     */
    public org.pangrova.spi.observability.event.QueryLineageMetadata captureLineageInfo(
            String queryId, String sql, PlannedQuery plannedQuery) {
        List<String> sources = new ArrayList<>();
        List<String> targets = new ArrayList<>();
        for (List<String> ref : plannedQuery.tableReferences()) {
            var physical = org.pangrova.spi.connector.metadata.table.QualifiedTableName.resolve(ref);
            sources.add(plannedQuery.physicalToLogicalDynamicTables()
                    .getOrDefault(physical, physical).toString());
        }
        plannedQuery.dmlWriteTarget().ifPresent(t ->
                targets.add(String.join(".", t.qualifiedTableName())));
        List<ColumnDerivation> columnDerivations = extractColumnDerivations(plannedQuery);
        var info = new QueryLineageInfo(sql, sources, targets, columnDerivations);
        if (checkCapacity("lineageInfo")) {
            lineageInfoMap.put(queryId, new TimestampedEntry<>(info, System.currentTimeMillis()));
        }
        return metadata(info);
    }

    private static QueryLineageMetadata metadata(QueryLineageInfo info) {
        var inputs = info.sourceTables().stream().map(name ->
                new QueryLineageMetadata.Dataset(
                        "pangrova", name, List.of())).toList();
        var columns = info.columnDerivations().stream().map(column -> {
            var kind = switch (column.expression()) {
                case DIRECT -> QueryLineageMetadata.DerivationKind.DIRECT;
                case TRANSFORM -> QueryLineageMetadata.DerivationKind.TRANSFORM;
                case AGGREGATE -> QueryLineageMetadata.DerivationKind.AGGREGATE;
            };
            return new QueryLineageMetadata.Column(column.outputColumn(),
                    column.sourceColumns().stream().map(source ->
                            new QueryLineageMetadata.SourceColumn(
                                    "pangrova", source.table(), source.column())).toList(), kind);
        }).toList();
        var outputs = info.targetTables().stream().map(name ->
                new QueryLineageMetadata.Dataset(
                        "pangrova", name, columns)).toList();
        return new QueryLineageMetadata(inputs, outputs);
    }

    private List<ColumnDerivation> extractColumnDerivations(PlannedQuery plannedQuery) {
        return plannedQuery.columnLineage();
    }

    /** Returns and removes captured lineage info for a query. */
    public QueryLineageInfo removeLineageInfo(String queryId) {
        TimestampedEntry<QueryLineageInfo> entry = lineageInfoMap.remove(queryId);
        return entry != null ? entry.value() : null;
    }

    /** Returns captured lineage info without removing it. */
    public QueryLineageInfo peekLineageInfo(String queryId) {
        TimestampedEntry<QueryLineageInfo> entry = lineageInfoMap.get(queryId);
        return entry != null ? entry.value() : null;
    }

    /** Returns and removes captured column names for a query. */
    public List<String> removeColumnNames(String queryId) {
        TimestampedEntry<List<String>> entry = columnNamesMap.remove(queryId);
        return entry != null ? entry.value() : null;
    }

    /** Current number of tracked lineage entries (for monitoring). */
    public int lineageSize() {
        return lineageInfoMap.size();
    }

    /** Current number of tracked column name entries (for monitoring). */
    public int columnNamesSize() {
        return columnNamesMap.size();
    }

    private boolean checkCapacity(String mapName) {
        int totalSize = lineageInfoMap.size() + columnNamesMap.size();
        if (totalSize >= MAX_ENTRIES) {
            LOG.warn("event=lineage_capture_full map={} size={} maxEntries={}",
                    mapName, totalSize, MAX_ENTRIES);
            return false;
        }
        return true;
    }

    private void evictExpired() {
        long now = System.currentTimeMillis();
        int evicted = 0;
        evicted += evictMap(lineageInfoMap, now);
        evicted += evictMap(columnNamesMap, now);
        if (evicted > 0) {
            LOG.info("event=lineage_ttl_eviction evictedEntries={} remainingLineage={} remainingColumns={}",
                    evicted, lineageInfoMap.size(), columnNamesMap.size());
        }
    }

    private <T> int evictMap(ConcurrentHashMap<String, TimestampedEntry<T>> map, long now) {
        int count = 0;
        var it = map.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (now - entry.getValue().createdAtMs() > ttlMs) {
                it.remove();
                count++;
            }
        }
        return count;
    }
}
