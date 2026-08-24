package org.pangrova.connector.iceberg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages query-scoped snapshot pinning for the Iceberg connector.
 *
 * <p>Pinned snapshot IDs are tracked per (queryId, table) with a creation
 * timestamp for TTL eviction. When a query begins, the current snapshot is
 * recorded so that all reads within the query see a consistent view, even if
 * compaction or new commits occur concurrently. Using a compound key ensures
 * each table within a multi-table query gets its own pinned snapshot
 * (F-CONNECTOR-08). Entries not cleaned up within the TTL (e.g. due to query
 * failure skipping cleanupQuery) are automatically evicted.</p>
 */
final class IcebergSnapshotPinManager implements QuerySnapshotRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSnapshotPinManager.class);

    private static final long PIN_EVICTION_INTERVAL_MS = 60_000L; // 1 minute

    private final String catalogName;
    private final long pinTtlMs;
    private final Runnable cleanupHook;
    private final ConcurrentHashMap<String, PinnedSnapshot> pinnedSnapshots = new ConcurrentHashMap<>();

    /**
     * Tracks queries that have been formally begun via {@link #beginQuery(String)}.
     * Only queries in this set are eligible for snapshot pinning.
     */
    private final Set<String> activeQueryIds = ConcurrentHashMap.newKeySet();

    IcebergSnapshotPinManager(String catalogName, long pinTtlMs, Runnable cleanupHook) {
        this.catalogName = catalogName;
        this.pinTtlMs = pinTtlMs;
        this.cleanupHook = cleanupHook;
    }

    /**
     * Starts periodic eviction of expired pinned snapshots.
     * Must be called once during connector initialization with a shared scheduler.
     */
    void startPinEviction(ScheduledExecutorService scheduler) {
        scheduler.scheduleWithFixedDelay(
                this::evictExpiredPins,
                PIN_EVICTION_INTERVAL_MS, PIN_EVICTION_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** Registers a query so that subsequent pin requests are accepted. */
    void beginQuery(String queryId) {
        activeQueryIds.add(queryId);
        LOG.debug("event=begin_query queryId={} catalog={}", queryId, catalogName);
    }

    @Override
    public void pinSnapshotForQuery(String queryId, long snapshotId) {
        pinSnapshotForQuery(queryId, null, null, snapshotId);
    }

    @Override
    public void pinSnapshotForQuery(String queryId, String schema, String table, long snapshotId) {
        if (!activeQueryIds.contains(queryId)) {
            LOG.debug("event=pin_snapshot_skipped queryId={} reason=query_not_begun", queryId);
            return;
        }
        String key = (schema != null && table != null) ? pinKey(queryId, schema, table) : queryId;
        pinnedSnapshots.compute(key, (k, existing) -> {
            if (existing == null) {
                LOG.debug("event=pin_snapshot queryId={} catalog={} table={}.{} snapshotId={}",
                        queryId, catalogName, schema, table, snapshotId);
                return new PinnedSnapshot(snapshotId, System.currentTimeMillis());
            }
            if (existing.snapshotId() == -1L) {
                LOG.debug("event=pin_real_snapshot queryId={} catalog={} snapshotId={}",
                        queryId, catalogName, snapshotId);
                return new PinnedSnapshot(snapshotId, existing.pinnedAtMs());
            }
            return existing;
        });
    }

    @Override
    public void cleanupQuery(String queryId) {
        activeQueryIds.remove(queryId);
        // Remove all entries for this query (compound keys start with queryId|)
        int removed = 0;
        var it = pinnedSnapshots.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            if (key.equals(queryId) || key.startsWith(queryId + "|")) {
                it.remove();
                removed++;
            }
        }
        if (cleanupHook != null) {
            cleanupHook.run();
        }
        LOG.debug("event=cleanup_query queryId={} catalog={} removedPins={}",
                queryId, catalogName, removed);
    }

    @Override
    public Optional<Long> getPinnedSnapshot(String queryId) {
        return getPinnedSnapshot(queryId, null, null);
    }

    @Override
    public Optional<Long> getPinnedSnapshot(String queryId, String schema, String table) {
        String key = (schema != null && table != null) ? pinKey(queryId, schema, table) : queryId;
        PinnedSnapshot pin = pinnedSnapshots.get(key);
        if (pin == null || pin.snapshotId() == -1L) {
            return Optional.empty();
        }
        return Optional.of(pin.snapshotId());
    }

    private void evictExpiredPins() {
        long now = System.currentTimeMillis();
        int evicted = 0;
        var it = pinnedSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (now - entry.getValue().pinnedAtMs() > pinTtlMs) {
                it.remove();
                evicted++;
                LOG.debug("event=pinned_snapshot_evicted queryId={} catalog={} ageMs={}",
                        entry.getKey(), catalogName, now - entry.getValue().pinnedAtMs());
            }
        }
        if (evicted > 0) {
            LOG.info("event=pinned_snapshot_ttl_eviction catalog={} evictedEntries={} remaining={}",
                    catalogName, evicted, pinnedSnapshots.size());
        }
    }

    /** Compound key for per-table snapshot pinning within a query. */
    private static String pinKey(String queryId, String schema, String table) {
        return queryId + "|" + schema + "." + table;
    }

    private record PinnedSnapshot(long snapshotId, long pinnedAtMs) {}
}
