package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.ConnectorSession;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-query pinned snapshot IDs so that all reads within a query see a
 * consistent table snapshot, even if compaction or new commits occur concurrently.
 */
final class PaimonSnapshotPins {

    /** Sentinel recorded at query start before the real snapshot is pinned. */
    private static final long UNPINNED = -1L;

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSnapshotPins.class);

    private final String catalogName;
    private final ConcurrentHashMap<String, Long> pinnedSnapshots = new ConcurrentHashMap<>();

    PaimonSnapshotPins(String catalogName) {
        this.catalogName = catalogName;
    }

    void beginQuery(String queryId) {
        pinnedSnapshots.put(queryId, UNPINNED);
        LOG.debug("event=begin_query queryId={} catalog={}", queryId, catalogName);
    }

    /** Removes the pin for the query, returning the removed snapshot ID or null. */
    Long remove(String queryId) {
        return pinnedSnapshots.remove(queryId);
    }

    /** Returns the pinned snapshot ID for the query, or empty if none pinned. */
    Optional<Long> getPinned(String queryId) {
        Long snapshotId = pinnedSnapshots.get(queryId);
        if (snapshotId == null || snapshotId == UNPINNED) {
            return Optional.empty();
        }
        return Optional.of(snapshotId);
    }

    /**
     * Pins the real snapshot ID for a query, replacing the sentinel with the
     * actual snapshot ID. If the query already has a real snapshot pinned,
     * this is a no-op to preserve the first-access snapshot consistency
     * guarantee.
     */
    void pin(String queryId, long snapshotId) {
        pinnedSnapshots.computeIfPresent(queryId, (key, existing) -> {
            if (existing == UNPINNED) {
                LOG.debug("event=pin_real_snapshot queryId={} catalog={} snapshotId={}",
                        queryId, catalogName, snapshotId);
                return snapshotId;
            }
            return existing;
        });
    }

    /** Pins the table's latest snapshot for the query carried by the session. */
    void pinLatestForQuery(ConnectorSession session, Table table) {
        if (!(table instanceof FileStoreTable fst)) {
            return;
        }
        Long snapshotId = fst.snapshotManager().latestSnapshotId();
        if (snapshotId == null) {
            return;
        }
        String queryId = session.getProperties().get("queryId");
        if (queryId != null) {
            pin(queryId, snapshotId);
        }
    }
}
