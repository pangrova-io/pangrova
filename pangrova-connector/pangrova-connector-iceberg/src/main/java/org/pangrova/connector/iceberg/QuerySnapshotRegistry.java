package org.pangrova.connector.iceberg;

import java.util.Optional;

/**
 * Narrow interface for query-scoped snapshot pinning.
 *
 * <p>Extracted from {@link IcebergMetadata} (F-CONNECTOR-10) so that
 * {@link IcebergSplitManager} depends on this interface rather than
 * the full metadata implementation, eliminating the bidirectional
 * coupling between split planning and metadata management.</p>
 */
public interface QuerySnapshotRegistry {

    /**
     * Pins a snapshot for a given query, ensuring consistent reads
     * across multiple split-planning calls within the same query.
     */
    void pinSnapshotForQuery(String queryId, long snapshotId);

    /**
     * Pins a snapshot for a specific table within a query.
     */
    void pinSnapshotForQuery(String queryId, String schema, String table, long snapshotId);

    /**
     * Returns the pinned snapshot for a query (global pin).
     */
    Optional<Long> getPinnedSnapshot(String queryId);

    /**
     * Returns the pinned snapshot for a specific table within a query.
     */
    Optional<Long> getPinnedSnapshot(String queryId, String schema, String table);

    /**
     * Cleans up all pinned snapshots for a completed query.
     */
    void cleanupQuery(String queryId);
}
