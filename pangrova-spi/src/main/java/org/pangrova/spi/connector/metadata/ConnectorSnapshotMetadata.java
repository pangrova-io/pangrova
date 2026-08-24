package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

import java.time.Instant;
import java.util.Optional;

import org.pangrova.spi.connector.ConnectorSession;

/**
 * Snapshot-related metadata operations for a connector.
 *
 * <p>Extracted from {@link ConnectorMetadata} per Interface Segregation Principle.
 * Connectors that do not support snapshot-based time travel need not implement
 * these methods beyond the default empty stubs.</p>
 */
@Experimental
public interface ConnectorSnapshotMetadata {

    /**
     * Resolves the commit timestamp of a specific snapshot by its ID.
     *
     * <p>Used by the coordinator to rewrite version-based time travel queries
     * into timestamp-based time travel. Returns empty for connectors that
     * do not support snapshot-based time travel — this is the permanent
     * contract behavior, not a placeholder.</p>
     *
     * @param session    the current session context
     * @param schemaName the schema containing the table
     * @param tableName  the table name
     * @param snapshotId the snapshot ID to resolve
     * @return the commit timestamp of the snapshot, or empty if not supported
     */
    default Optional<Instant> resolveSnapshotTimestamp(
            ConnectorSession session, String schemaName, String tableName, long snapshotId) {
        return Optional.empty();
    }
}
