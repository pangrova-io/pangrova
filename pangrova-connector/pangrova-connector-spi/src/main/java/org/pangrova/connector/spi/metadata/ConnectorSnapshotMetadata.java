package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;

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
     * Lists named references from catalog metadata for control-plane maintenance.
     * This is not a system-table row scan. Connectors without this capability
     * must fail explicitly so maintenance cannot silently skip supported tables.
     */
    default List<SnapshotReference> listSnapshotReferences(
            ConnectorSession session, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("listSnapshotReferences not supported");
    }

    /**
     * Removes a non-main branch only if it still points to the observed snapshot.
     * A conflict or unknown commit state must not trigger an automatic replay;
     * maintenance must re-evaluate fresh catalog metadata on a later cycle.
     */
    default ConditionalBranchDropResult dropBranchIfSnapshotMatches(
            ConnectorSession session, ConnectorTableHandle table,
            String branchName, long expectedSnapshotId) {
        throw new UnsupportedOperationException("dropBranchIfSnapshotMatches not supported");
    }

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
