package org.pangrova.spi.connector.index;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

/**
 * SPI for managing snapshot pins that prevent compaction from deleting
 * snapshots still referenced by active indexes.
 *
 * <p>When an index is built against a table snapshot, the snapshot must be
 * pinned to ensure compaction does not remove the underlying data files.
 * Connectors implement this interface using their native pinning mechanism
 * (e.g., Iceberg branch refs, Paimon tags).</p>
 *
 * <p>Lifecycle: {@link #pinSnapshot} must succeed before the builder reads source
 * files or publishes an index as ready; {@link #unpinSnapshot} is called when its retained snapshot is
 * garbage-collected. Operations should be idempotent for crash retries.</p>
 */
@Experimental
public interface SnapshotPinProvider {

    /**
     * Pins a snapshot so that compaction cannot expire it while the index
     * references it.
     *
     * @param catalogName    catalog containing the table
     * @param schemaName     schema containing the table
     * @param tableName      table whose snapshot should be pinned
     * @param snapshotId     snapshot identifier to pin
     * @param pinnedByIndexId the index that requires this snapshot
     * @throws SnapshotExpiredException if the snapshot has already expired
     */
    void pinSnapshot(String catalogName,
                     String schemaName,
                     String tableName,
                     long snapshotId,
                     String pinnedByIndexId);

    /**
     * Unpins a snapshot previously pinned by the given index, allowing
     * compaction to reclaim it if no other pins remain.
     *
     * @param catalogName    catalog containing the table
     * @param schemaName     schema containing the table
     * @param tableName      table whose snapshot should be unpinned
     * @param snapshotId     snapshot identifier to unpin
     * @param pinnedByIndexId the index releasing this snapshot
     */
    void unpinSnapshot(String catalogName,
                       String schemaName,
                       String tableName,
                       long snapshotId,
                       String pinnedByIndexId);

    /**
     * Returns all snapshot identifiers currently pinned for the given table.
     *
     * @param catalogName catalog containing the table
     * @param schemaName  schema containing the table
     * @param tableName   table to query
     * @return list of pinned snapshot identifiers; empty if none are pinned
     */
    List<Long> getPinnedSnapshots(String catalogName,
                                  String schemaName,
                                  String tableName);

    /** Enumerates every durable snapshot ref for one exact index, including retired generations. */
    default List<Long> getPinnedSnapshotsForIndex(String catalogName, String schemaName,
                                                  String tableName, String indexId) {
        throw new UnsupportedOperationException("Per-index snapshot pin enumeration is unavailable");
    }
}
