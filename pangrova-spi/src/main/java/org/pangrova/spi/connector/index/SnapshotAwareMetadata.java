package org.pangrova.spi.connector.index;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

/**
 * SPI for connectors that support snapshot-diff based incremental indexing.
 *
 * <p>Connectors implementing this interface enable efficient incremental builds
 * by providing the list of added/removed data files between two snapshots,
 * avoiding full table scans.</p>
 *
 * <p>If a connector does not support snapshot-diff (e.g., the snapshot has expired),
 * the indexing system falls back to CommitEvent-based incremental builds.</p>
 */
@Experimental
public interface SnapshotAwareMetadata {

    /**
     * Lists data file paths added between two snapshots.
     *
     * @param fromSnapshotId the starting snapshot (exclusive); {@code null} means from the beginning
     * @param toSnapshotId   the ending snapshot (inclusive)
     * @return list of added data file paths; empty if no additions
     * @throws SnapshotExpiredException if {@code fromSnapshotId} is no longer available
     */
    List<String> listAddedFilesBetween(String fromSnapshotId, String toSnapshotId);

    /**
     * Lists data file paths removed between two snapshots.
     *
     * @param fromSnapshotId the starting snapshot (exclusive); {@code null} means from the beginning
     * @param toSnapshotId   the ending snapshot (inclusive)
     * @return list of removed data file paths; empty if no removals
     * @throws SnapshotExpiredException if {@code fromSnapshotId} is no longer available
     */
    List<String> listRemovedFilesBetween(String fromSnapshotId, String toSnapshotId);
}
