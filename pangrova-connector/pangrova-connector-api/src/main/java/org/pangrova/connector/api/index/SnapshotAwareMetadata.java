package org.pangrova.connector.api.index;

import org.pangrova.connector.api.exception.SnapshotExpiredException;

import org.pangrova.connector.api.enums.SnapshotRelation;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * SPI for connectors that support snapshot-diff based incremental indexing.
 *
 * <p>Connectors implementing this interface enable efficient incremental builds
 * by providing the list of added/removed data files between two snapshots,
 * avoiding full table scans.</p>
 *
 * <p>Every operation is bound to an explicit immutable snapshot. Implementations
 * must fail closed if the requested snapshot cannot be read completely.</p>
 */
@Experimental
public interface SnapshotAwareMetadata {

    /** Returns the connector's current immutable snapshot token, if one exists. */
    Optional<String> currentSnapshotId();

    /** Selects the current snapshot and lists its files in the same connector observation. */
    Optional<CurrentSnapshotFiles> currentSnapshotFiles();

    /** A complete file set tied to the snapshot selected for this observation. */
    record CurrentSnapshotFiles(String snapshotId, List<SnapshotDataFile> files) {
        public CurrentSnapshotFiles {
            files = List.copyOf(files);
        }
    }

    /**
     * Lists the complete file set for an immutable snapshot.
     *
     * @param snapshotId immutable connector snapshot token
     * @return complete file identities and statistics for the snapshot
     * @throws SnapshotExpiredException if the snapshot is no longer available
     */
    List<SnapshotDataFile> listFilesAtSnapshot(String snapshotId);

    /**
     * Resolves the causal relationship between two snapshots without assuming
     * that snapshot tokens are numeric or lexically ordered.
     */
    SnapshotRelation relation(String leftSnapshotId, String rightSnapshotId);

    /**
     * Measures lineage and, for a forward update, reads both file inventories in
     * the same connector observation. The target inventory is the one used to
     * derive the delta. Other lineage outcomes do not open file inventories.
     */
    SnapshotAdvance observeAdvance(String fromSnapshotId, String toSnapshotId);

    /** Complete forward file evidence, or a lineage decision with no file reads. */
    record SnapshotAdvance(SnapshotRelation relation, List<SnapshotDataFile> targetFiles, SnapshotFileDelta delta) {
        public SnapshotAdvance {
            java.util.Objects.requireNonNull(relation, "relation");
            java.util.Objects.requireNonNull(delta, "delta");
            targetFiles = List.copyOf(targetFiles);
            if (relation != SnapshotRelation.LEFT_ANCESTOR
                    && !(targetFiles.isEmpty() && delta.added().isEmpty() && delta.removed().isEmpty())) {
                throw new IllegalArgumentException("Only a forward snapshot advance carries file evidence");
            }
        }

        /** Returns a lineage-only decision without opening either snapshot inventory. */
        public static SnapshotAdvance noAdvance(SnapshotRelation relation) {
            if (relation == SnapshotRelation.LEFT_ANCESTOR) {
                throw new IllegalArgumentException("A forward snapshot advance requires its file evidence");
            }
            return new SnapshotAdvance(relation, List.of(), new SnapshotFileDelta(List.of(), List.of()));
        }
    }

    /**
     * Computes one complete file delta between immutable snapshots.
     *
     * @param fromSnapshotId the starting snapshot (exclusive); {@code null} means from the beginning
     * @param toSnapshotId   the ending snapshot (inclusive)
     * @return added file identities and removed file paths
     * @throws SnapshotExpiredException if either snapshot is no longer available
     */
    SnapshotFileDelta diffFiles(String fromSnapshotId, String toSnapshotId);
}
