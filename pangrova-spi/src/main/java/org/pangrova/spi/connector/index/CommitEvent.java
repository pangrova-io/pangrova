package org.pangrova.spi.connector.index;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Immutable event representing a commit in a connector's table storage.
 *
 * <p>Carries the list of added and removed data files so that the indexing
 * module can incrementally maintain indexes.
 *
 * @param catalogName     catalog name (must not be blank)
 * @param schemaName      schema name (must not be blank)
 * @param tableName       table name (must not be blank)
 * @param addedFiles      data files added in this commit (defensively copied; null → empty)
 * @param removedFiles    data files removed in this commit (defensively copied; null → empty)
 * @param commitTimestamp timestamp of the commit
 * @param snapshotId      connector snapshot identifier for this commit
 * @param eventType       type of commit operation (APPEND, OVERWRITE, COMPACT, DELETE)
 */
public record CommitEvent(
        String catalogName,
        String schemaName,
        String tableName,
        List<String> addedFiles,
        List<String> removedFiles,
        Instant commitTimestamp,
        String snapshotId,
        CommitEventType eventType
) {

    public CommitEvent {
        requireName(catalogName, "catalogName");
        requireName(schemaName, "schemaName");
        requireName(tableName, "tableName");
        Objects.requireNonNull(commitTimestamp, "commitTimestamp must not be null");
        Objects.requireNonNull(snapshotId, "snapshotId must not be null");
        addedFiles = addedFiles == null ? List.of() : List.copyOf(addedFiles);
        removedFiles = removedFiles == null ? List.of() : List.copyOf(removedFiles);
        if (eventType == null) {
            eventType = CommitEventType.APPEND;
        }
    }

    private static void requireName(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    /**
     * Backward-compatible factory: creates an APPEND event (default for existing callers).
     */
    public CommitEvent(
            String catalogName, String schemaName, String tableName,
            List<String> addedFiles, List<String> removedFiles,
            Instant commitTimestamp, String snapshotId) {
        this(catalogName, schemaName, tableName, addedFiles, removedFiles,
                commitTimestamp, snapshotId, CommitEventType.APPEND);
    }
}
