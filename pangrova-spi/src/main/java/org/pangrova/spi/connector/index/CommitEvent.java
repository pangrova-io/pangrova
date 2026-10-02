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
 * @param tenantId       authenticated tenant identity (must not be blank)
 * @param catalogName     catalog name (must not be blank)
 * @param schemaName      schema name (must not be blank)
 * @param tableName       table name (must not be blank)
 * @param addedFiles      data files added in this commit (defensively copied; null → empty)
 * @param removedFiles    data files removed in this commit (defensively copied; null → empty)
 * @param commitTimestamp timestamp of the commit
 * @param snapshotId      connector snapshot identifier for this commit
 * @param eventType       type of commit operation (APPEND, OVERWRITE, COMPACT, DELETE)
 * @param commitFingerprint durable journaled-write identity, or null for ordinary events
 */
public record CommitEvent(
        String tenantId,
        String catalogName,
        String schemaName,
        String tableName,
        List<String> addedFiles,
        List<String> removedFiles,
        Instant commitTimestamp,
        String snapshotId,
        CommitEventType eventType,
        String commitFingerprint
) {

    public CommitEvent {
        requireName(tenantId, "tenantId");
        requireName(catalogName, "catalogName");
        requireName(schemaName, "schemaName");
        requireName(tableName, "tableName");
        Objects.requireNonNull(commitTimestamp, "commitTimestamp must not be null");
        requireName(snapshotId, "snapshotId");
        addedFiles = copyFiles(addedFiles);
        removedFiles = copyFiles(removedFiles);
        requireFilePaths(addedFiles, removedFiles);
        Objects.requireNonNull(eventType, "eventType must not be null");
        requireFingerprint(commitFingerprint);
    }

    private static List<String> copyFiles(List<String> files) {
        return files == null ? List.of() : List.copyOf(files);
    }

    private static void requireFilePaths(List<String> addedFiles, List<String> removedFiles) {
        if (addedFiles.stream().anyMatch(String::isBlank) || removedFiles.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("commit file paths must not be null or blank");
        }
    }

    private static void requireFingerprint(String commitFingerprint) {
        if (commitFingerprint != null && !commitFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("commitFingerprint must be a lowercase SHA-256 digest");
        }
    }

    private static void requireName(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

}
