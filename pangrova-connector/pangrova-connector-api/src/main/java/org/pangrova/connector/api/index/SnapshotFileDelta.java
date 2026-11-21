package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/** Complete file-level change set between two immutable connector snapshots. */
@Experimental
public record SnapshotFileDelta(
        List<SnapshotDataFile> added,
        List<String> removed) {

    public SnapshotFileDelta {
        added = List.copyOf(added);
        removed = List.copyOf(removed);
        if (added.stream().map(SnapshotDataFile::path).distinct().count() != added.size()) {
            throw new IllegalArgumentException("added files must have unique paths");
        }
        if (removed.stream().anyMatch(path -> path == null || path.isBlank())
                || removed.stream().distinct().count() != removed.size()) {
            throw new IllegalArgumentException("removed files must be non-blank and unique");
        }
    }
}
