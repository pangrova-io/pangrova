package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Complete file-level change set between two immutable connector snapshots. */
@Experimental
public record SnapshotFileDelta(
        List<SnapshotDataFile> added,
        List<String> removed) {

    /** Computes the change from two duplicate-free complete inventories keyed by data-file path. */
    public static SnapshotFileDelta between(
            Map<String, SnapshotDataFile> from, Map<String, SnapshotDataFile> to) {
        var added = to.values().stream().filter(file -> !from.containsKey(file.path()))
                .sorted(Comparator.comparing(SnapshotDataFile::path)).toList();
        var removed = from.keySet().stream().filter(path -> !to.containsKey(path)).sorted().toList();
        return new SnapshotFileDelta(added, removed);
    }

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
