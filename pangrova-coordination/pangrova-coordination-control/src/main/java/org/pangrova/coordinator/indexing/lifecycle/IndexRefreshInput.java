package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.build.DataFileInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Snapshot-pinned file delta used to build one immutable index generation. */
record IndexRefreshInput(
        String sourceSnapshot,
        List<DataFileInfo> addedFiles,
        List<String> removedFiles,
        boolean forceReplacement) {

    IndexRefreshInput {
        if (sourceSnapshot == null || sourceSnapshot.isBlank()) {
            throw new IllegalArgumentException("sourceSnapshot must not be blank");
        }
        addedFiles = List.copyOf(addedFiles);
        removedFiles = List.copyOf(removedFiles);
        Set<String> addedPaths = new HashSet<>();
        for (DataFileInfo file : addedFiles) {
            if (!addedPaths.add(file.filePath())) {
                throw new IllegalArgumentException(
                        "Duplicate added file: " + file.filePath());
            }
        }
        Set<String> removedPaths = new HashSet<>();
        for (String path : removedFiles) {
            if (path == null || path.isBlank() || !removedPaths.add(path)) {
                throw new IllegalArgumentException(
                        "Removed files must be non-blank and unique");
            }
        }
    }
}
