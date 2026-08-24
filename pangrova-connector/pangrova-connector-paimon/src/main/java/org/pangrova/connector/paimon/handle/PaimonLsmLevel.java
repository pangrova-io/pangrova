package org.pangrova.connector.paimon.handle;

import java.util.List;

/**
 * Describes a single LSM level within a Paimon split.
 *
 * @param level            LSM tree level number (0 = newest)
 * @param filePaths        data file paths in this level
 * @param totalRecordCount total record count across all files in this level
 */
public record PaimonLsmLevel(
        int level,
        List<String> filePaths,
        long totalRecordCount) {

    public PaimonLsmLevel {
        filePaths = filePaths == null ? List.of() : List.copyOf(filePaths);
    }
}
