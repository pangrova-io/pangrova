package org.pangrova.connector.paimon.handle;

import java.util.List;

/**
 * Describes a single LSM level within a Paimon split.
 *
 * @param level            LSM tree level number (0 = newest)
 * @param files            exact data files in this level
 * @param totalRecordCount total record count across all files in this level
 */
public record PaimonLsmLevel(
        int level,
        List<PaimonLsmFile> files,
        long totalRecordCount) {

    public PaimonLsmLevel {
        files = List.copyOf(files);
    }

    public List<String> filePaths() { return files.stream().map(file -> file.physical().path()).toList(); }
}
