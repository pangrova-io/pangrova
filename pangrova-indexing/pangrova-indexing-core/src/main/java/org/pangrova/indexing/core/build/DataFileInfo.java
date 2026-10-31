package org.pangrova.indexing.core.build;

public record DataFileInfo(String filePath, long rowCount, long sizeBytes, String format) {
    public static final String UNKNOWN_FORMAT = "unknown";

    public DataFileInfo {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("filePath must not be null or blank");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must not be negative, got " + rowCount);
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must not be negative, got " + sizeBytes);
        }
        if (format == null || format.isBlank()) {
            format = UNKNOWN_FORMAT;
        }
    }

    public boolean hasRowCountStatistics() {
        return rowCount > 0;
    }
}
