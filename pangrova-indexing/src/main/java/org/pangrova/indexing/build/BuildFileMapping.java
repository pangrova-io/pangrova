package org.pangrova.indexing.build;

public record BuildFileMapping(
        String indexId,
        String dataFilePath,
        String indexFilePath,
        long rowCount,
        long indexSizeBytes,
        String checksum) {
    public BuildFileMapping {
        requireText(indexId, "indexId");
        requireText(dataFilePath, "dataFilePath");
        requireText(indexFilePath, "indexFilePath");
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must not be negative, got " + rowCount);
        }
        if (indexSizeBytes < 0) {
            throw new IllegalArgumentException("indexSizeBytes must not be negative, got " + indexSizeBytes);
        }
        checksum = checksum == null ? "" : checksum;
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be null or blank");
        }
    }
}