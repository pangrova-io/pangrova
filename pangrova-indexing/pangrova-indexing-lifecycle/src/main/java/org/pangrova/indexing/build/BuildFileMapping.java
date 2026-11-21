package org.pangrova.indexing.build;

import java.util.Objects;
import java.util.UUID;

public record BuildFileMapping(
        UUID attemptId,
        String indexId,
        String dataFilePath,
        String indexFilePath,
        long rowCount,
        long indexSizeBytes,
        String checksum) {
    public BuildFileMapping {
        Objects.requireNonNull(attemptId, "attemptId");
        requireText(indexId, "indexId");
        requireText(dataFilePath, "dataFilePath");
        requireText(indexFilePath, "indexFilePath");
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must not be negative, got " + rowCount);
        }
        if (indexSizeBytes < 0) {
            throw new IllegalArgumentException("indexSizeBytes must not be negative, got " + indexSizeBytes);
        }
        requireSha256(checksum);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be null or blank");
        }
    }

    private static void requireSha256(String value) {
        if (value == null || value.length() != 64
                || !value.chars().allMatch(BuildFileMapping::isHexDigit)) {
            throw new IllegalArgumentException(
                    "checksum must be a 64-character SHA-256 hex digest");
        }
    }

    private static boolean isHexDigit(int value) {
        return value >= '0' && value <= '9'
                || value >= 'a' && value <= 'f'
                || value >= 'A' && value <= 'F';
    }
}
