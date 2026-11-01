package org.pangrova.indexing.api;

/** Versioned cross-runtime identity of the bytes stored in an index artifact. */
public record IndexArtifactFormat(
        String formatId,
        int majorVersion,
        int minorVersion,
        String schemaSha256,
        String writerBuildId) {

    public IndexArtifactFormat {
        requireText(formatId, "formatId");
        requireText(writerBuildId, "writerBuildId");
        if (majorVersion <= 0 || minorVersion < 0) {
            throw new IllegalArgumentException("artifact format version is invalid");
        }
        if (!isSha256(schemaSha256)) {
            throw new IllegalArgumentException("schemaSha256 must be a SHA-256 hex digest");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static boolean isSha256(String value) {
        return value != null && value.length() == 64
                && value.chars().allMatch(character -> character >= '0' && character <= '9'
                        || character >= 'a' && character <= 'f'
                        || character >= 'A' && character <= 'F');
    }
}
