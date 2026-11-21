package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

import static org.pangrova.common.util.Preconditions.requireText;

/** Versioned cross-runtime identity of the bytes stored in an index artifact. */
@Experimental
public record IndexGenerationFormat(
        String formatId,
        int majorVersion,
        int minorVersion,
        String schemaSha256,
        String writerBuildId) {

    public IndexGenerationFormat {
        requireText(formatId, "formatId");
        requireText(writerBuildId, "writerBuildId");
        if (majorVersion <= 0 || minorVersion < 0) {
            throw new IllegalArgumentException("artifact format version is invalid");
        }
        if (!isSha256(schemaSha256)) {
            throw new IllegalArgumentException("schemaSha256 must be a SHA-256 hex digest");
        }
    }

    private static boolean isSha256(String value) {
        return value != null && value.length() == 64
                && value.chars().allMatch(character -> character >= '0' && character <= '9'
                        || character >= 'a' && character <= 'f'
                        || character >= 'A' && character <= 'F');
    }
}
