package org.pangrova.generation.api.output;

/** Immutable object metadata emitted directly by a Native background task. */
public record GenerationOutput(
        String objectId,
        String opaqueUri,
        String objectRole,
        String sourceObjectId,
        int ordinal,
        GenerationOutputScope scope,
        long sizeBytes,
        long rowCount,
        String checksumSha256,
        String encryptionKeyRef) {

    public GenerationOutput {
        requireText(objectId, "objectId");
        requireText(opaqueUri, "opaqueUri");
        requireText(objectRole, "objectRole");
        requireText(sourceObjectId, "sourceObjectId");
        requireText(checksumSha256, "checksumSha256");
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        if (ordinal < 0 || sizeBytes <= 0 || rowCount < 0) {
            throw new IllegalArgumentException("invalid artifact object counters");
        }
        if (!checksumSha256.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("checksumSha256 must be 64 hexadecimal characters");
        }
        if (encryptionKeyRef == null) {
            throw new IllegalArgumentException("encryptionKeyRef must not be null");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
