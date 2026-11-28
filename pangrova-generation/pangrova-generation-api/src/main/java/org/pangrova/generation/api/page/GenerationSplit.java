package org.pangrova.generation.api.page;

import org.pangrova.common.crypto.Sha256Digests;

import java.util.Base64;

/** Exact canonical DataSplit bytes assigned to one durable artifact work page. */
public record GenerationSplit(
        String dataObjectId,
        String canonicalSplitBase64,
        String splitSha256) {

    public GenerationSplit {
        requireIdentity(dataObjectId, canonicalSplitBase64, splitSha256);
        byte[] bytes = decode(canonicalSplitBase64);
        if (bytes.length == 0 || !Sha256Digests.sha256Hex(bytes).equalsIgnoreCase(splitSha256)) {
            throw new IllegalArgumentException("artifact split is empty or its SHA-256 differs");
        }
    }

    private static void requireIdentity(
            String dataObjectId, String canonicalSplitBase64, String splitSha256) {
        if (dataObjectId == null || dataObjectId.isBlank()
                || canonicalSplitBase64 == null || canonicalSplitBase64.isBlank()
                || splitSha256 == null || !splitSha256.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("artifact split identity and checksum are required");
        }
    }

    private static byte[] decode(String canonicalSplitBase64) {
        try {
            return Base64.getDecoder().decode(canonicalSplitBase64);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("artifact split is not valid Base64", e);
        }
    }

    public static GenerationSplit of(String dataObjectId, byte[] canonicalSplit) {
        return new GenerationSplit(
                dataObjectId, Base64.getEncoder().encodeToString(canonicalSplit),
                Sha256Digests.sha256Hex(canonicalSplit));
    }

    public byte[] canonicalSplitBytes() {
        return Base64.getDecoder().decode(canonicalSplitBase64);
    }
}
