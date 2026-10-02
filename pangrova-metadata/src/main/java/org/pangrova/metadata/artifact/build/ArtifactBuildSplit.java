package org.pangrova.metadata.artifact.build;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;

/** Exact canonical DataSplit bytes assigned to one durable artifact work page. */
public record ArtifactBuildSplit(
        String dataObjectId,
        String canonicalSplitBase64,
        String splitSha256) {

    public ArtifactBuildSplit {
        requireIdentity(dataObjectId, canonicalSplitBase64, splitSha256);
        byte[] bytes = decode(canonicalSplitBase64);
        if (bytes.length == 0 || !sha256(bytes).equalsIgnoreCase(splitSha256)) {
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

    public static ArtifactBuildSplit of(String dataObjectId, byte[] canonicalSplit) {
        return new ArtifactBuildSplit(
                dataObjectId, Base64.getEncoder().encodeToString(canonicalSplit),
                sha256(canonicalSplit));
    }

    public byte[] canonicalSplitBytes() {
        return Base64.getDecoder().decode(canonicalSplitBase64);
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
