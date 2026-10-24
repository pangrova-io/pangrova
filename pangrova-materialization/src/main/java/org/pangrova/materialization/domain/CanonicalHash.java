package org.pangrova.materialization.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;

/**
 * Canonical SHA-256 hashing for domain identities.
 */
final class CanonicalHash {

    /**
     * Orders strings by unsigned UTF-8 bytes, matching PostgreSQL's {@code COLLATE "C"}.
     */
    static final Comparator<String> UTF8_ORDER = (left, right) -> {
        byte[] a = left.getBytes(StandardCharsets.UTF_8);
        byte[] b = right.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(a.length, b.length);
        for (int i = 0; i < length; i++) {
            int comparison = Integer.compare(Byte.toUnsignedInt(a[i]), Byte.toUnsignedInt(b[i]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(a.length, b.length);
    };

    private CanonicalHash() {
    }

    static String sha256(String canonicalPayload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(canonicalPayload.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }
}
