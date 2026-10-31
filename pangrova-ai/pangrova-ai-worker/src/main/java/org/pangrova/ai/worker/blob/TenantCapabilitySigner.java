package org.pangrova.ai.worker.blob;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Authenticates opaque FILE references using externally supplied cluster key material. */
public final class TenantCapabilitySigner {

    private static final int MINIMUM_KEY_BYTES = 32;
    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final byte[] DERIVATION_CONTEXT =
            "pangrova-file-capability-v2".getBytes(StandardCharsets.US_ASCII);

    private final byte[] masterKey;
    private boolean destroyed;

    TenantCapabilitySigner(byte[] masterKey) {
        if (masterKey == null || masterKey.length < MINIMUM_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "FILE capability key must contain at least 32 bytes");
        }
        this.masterKey = masterKey.clone();
    }

    /** Decodes and validates externally resolved cluster key material. */
    public static byte[] decodeBase64Secret(String resolvedSecret) {
        if (resolvedSecret == null || resolvedSecret.isBlank()) {
            throw new IllegalArgumentException("FILE capability secret must not be empty");
        }
        final byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(resolvedSecret.trim());
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "FILE capability secret must be canonical base64", failure);
        }
        if (!Base64.getEncoder().encodeToString(decoded).equals(resolvedSecret.trim())) {
            Arrays.fill(decoded, (byte) 0);
            throw new IllegalArgumentException(
                    "FILE capability secret must be canonical padded base64");
        }
        if (decoded.length < MINIMUM_KEY_BYTES) {
            Arrays.fill(decoded, (byte) 0);
            throw new IllegalArgumentException(
                    "FILE capability secret must decode to at least 32 bytes");
        }
        return decoded;
    }

    synchronized String sign(
            String tenantScope, String mimeType, long size, String checksum) {
        if (destroyed) {
            throw new IllegalStateException("FILE capability signer is closed");
        }
        byte[] tenantKey = deriveTenantKey(tenantScope);
        try {
            Mac mac = mac(tenantKey);
            update(mac, tenantScope);
            update(mac, mimeType);
            update(mac, Long.toString(size));
            update(mac, checksum);
            return HexFormat.of().formatHex(mac.doFinal());
        } finally {
            Arrays.fill(tenantKey, (byte) 0);
        }
    }

    synchronized void destroy() {
        Arrays.fill(masterKey, (byte) 0);
        destroyed = true;
    }

    private byte[] deriveTenantKey(String tenantScope) {
        Mac mac = mac(masterKey);
        mac.update(DERIVATION_CONTEXT);
        mac.update((byte) 0);
        mac.update(tenantScope.getBytes(StandardCharsets.US_ASCII));
        return mac.doFinal();
    }

    private static void update(Mac mac, String value) {
        mac.update(value.getBytes(StandardCharsets.UTF_8));
        mac.update((byte) 0);
    }

    private static Mac mac(byte[] key) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(new SecretKeySpec(key, HMAC_SHA256));
            return mac;
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException("HMAC-SHA-256 is unavailable", failure);
        }
    }
}
