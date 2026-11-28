package org.pangrova.common.file;

import org.pangrova.common.crypto.Sha256Digests;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * HMAC binding for a coordinator-issued file access descriptor.
 *
 * <p>A descriptor names a bucket, key, operation, and expiry but carries no
 * credential. Signing it with the coordinator/worker shared secret lets the
 * Worker accept the descriptor directly from a client without exposing its
 * storage credentials or accepting hand-crafted descriptors. The signature
 * covers the exact encoded descriptor string, so any field change invalidates
 * it.</p>
 */
public final class FileAccessSignature {


    private FileAccessSignature() {}

    /** Returns a URL-safe, unpadded HMAC over the encoded descriptor. */
    public static String sign(String secret, String encodedDescriptor) {
        requireSecret(secret);
        byte[] digest = Sha256Digests.hmacSha256(secret.getBytes(StandardCharsets.UTF_8))
                .doFinal(encodedDescriptor.getBytes(StandardCharsets.US_ASCII));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    }

    /** Rejects a missing or mismatched signature without leaking which one failed. */
    public static void requireValid(String secret, String encodedDescriptor, String signature) {
        requireSecret(secret);
        if (signature == null || signature.isBlank() || encodedDescriptor == null) {
            throw denied("A signed file access descriptor is required");
        }
        byte[] expected = sign(secret, encodedDescriptor).getBytes(StandardCharsets.US_ASCII);
        byte[] provided = signature.getBytes(StandardCharsets.US_ASCII);
        if (!MessageDigest.isEqual(expected, provided)) {
            throw denied("File access descriptor signature is not valid");
        }
    }

    private static void requireSecret(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException(
                    "File access signing requires a configured internal auth secret");
        }
    }

    private static PangrovaException denied(String message) {
        return new PangrovaException(PangrovaErrorCode.ACCESS_DENIED, message);
    }
}
