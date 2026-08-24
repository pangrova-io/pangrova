package org.pangrova.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * SHA-256 digest utilities for hashing strings and byte arrays.
 *
 * <p>Provides convenient methods for computing SHA-256 digests and encoding
 * them as lowercase hexadecimal strings. Also exposes a factory method for
 * obtaining a fresh {@link MessageDigest} instance for streaming use cases.
 *
 * <p>This is a stateless utility class — all methods are static, and the class
 * cannot be instantiated or subclassed.
 *
 * <p>Thread safety: all methods are safe for concurrent use. Each invocation
 * creates its own {@link MessageDigest} instance (MessageDigest is not thread-safe).
 */
public final class DigestUtils {

    private DigestUtils() {}

    /**
     * Computes the SHA-256 digest of the given string using UTF-8 encoding
     * and returns the result as a lowercase hexadecimal string.
     *
     * @param input the string to hash; must not be null
     * @return 64-character lowercase hex-encoded SHA-256 digest
     * @throws NullPointerException if {@code input} is null
     */
    public static String sha256Hex(String input) {
        return sha256Hex(input.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Computes the SHA-256 digest of the given byte array and returns the
     * result as a lowercase hexadecimal string.
     *
     * @param data the bytes to hash; must not be null
     * @return 64-character lowercase hex-encoded SHA-256 digest
     * @throws NullPointerException if {@code data} is null
     */
    public static String sha256Hex(byte[] data) {
        return HexFormat.of().formatHex(sha256().digest(data));
    }

    /**
     * Returns a fresh {@link MessageDigest} instance configured for SHA-256.
     *
     * <p>Useful for streaming digest computation where data is fed incrementally
     * via {@link MessageDigest#update(byte[])}.
     *
     * @return a new SHA-256 {@link MessageDigest} instance
     * @throws IllegalStateException if the JVM does not support SHA-256
     *         (should never happen — SHA-256 is mandatory per the Java spec)
     */
    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
