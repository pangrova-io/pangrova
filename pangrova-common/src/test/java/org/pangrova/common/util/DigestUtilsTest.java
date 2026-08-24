package org.pangrova.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DigestUtils}.
 * Covers known-hash verification, String/byte[] consistency,
 * MessageDigest factory, output format, and constructor inaccessibility.
 *
 * <p>Validates: Requirements 5.1-5.6
 */
class DigestUtilsTest {

    // ── Known SHA-256 hash verification (Requirements 5.2, 5.6) ──

    @Test
    void sha256Hex_emptyString_returnsKnownHash() {
        assertThat(DigestUtils.sha256Hex(""))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void sha256Hex_hello_returnsKnownHash() {
        assertThat(DigestUtils.sha256Hex("hello"))
                .isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
    }

    // ── String and byte[] consistency (Requirements 5.2, 5.3) ──

    @Test
    void sha256Hex_stringAndBytes_produceSameResult() {
        String input = "pangrova-common-digest-test";
        String fromString = DigestUtils.sha256Hex(input);
        String fromBytes = DigestUtils.sha256Hex(input.getBytes(StandardCharsets.UTF_8));
        assertThat(fromString).isEqualTo(fromBytes);
    }

    // ── sha256() returns usable MessageDigest (Requirement 5.4, 5.5) ──

    @Test
    void sha256_returnsMessageDigestThatCanDigest() {
        MessageDigest digest = DigestUtils.sha256();
        assertThat(digest).isNotNull();
        assertThat(digest.getAlgorithm()).isEqualTo("SHA-256");

        byte[] result = digest.digest("test".getBytes(StandardCharsets.UTF_8));
        assertThat(result).hasSize(32);
    }

    // ── Output format: always 64 hex chars ──

    @Test
    void sha256Hex_outputIsAlways64Characters() {
        assertThat(DigestUtils.sha256Hex("")).hasSize(64);
        assertThat(DigestUtils.sha256Hex("hello")).hasSize(64);
        assertThat(DigestUtils.sha256Hex("a longer input string for testing")).hasSize(64);
    }

    // ── Output format: only lowercase hex [0-9a-f] ──

    @Test
    void sha256Hex_outputContainsOnlyLowercaseHex() {
        String hex = DigestUtils.sha256Hex("hello world");
        assertThat(hex).matches("[0-9a-f]{64}");
    }

    // ── Private constructor inaccessibility ──

    @Test
    void cannotInstantiate() throws Exception {
        var ctor = DigestUtils.class.getDeclaredConstructor();
        assertThat(ctor.canAccess(null)).isFalse();
    }
}
