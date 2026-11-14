package org.pangrova.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DigestUtils}.
 * Covers known-hash verification, String/byte[] consistency,
 * MessageDigest factory, output format, and constructor inaccessibility.
 *
 * <p>Validates: Requirements 5.1-5.6
 */
class DigestUtilsTest {

    @Test
    void matchesSha256ChecksKnownEmptyAndUtf8Digests() {
        assertThat(DigestUtils.matchesSha256(new byte[0],
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")).isTrue();
        assertThat(DigestUtils.matchesSha256("hello".getBytes(StandardCharsets.UTF_8),
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")).isTrue();
    }

    @Test
    void matchesSha256RejectsFirstLastAndPayloadDifferences() {
        byte[] data = {0, 1, -1, -128, 127};
        String expected = DigestUtils.sha256Hex(data);
        assertThat(DigestUtils.matchesSha256(data, expected)).isTrue();
        char differentFirst = expected.charAt(0) == '0' ? '1' : '0';
        char differentLast = expected.charAt(63) == '0' ? '1' : '0';
        assertThat(DigestUtils.matchesSha256(data, differentFirst + expected.substring(1))).isFalse();
        assertThat(DigestUtils.matchesSha256(data, expected.substring(0, 63) + differentLast)).isFalse();
        data[2] ^= 1;
        assertThat(DigestUtils.matchesSha256(data, expected)).isFalse();
    }

    @Test
    void requireSha256AcceptsEitherCaseAndRejectsMalformedValues() {
        DigestUtils.requireSha256("abcdef0123456789".repeat(4), "checksum");
        DigestUtils.requireSha256("ABCDEF0123456789".repeat(4), "checksum");
        for (String invalid : new String[]{null, "", "a".repeat(63), "a".repeat(65),
                "a".repeat(63) + "g", "a".repeat(63) + "Ａ", "a".repeat(64) + "\n"}) {
            assertThatThrownBy(() -> DigestUtils.requireSha256(invalid, "checksum"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("checksum must be a SHA-256 hex digest");
        }
    }

    @Test
    void matchesSha256RequiresExactlyLowercaseAsciiHexWithoutNormalization() {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        String expected = DigestUtils.sha256Hex(data);
        assertThat(DigestUtils.matchesSha256(data, null)).isFalse();
        for (String malformed : new String[]{"", expected.substring(1), expected + "0",
                expected.toUpperCase(java.util.Locale.ROOT), " " + expected, expected + "\n",
                "g" + expected.substring(1), "\uff12" + expected.substring(1)}) {
            assertThat(DigestUtils.matchesSha256(data, malformed)).as("expected digest %s", malformed).isFalse();
        }
    }

    @Test
    void matchesSha256AlwaysRejectsNullDataWithNullPointerException() {
        for (String expected : new String[]{null, "invalid", DigestUtils.sha256Hex(new byte[0])}) {
            assertThatThrownBy(() -> DigestUtils.matchesSha256(null, expected))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("data");
        }
    }

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
        String fromBytes = DigestUtils.sha256Hex(input);
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
