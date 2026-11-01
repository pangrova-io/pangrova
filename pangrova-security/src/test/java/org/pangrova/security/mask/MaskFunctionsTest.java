package org.pangrova.security.mask;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 8.3 — MaskFunctions correctly mask PII patterns.
 */
class MaskFunctionsTest {

    @Test
    void maskEmailPreservesFirstCharAndDomain() {
        assertThat(MaskFunctions.maskEmail("alice@example.com")).isEqualTo("a***@example.com");
    }

    @Test
    void maskEmailReturnsNullForNull() {
        assertThat(MaskFunctions.maskEmail(null)).isNull();
    }

    @Test
    void maskEmailReturnsInputWithoutAtSign() {
        assertThat(MaskFunctions.maskEmail("noemail")).isEqualTo("noemail");
    }

    @Test
    void maskPhoneShowsLastFourDigits() {
        assertThat(MaskFunctions.maskPhone("1234567890")).isEqualTo("****7890");
    }

    @Test
    void maskPhoneReturnsShortInputUnchanged() {
        assertThat(MaskFunctions.maskPhone("123")).isEqualTo("123");
    }

    @Test
    void maskPhoneReturnsNullForNull() {
        assertThat(MaskFunctions.maskPhone(null)).isNull();
    }

    @Test
    void maskCardShowsLastFourDigits() {
        assertThat(MaskFunctions.maskCard("4111111111111111"))
                .isEqualTo("****-****-****-1111");
    }

    @Test
    void maskCardReturnsShortInputUnchanged() {
        assertThat(MaskFunctions.maskCard("123")).isEqualTo("123");
    }

    @Test
    void maskNamePreservesFirstChar() {
        assertThat(MaskFunctions.maskName("Alice")).isEqualTo("A****");
    }

    @Test
    void maskNameReturnsEmptyForEmpty() {
        assertThat(MaskFunctions.maskName("")).isEmpty();
    }

    @Test
    void maskNullReturnsNull() {
        assertThat(MaskFunctions.maskNull("anything")).isNull();
    }

    @Test
    void maskHashProducesDeterministicHmac() {
        String salt = "table_orders_salt_2024";
        String hash1 = MaskFunctions.maskHash("test", salt);
        String hash2 = MaskFunctions.maskHash("test", salt);
        assertThat(hash1).isEqualTo(hash2);
        assertThat(hash1).hasSize(64); // HMAC-SHA256 hex = 64 chars
    }

    @Test
    void maskHashReturnsNullForNull() {
        assertThat(MaskFunctions.maskHash(null, "salt")).isNull();
    }

    @Test
    void maskHashDifferentSaltsProduceDifferentOutputs() {
        String value = "sensitive_data";
        String hash1 = MaskFunctions.maskHash(value, "salt_table_a");
        String hash2 = MaskFunctions.maskHash(value, "salt_table_b");
        assertThat(hash1).isNotEqualTo(hash2);
    }

    @Test
    void maskHashRejectsNullSalt() {
        assertThatThrownBy(() -> MaskFunctions.maskHash("value", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maskHashRejectsEmptySalt() {
        assertThatThrownBy(() -> MaskFunctions.maskHash("value", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maskRangeProducesCorrectBucket() {
        assertThat(MaskFunctions.maskRange(42, 10)).isEqualTo("40-50");
        assertThat(MaskFunctions.maskRange(50, 10)).isEqualTo("50-60");
        assertThat(MaskFunctions.maskRange(0, 100)).isEqualTo("0-100");
    }
}
