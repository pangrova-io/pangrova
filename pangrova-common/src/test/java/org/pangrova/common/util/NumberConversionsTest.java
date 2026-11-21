package org.pangrova.common.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NumberConversions}.
 * Validates null safety, non-Number fallback, normal conversions, and boundary values.
 */
class NumberConversionsTest {

    @Test
    void preservesCallerDefaultsForMissingAndNonNumericValuesOnly() {
        assertThat(NumberConversions.toLong(null, -1L)).isEqualTo(-1L);
        assertThat(NumberConversions.toLong("12", -1L)).isEqualTo(-1L);
        assertThat(NumberConversions.toLong(12.9, -1L)).isEqualTo(12L);
        assertThat(NumberConversions.toDouble(null, -1.5)).isEqualTo(-1.5);
        assertThat(NumberConversions.toDouble("12", -1.5)).isEqualTo(-1.5);
        assertThat(NumberConversions.toDouble(0L, -1.5)).isZero();
    }

    // --- toDouble ---

    @Test
    void toDouble_nullReturnsZero() {
        assertThat(NumberConversions.toDouble(null)).isEqualTo(0.0);
    }

    @Test
    void toDouble_nonNumberReturnsZero() {
        assertThat(NumberConversions.toDouble("not a number")).isEqualTo(0.0);
    }

    @Test
    void toDouble_integerValue() {
        assertThat(NumberConversions.toDouble(42)).isEqualTo(42.0);
    }

    @Test
    void toDouble_longMaxValue() {
        assertThat(NumberConversions.toDouble(Long.MAX_VALUE)).isEqualTo((double) Long.MAX_VALUE);
    }

    @Test
    void toDouble_floatValue() {
        assertThat(NumberConversions.toDouble(3.14f)).isEqualTo((double) 3.14f);
    }

    // --- toLong ---

    @Test
    void toLong_nullReturnsZero() {
        assertThat(NumberConversions.toLong(null)).isEqualTo(0L);
    }

    @Test
    void toLong_nonNumberReturnsZero() {
        assertThat(NumberConversions.toLong("text")).isEqualTo(0L);
    }

    @Test
    void toLong_integerMaxValue() {
        assertThat(NumberConversions.toLong(Integer.MAX_VALUE)).isEqualTo((long) Integer.MAX_VALUE);
    }

    @Test
    void toLong_doubleValueTruncates() {
        assertThat(NumberConversions.toLong(9.99)).isEqualTo(9L);
    }

    // --- toInt ---

    @Test
    void toInt_nullReturnsZero() {
        assertThat(NumberConversions.toInt(null)).isEqualTo(0);
    }

    @Test
    void toInt_nonNumberReturnsZero() {
        assertThat(NumberConversions.toInt(new Object())).isEqualTo(0);
    }

    @Test
    void toInt_shortValue() {
        assertThat(NumberConversions.toInt((short) 100)).isEqualTo(100);
    }

    @Test
    void toInt_integerMaxValue() {
        assertThat(NumberConversions.toInt(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    // --- toFloat ---

    @Test
    void toFloat_nullReturnsZero() {
        assertThat(NumberConversions.toFloat(null)).isEqualTo(0.0f);
    }

    @Test
    void toFloat_nonNumberReturnsZero() {
        assertThat(NumberConversions.toFloat(Boolean.TRUE)).isEqualTo(0.0f);
    }

    @Test
    void toFloat_doubleValue() {
        assertThat(NumberConversions.toFloat(2.5)).isEqualTo(2.5f);
    }

    @Test
    void toFloat_negativeValue() {
        assertThat(NumberConversions.toFloat(-7)).isEqualTo(-7.0f);
    }
}
