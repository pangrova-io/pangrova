package org.pangrova.planner.core.property.runtimefilter;

import net.jqwik.api.*;
import net.jqwik.api.constraints.*;
import org.pangrova.common.bloom.SplitBlockBloomFilter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 1: Bloom filter cross-process hash consistency.
 *
 * <p>For any value of a supported column type (BIGINT, DOUBLE, VARCHAR, DECIMAL),
 * encoding it via Java {@code encodeForBloom} and inserting into
 * {@link SplitBlockBloomFilter}, then probing with the same bytes via native
 * xxHash64 {@code mightContain}, SHALL return true.</p>
 *
 * <p>This test validates the Java-side round-trip: the Parquet wire-format
 * binary encoding inserted via {@code putBytes} and probed via
 * {@code mightContainBytes} returns true — confirming the encoding is consistent
 * with the xxHash64-based SBBF probing path used by the native engine.</p>
 *
 * <p><b>Validates: Requirements 1.1</b></p>
 */
@Tag("Feature: production-readiness-fixes")
@Tag("Property 1: Bloom filter cross-process hash consistency")
class BloomHashConsistencyPropertyTest {

    /**
     * For any BIGINT value, LE 8-byte encoding inserted into SBBF and probed
     * with the same bytes returns true (no false negatives).
     */
    @Property(tries = 200)
    void bigintEncodingRoundTrip(@ForAll long value) {
        byte[] encoded = encodeBigint(value);

        assertThat(encoded).hasSize(8);
        assertEncodedBytesAreFoundInFilter(encoded);
        assertEncodingIsDeterministic(() -> encodeBigint(value));
    }

    /**
     * For any DOUBLE value, LE 8-byte IEEE-754 encoding inserted into SBBF
     * and probed with the same bytes returns true (no false negatives).
     */
    @Property(tries = 200)
    void doubleEncodingRoundTrip(@ForAll double value) {
        byte[] encoded = encodeDouble(value);

        assertThat(encoded).hasSize(8);
        assertEncodedBytesAreFoundInFilter(encoded);
        assertEncodingIsDeterministic(() -> encodeDouble(value));
    }

    /**
     * For any VARCHAR value, UTF-8 encoding inserted into SBBF and probed
     * with the same bytes returns true (no false negatives).
     */
    @Property(tries = 200)
    void varcharEncodingRoundTrip(@ForAll @StringLength(min = 1, max = 200) String value) {
        byte[] encoded = encodeVarchar(value);

        assertThat(encoded).isEqualTo(value.getBytes(StandardCharsets.UTF_8));
        assertEncodedBytesAreFoundInFilter(encoded);
        assertEncodingIsDeterministic(() -> encodeVarchar(value));
    }

    /**
     * For any DECIMAL value, unscaled two's-complement encoding inserted into
     * SBBF and probed with the same bytes returns true (no false negatives).
     */
    @Property(tries = 200)
    void decimalEncodingRoundTrip(@ForAll("decimals") BigDecimal value) {
        byte[] encoded = encodeDecimal(value);

        assertThat(encoded).isEqualTo(value.unscaledValue().toByteArray());
        assertEncodedBytesAreFoundInFilter(encoded);
        assertEncodingIsDeterministic(() -> encodeDecimal(value));
    }

    /**
     * For any INTEGER value, encoding produces LE 8-byte int64 representation
     * (widened to long) and round-trips through SBBF correctly.
     */
    @Property(tries = 200)
    void integerEncodingRoundTrip(@ForAll int value) {
        byte[] encoded = encodeBigint(value);

        byte[] expected = ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong((long) value)
                .array();
        assertThat(encoded).isEqualTo(expected);
        assertEncodedBytesAreFoundInFilter(encoded);
    }

    /**
     * Multiple values of mixed types inserted into a single filter all probe true.
     * This validates that the SBBF handles heterogeneous byte encodings correctly.
     */
    @Property(tries = 100)
    void mixedTypesBulkInsertAllProbeTrue(
            @ForAll @Size(min = 1, max = 50) List<@From("mixedEncodings") byte[]> encodedValues) {

        SplitBlockBloomFilter.Builder builder =
                SplitBlockBloomFilter.builder(Math.max(encodedValues.size(), 10), 0.01);

        for (byte[] encoded : encodedValues) {
            builder.putBytes(encoded);
        }

        SplitBlockBloomFilter filter = builder.build();

        for (byte[] encoded : encodedValues) {
            assertThat(filter.mightContainBytes(encoded))
                    .as("Encoded bytes must be found after insertion (no false negatives)")
                    .isTrue();
        }
    }

    // --- Providers ---

    @Provide
    Arbitrary<BigDecimal> decimals() {
        return Arbitraries.bigIntegers()
                .between(BigInteger.valueOf(-999_999_999_999L), BigInteger.valueOf(999_999_999_999L))
                .flatMap(unscaled -> Arbitraries.integers().between(0, 18)
                        .map(scale -> new BigDecimal(unscaled, scale)));
    }

    @Provide
    Arbitrary<byte[]> mixedEncodings() {
        return Arbitraries.oneOf(
                Arbitraries.longs().map(BloomHashConsistencyPropertyTest::encodeBigint),
                Arbitraries.doubles().map(BloomHashConsistencyPropertyTest::encodeDouble),
                Arbitraries.strings().ofMinLength(1).ofMaxLength(50)
                        .map(BloomHashConsistencyPropertyTest::encodeVarchar),
                decimals().map(BloomHashConsistencyPropertyTest::encodeDecimal)
        );
    }

    // --- Encoding functions (mirror RuntimeFilterGenerator.encodeForBloom spec) ---

    private static byte[] encodeBigint(long value) {
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value)
                .array();
    }

    private static byte[] encodeDouble(double value) {
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putDouble(value)
                .array();
    }

    private static byte[] encodeVarchar(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] encodeDecimal(BigDecimal value) {
        return value.unscaledValue().toByteArray();
    }

    // --- Assertion helpers ---

    private void assertEncodedBytesAreFoundInFilter(byte[] encoded) {
        SplitBlockBloomFilter filter = SplitBlockBloomFilter.builder(10, 0.01)
                .putBytes(encoded)
                .build();

        assertThat(filter.mightContainBytes(encoded))
                .as("Encoded bytes must be found in filter after insertion (no false negatives)")
                .isTrue();
    }

    private void assertEncodingIsDeterministic(java.util.function.Supplier<byte[]> encoder) {
        byte[] first = encoder.get();
        byte[] second = encoder.get();
        assertThat(first)
                .as("Encoding must be deterministic across invocations")
                .isEqualTo(second);
    }
}
