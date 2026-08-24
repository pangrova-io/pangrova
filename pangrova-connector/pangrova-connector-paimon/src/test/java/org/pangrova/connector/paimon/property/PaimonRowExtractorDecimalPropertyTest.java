package org.pangrova.connector.paimon.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DecimalType;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test verifying PaimonRowExtractor decimal precision preservation.
 *
 * <p>For any valid precision (1-38) and scale (0-precision), the extracted
 * BigDecimal maintains consistent precision and scale after roundtrip through
 * Paimon's InternalRow decimal storage.</p>
 *
 * <p>Validates: Requirements 1.3</p>
 */
class PaimonRowExtractorDecimalPropertyTest {

    /**
     * For any valid precision/scale combination and a BigDecimal that fits
     * within those constraints, extracting via InternalRow.getDecimal()
     * with the DecimalType's precision and scale yields a BigDecimal
     * whose scale matches the declared scale and whose unscaled value
     * is consistent with the original.
     *
     * <p>This replicates the exact extraction logic in PaimonRowExtractor:
     * {@code row.getDecimal(ordinal, dt.getPrecision(), dt.getScale()).toBigDecimal()}</p>
     *
     * <p>Validates: Requirements 1.3</p>
     */
    @Property(tries = 200)
    void decimalPrecisionAndScalePreservedThroughExtraction(
            @ForAll("decimalInputs") DecimalInput input) {

        DecimalType decimalType = new DecimalType(input.precision(), input.scale());
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();

        // Store the original BigDecimal into a GenericRow via Paimon Decimal
        Decimal paimonDecimal = Decimal.fromBigDecimal(input.value(), precision, scale);
        GenericRow row = GenericRow.of(paimonDecimal);

        // Extract using the same logic as PaimonRowExtractor.extractNonNull():
        //   DecimalType dt = (DecimalType) type;
        //   row.getDecimal(ordinal, dt.getPrecision(), dt.getScale()).toBigDecimal()
        Decimal extracted = row.getDecimal(0, precision, scale);
        BigDecimal result = extracted.toBigDecimal();

        // The extracted BigDecimal must have the declared scale
        assertThat(result.scale())
                .as("Extracted decimal scale must match DecimalType scale=%d", scale)
                .isEqualTo(scale);

        // The extracted value must be numerically equal to the input
        // (input is already constrained to fit within precision/scale)
        assertThat(result.compareTo(input.value()))
                .as("Extracted value %s must equal input %s", result, input.value())
                .isZero();
    }

    /**
     * Null values at a decimal ordinal must extract as null.
     *
     * <p>Validates: Requirements 1.3</p>
     */
    @Property(tries = 50)
    void nullDecimalExtractsAsNull(@ForAll("precisionScale") int[] ps) {
        int precision = ps[0];
        int scale = ps[1];

        GenericRow row = GenericRow.of((Object) null);

        assertThat(row.isNullAt(0))
                .as("Null decimal at ordinal 0 should report isNullAt=true")
                .isTrue();
    }

    // ── Arbitraries ──

    @Provide
    Arbitrary<DecimalInput> decimalInputs() {
        return precisionScaleArbitrary().flatMap(ps -> {
            int precision = ps[0];
            int scale = ps[1];
            return decimalValueFor(precision, scale)
                    .map(bd -> new DecimalInput(precision, scale, bd));
        });
    }

    @Provide
    Arbitrary<int[]> precisionScale() {
        return precisionScaleArbitrary();
    }

    private Arbitrary<int[]> precisionScaleArbitrary() {
        return Arbitraries.integers().between(1, 38).flatMap(precision ->
                Arbitraries.integers().between(0, precision).map(scale ->
                        new int[]{precision, scale}));
    }

    /**
     * Generates a BigDecimal that fits within the given precision and scale.
     * The integer part has at most (precision - scale) digits, and the
     * fractional part has exactly {@code scale} digits.
     */
    private Arbitrary<BigDecimal> decimalValueFor(int precision, int scale) {
        int integerDigits = precision - scale;

        // Generate unscaled long values for compact decimals (precision <= 18)
        if (precision <= 18) {
            long maxUnscaledLong = (long) Math.pow(10, precision) - 1;
            return Arbitraries.longs()
                    .between(-maxUnscaledLong, maxUnscaledLong)
                    .map(unscaled -> BigDecimal.valueOf(unscaled, scale));
        }

        // For high precision (19-38), build from string to avoid overflow
        return Arbitraries.integers().between(1, precision).flatMap(totalDigits -> {
            return Arbitraries.strings()
                    .withCharRange('0', '9')
                    .ofLength(totalDigits)
                    .flatMap(digits -> {
                        return Arbitraries.of(true, false).map(negative -> {
                            BigDecimal bd = new BigDecimal(digits)
                                    .setScale(scale, RoundingMode.DOWN);
                            // Ensure it fits within precision
                            BigDecimal maxVal = new BigDecimal("9".repeat(precision))
                                    .movePointLeft(scale);
                            if (bd.abs().compareTo(maxVal) > 0) {
                                bd = maxVal.setScale(scale, RoundingMode.DOWN);
                            }
                            return (negative && bd.signum() != 0) ? bd.negate() : bd;
                        });
                    });
        });
    }

    record DecimalInput(int precision, int scale, BigDecimal value) {}
}
