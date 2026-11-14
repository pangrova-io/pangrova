package org.pangrova.streaming.aggregation;

import org.pangrova.types.value.ColumnValue;

/**
 * Approximate COUNT DISTINCT using HyperLogLog algorithm.
 *
 * <p>Provides O(1) memory usage regardless of cardinality, with typical
 * standard error of ~1.04/sqrt(m) where m = 2^precision. Default precision
 * of 14 gives ~0.81% standard error with 16KB memory.</p>
 *
 * <p>Note: HyperLogLog is an append-only sketch. Retraction is not supported
 * in approximate mode (retracting from HLL would require storing all values,
 * defeating the purpose). Retract calls are ignored with a no-op.</p>
 */
public final class HyperLogLogCountDistinctAccumulator implements StreamingAccumulator {

    private static final int DEFAULT_PRECISION = 14;
    private static final int MIN_PRECISION = 4;
    private static final int MAX_PRECISION = 18;
    private static final int LONG_BIT_WIDTH = 64;
    private static final double SMALL_RANGE_CORRECTION_FACTOR = 2.5;
    private static final double LARGE_RANGE_THRESHOLD_DIVISOR = 30.0;
    private static final int LARGE_RANGE_BIT_WIDTH = 32;
    private static final int REGISTER_COUNT_16384 = 16384;
    private static final double ALPHA_NUMERATOR = 0.7213;
    private static final double ALPHA_SLOPE_COEFFICIENT = 1.079;
    private static final long HASH_MIX_SHIFT = 33;
    private static final long HASH_MIX_CONSTANT_1 = 0xff51afd7ed558ccdL;
    private static final long HASH_MIX_CONSTANT_2 = 0xc4ceb9fe1a85ec53L;
    private static final double ALPHA_16384 =
            ALPHA_NUMERATOR / (1.0 + ALPHA_SLOPE_COEFFICIENT / REGISTER_COUNT_16384);

    private final int precision;
    private final int registerCount;
    private final byte[] registers;

    /** Creates an accumulator with the default precision. */
    public HyperLogLogCountDistinctAccumulator() {
        this(DEFAULT_PRECISION);
    }

    /** Creates an accumulator with the given precision (4 to 18). */
    public HyperLogLogCountDistinctAccumulator(int precision) {
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    "HyperLogLog precision must be between " + MIN_PRECISION
                            + " and " + MAX_PRECISION + ", got: " + precision);
        }
        this.precision = precision;
        this.registerCount = 1 << precision;
        this.registers = new byte[registerCount];
    }

    @Override
    public void accumulate(ColumnValue value) {
        if (value instanceof ColumnValue.NullValue) {
            return;
        }
        long hash = hash64(value);
        int registerIndex = (int) (hash >>> (LONG_BIT_WIDTH - precision));
        long remaining = (hash << precision) | (1L << (precision - 1));
        int leadingZeros = Long.numberOfLeadingZeros(remaining) + 1;
        if (leadingZeros > registers[registerIndex]) {
            registers[registerIndex] = (byte) leadingZeros;
        }
    }

    /**
     * Retraction is not supported for HyperLogLog (append-only sketch).
     *
     * <p>HLL cannot remove elements without maintaining the full set of elements,
     * which would defeat its O(1) memory guarantee. This is a documented
     * intentional no-op inherent to the probabilistic data structure.</p>
     */
    @Override
    public void retract(ColumnValue value) {
        // Intentional no-op: HyperLogLog is an append-only probabilistic sketch.
    }

    @Override
    public ColumnValue result() {
        double estimate = rawEstimate();
        long corrected = applyCorrections(estimate);
        return new ColumnValue.LongValue(corrected);
    }

    @Override
    public void reset() {
        java.util.Arrays.fill(registers, (byte) 0);
    }

    /**
     * Returns the estimated cardinality as a long value.
     */
    public long estimatedCount() {
        double estimate = rawEstimate();
        return applyCorrections(estimate);
    }

    private double rawEstimate() {
        double harmonicMean = 0.0;
        for (byte register : registers) {
            harmonicMean += 1.0 / (1L << register);
        }
        double alpha = computeAlpha();
        return alpha * registerCount * registerCount / harmonicMean;
    }

    private long applyCorrections(double estimate) {
        if (estimate <= SMALL_RANGE_CORRECTION_FACTOR * registerCount) {
            int zeroRegisters = countZeroRegisters();
            if (zeroRegisters > 0) {
                return Math.round(registerCount * Math.log((double) registerCount / zeroRegisters));
            }
        }
        if (estimate > (1.0 / LARGE_RANGE_THRESHOLD_DIVISOR) * (1L << LARGE_RANGE_BIT_WIDTH)) {
            return Math.round(-1.0 * (1L << LARGE_RANGE_BIT_WIDTH)
                    * Math.log(1.0 - estimate / (1L << LARGE_RANGE_BIT_WIDTH)));
        }
        return Math.round(estimate);
    }

    private int countZeroRegisters() {
        int count = 0;
        for (byte register : registers) {
            if (register == 0) {
                count++;
            }
        }
        return count;
    }

    private double computeAlpha() {
        if (registerCount == REGISTER_COUNT_16384) {
            return ALPHA_16384;
        }
        return ALPHA_NUMERATOR / (1.0 + ALPHA_SLOPE_COEFFICIENT / registerCount);
    }

    private static long hash64(ColumnValue value) {
        long h = value.hashCode();
        h ^= (h >>> HASH_MIX_SHIFT);
        h *= HASH_MIX_CONSTANT_1;
        h ^= (h >>> HASH_MIX_SHIFT);
        h *= HASH_MIX_CONSTANT_2;
        h ^= (h >>> HASH_MIX_SHIFT);
        return h;
    }
}
