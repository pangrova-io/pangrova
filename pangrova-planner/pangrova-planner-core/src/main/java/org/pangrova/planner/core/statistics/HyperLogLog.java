package org.pangrova.planner.core.statistics;

import java.util.Arrays;

/**
 * HyperLogLog cardinality estimator for NDV (number of distinct values).
 *
 * <p>Uses the HyperLogLog algorithm with bias correction for small cardinalities.
 * Default precision is 14 bits (16384 registers), giving ~0.8% standard error,
 * which is suitable for CBO cost estimation.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * HyperLogLog hll = new HyperLogLog();
 * for (Object value : columnValues) {
 *     hll.add(value);
 * }
 * long ndv = hll.estimate();
 * }</pre>
 *
 * <h3>Industry context</h3>
 * <ul>
 *   <li><b>Iceberg:</b> Uses Apache Datasketches theta-sketch for NDV in Puffin files.</li>
 *   <li><b>Spark:</b> Uses HyperLogLog++ (HLL++) with precision 14 for ANALYZE.</li>
 *   <li><b>Trino:</b> Uses HyperLogLog with precision 12 for ANALYZE.</li>
 * </ul>
 */
public final class HyperLogLog {

    private static final int DEFAULT_PRECISION = 14;

    private final int precision;
    private final int registerCount;
    private final byte[] registers;
    private final double alphaMM;

    /** Creates an HLL with default precision (14 bits, ~0.8% error). */
    public HyperLogLog() {
        this(DEFAULT_PRECISION);
    }

    /**
     * Creates an HLL with the specified precision.
     *
     * @param precision number of bits for register indexing (4-18)
     */
    public HyperLogLog(int precision) {
        if (precision < 4 || precision > 18) {
            throw new IllegalArgumentException("precision must be in [4, 18], got " + precision);
        }
        this.precision = precision;
        this.registerCount = 1 << precision;
        this.registers = new byte[registerCount];
        this.alphaMM = computeAlpha(registerCount) * registerCount * registerCount;
    }

    /**
     * Adds a value to the estimator.
     */
    public void add(Object value) {
        if (value == null) return;
        long hash = hash64(value);
        int index = (int) (hash >>> (64 - precision));
        int remaining = (int) (hash << precision);
        int leadingZeros = Integer.numberOfLeadingZeros(remaining) + 1;
        if (leadingZeros > registers[index]) {
            registers[index] = (byte) leadingZeros;
        }
    }

    /**
     * Estimates the number of distinct values added.
     *
     * @return estimated NDV (always >= 0)
     */
    public long estimate() {
        double harmonicMean = 0;
        int zeros = 0;
        for (byte reg : registers) {
            harmonicMean += 1.0 / (1L << reg);
            if (reg == 0) zeros++;
        }
        double raw = alphaMM / harmonicMean;

        // Small-range correction
        if (raw <= 2.5 * registerCount && zeros > 0) {
            raw = registerCount * Math.log((double) registerCount / zeros);
        }

        return Math.max(0, Math.round(raw));
    }

    /**
     * Merges another HLL into this one (union).
     */
    public void merge(HyperLogLog other) {
        if (other.precision != this.precision) {
            throw new IllegalArgumentException(
                    "Cannot merge HLLs with different precision: " + precision + " vs " + other.precision);
        }
        for (int i = 0; i < registerCount; i++) {
            if (other.registers[i] > registers[i]) {
                registers[i] = other.registers[i];
            }
        }
    }

    /** Returns the number of registers (2^precision). */
    public int registerCount() {
        return registerCount;
    }

    /** Resets all registers to zero. */
    public void reset() {
        Arrays.fill(registers, (byte) 0);
    }

    private static double computeAlpha(int m) {
        if (m <= 16) return 0.673;
        if (m <= 32) return 0.697;
        if (m <= 64) return 0.709;
        return 0.7213 / (1.0 + 1.079 / m);
    }

    private static long hash64(Object value) {
        // Mix bits of hashCode to spread across 64 bits
        long h = value.hashCode();
        h = h * 0x9E3779B97F4A7C15L;
        h = (h ^ (h >>> 30)) * 0xBF58476D1CE4E5B9L;
        h = (h ^ (h >>> 27)) * 0x94D049BB133111EBL;
        return h ^ (h >>> 31);
    }
}
