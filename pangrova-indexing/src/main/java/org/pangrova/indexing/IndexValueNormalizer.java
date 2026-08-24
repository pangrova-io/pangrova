package org.pangrova.indexing;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Canonical representations for index values, ensuring build-time and
 * evaluate-time produce identical keys for the same logical value.
 *
 * <p>Problem: {@code BigDecimal("100.0").toString()} yields {@code "100.0"}
 * while {@code Integer(100).toString()} yields {@code "100"}. If build uses
 * one representation and evaluate uses another, Bloom filters produce false
 * negatives (data loss). Similarly, {@code Long(100).equals(Integer(100))}
 * is false, breaking Bitmap HashMap lookups.</p>
 *
 * <p>Two normalization modes:
 * <ul>
 *   <li>{@link #toCanonicalString} — for Bloom filter keys (string-based)</li>
 *   <li>{@link #toCanonicalKey} — for Bitmap map keys (preserves Comparable)</li>
 * </ul>
 */
public final class IndexValueNormalizer {

    private IndexValueNormalizer() {}

    /**
     * Returns a canonical string for the given value, safe for use as
     * Bloom filter hash keys. All numeric types produce the same string
     * for the same logical value.
     *
     * @param value non-null comparable value
     * @return canonical string representation
     */
    public static String toCanonicalString(Comparable<?> value) {
        if (value instanceof BigDecimal bd) {
            return normalizeDecimalString(bd);
        }
        if (value instanceof Number n) {
            return numberToCanonicalString(n);
        }
        return value.toString();
    }

    private static String numberToCanonicalString(Number n) {
        if (n instanceof BigInteger bi) {
            return bi.toString();
        }
        if (n instanceof Double || n instanceof Float) {
            return doubleToCanonicalString(n.doubleValue());
        }
        return Long.toString(n.longValue());
    }

    /**
     * Returns a canonical Comparable key for the given value, safe for use
     * as HashMap keys. Integral types are unified to Long, floating types
     * to Double, so that {@code equals()} works across type boundaries.
     *
     * @param value non-null comparable value
     * @return canonical comparable key
     */
    public static Comparable<?> toCanonicalKey(Comparable<?> value) {
        if (value instanceof BigDecimal bd) {
            return normalizeDecimalKey(bd);
        }
        if (value instanceof Number n) {
            return numberToCanonicalKey(n);
        }
        return value;
    }

    private static Comparable<?> numberToCanonicalKey(Number n) {
        if (n instanceof BigInteger bi) {
            try {
                return bi.longValueExact();
            } catch (ArithmeticException e) {
                return bi;
            }
        }
        if (n instanceof Double || n instanceof Float) {
            return floatingToCanonicalKey(n.doubleValue());
        }
        return n.longValue();
    }

    private static Comparable<?> floatingToCanonicalKey(double d) {
        if (d == Math.floor(d) && !Double.isInfinite(d)
                && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
            return (long) d;
        }
        return d;
    }

    private static final int MAX_PLAIN_STRING_PRECISION = 38;

    private static String normalizeDecimalString(BigDecimal bd) {
        try {
            return Long.toString(bd.longValueExact());
        } catch (ArithmeticException e) {
            BigDecimal stripped = bd.stripTrailingZeros();
            if (stripped.scale() < -MAX_PLAIN_STRING_PRECISION
                    || stripped.precision() > MAX_PLAIN_STRING_PRECISION) {
                return stripped.toEngineeringString();
            }
            return stripped.toPlainString();
        }
    }

    private static Comparable<?> normalizeDecimalKey(BigDecimal bd) {
        try {
            return bd.longValueExact();
        } catch (ArithmeticException e) {
            // Preserve BigDecimal precision; falling back to double would lose
            // significant digits for any decimal beyond ~15-17 significant figures
            // and would also make equal-but-different-scale decimals collide.
            return bd.stripTrailingZeros();
        }
    }

    private static String doubleToCanonicalString(double d) {
        if (d == Math.floor(d) && !Double.isInfinite(d)
                && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
            return Long.toString((long) d);
        }
        return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
    }
}
