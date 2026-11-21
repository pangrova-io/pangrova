package org.pangrova.common.util;

/**
 * Safe numeric type conversions for heterogeneous data sources.
 * Centralizes the repeated toDouble/toLong/toInt patterns found
 * across connector, test, and query evaluation code.
 */
public final class NumberConversions {

    private NumberConversions() {
        // utility class
    }

    /**
     * Converts a value to double. Returns 0.0 if value is null or not a Number.
     */
    public static double toDouble(Object value) {
        return toDouble(value, 0.0);
    }

    /** Converts numbers using Number.doubleValue(), returning the supplied default for other values. */
    public static double toDouble(Object value, double defaultValue) {
        return value instanceof Number n ? n.doubleValue() : defaultValue;
    }

    /**
     * Converts a value to long. Returns 0 if value is null or not a Number.
     */
    public static long toLong(Object value) {
        return toLong(value, 0L);
    }

    /** Converts numbers using Number.longValue(), returning the supplied default for other values. */
    public static long toLong(Object value, long defaultValue) {
        return value instanceof Number n ? n.longValue() : defaultValue;
    }

    /**
     * Converts a value to int. Returns 0 if value is null or not a Number.
     */
    public static int toInt(Object value) {
        return value instanceof Number n ? n.intValue() : 0;
    }

    /**
     * Converts a value to float. Returns 0.0f if value is null or not a Number.
     */
    public static float toFloat(Object value) {
        return value instanceof Number n ? n.floatValue() : 0.0f;
    }
}
