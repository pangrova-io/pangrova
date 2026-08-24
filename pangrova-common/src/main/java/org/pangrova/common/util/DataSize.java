package org.pangrova.common.util;

/**
 * Byte-size constants and conversion utilities.
 *
 * <p>Replaces scattered {@code 256L * 1024 * 1024} magic expressions with
 * readable constants and factory methods such as {@code DataSize.megabytes(256)}.
 *
 * <p>This is a pure utility class with no instances.
 */
public final class DataSize {

    private DataSize() {}

    /** One kilobyte in bytes (1024). */
    public static final long KB = 1024L;

    /** One megabyte in bytes (1024 * 1024 = 1,048,576). */
    public static final long MB = 1024L * 1024;

    /** One gigabyte in bytes (1024 * 1024 * 1024 = 1,073,741,824). */
    public static final long GB = 1024L * 1024 * 1024;

    /**
     * Converts megabytes to bytes.
     *
     * @param mb the number of megabytes
     * @return the equivalent number of bytes
     */
    public static long megabytes(long mb) {
        return mb * MB;
    }

    /**
     * Converts gigabytes to bytes.
     *
     * @param gb the number of gigabytes
     * @return the equivalent number of bytes
     */
    public static long gigabytes(long gb) {
        return gb * GB;
    }

    /**
     * Formats a byte count into a human-readable string.
     *
     * <p>The output uses the largest unit where the value is {@code >= 1.0},
     * formatted to one decimal place. Examples:
     * <ul>
     *   <li>{@code toHumanReadable(0)} returns {@code "0 B"}
     *   <li>{@code toHumanReadable(1024)} returns {@code "1.0 KB"}
     *   <li>{@code toHumanReadable(megabytes(256))} returns {@code "256.0 MB"}
     *   <li>{@code toHumanReadable(gigabytes(2))} returns {@code "2.0 GB"}
     * </ul>
     *
     * @param bytes the byte count (may be zero or positive)
     * @return a formatted string such as {@code "256.0 MB"} or {@code "42 B"}
     */
    public static String toHumanReadable(long bytes) {
        if (bytes >= GB) {
            return String.format("%.1f GB", (double) bytes / GB);
        }
        if (bytes >= MB) {
            return String.format("%.1f MB", (double) bytes / MB);
        }
        if (bytes >= KB) {
            return String.format("%.1f KB", (double) bytes / KB);
        }
        return bytes + " B";
    }
}
