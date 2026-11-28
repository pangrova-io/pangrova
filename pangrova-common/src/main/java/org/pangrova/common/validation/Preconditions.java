package org.pangrova.common.validation;

import java.time.Duration;
import java.util.Collection;

/**
 * Static utility methods for validating preconditions.
 * Throws standard JDK exceptions on failure.
 */
public final class Preconditions {

    private static final int MAX_PORT = 65_535;

    private Preconditions() {}

    /**
     * Ensures that a reference is not null.
     *
     * @param reference the reference to check
     * @param message   the exception message if null
     * @param <T>       the reference type
     * @return the non-null reference
     * @throws NullPointerException if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference, String message) {
        if (reference == null) {
            throw new NullPointerException(message);
        }
        return reference;
    }

    /**
     * Ensures the truth of an expression involving method arguments.
     *
     * @param expression a boolean expression
     * @param message    the exception message if false
     * @throws IllegalArgumentException if {@code expression} is false
     */
    public static void checkArgument(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Ensures the truth of an expression involving object state.
     *
     * @param expression a boolean expression
     * @param message    the exception message if false
     * @throws IllegalStateException if {@code expression} is false
     */
    public static void checkState(boolean expression, String message) {
        if (!expression) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Ensures that a string is neither null nor blank (empty or whitespace-only).
     *
     * @param value   the string to check
     * @param message the exception message if null or blank
     * @return the validated non-blank string
     * @throws IllegalArgumentException if {@code value} is null or blank
     */
    public static String checkNotBlank(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    /** Returns the untrimmed field value, or names the missing or blank field in an argument error. */
    public static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    /** Accepts absent text and rejects blank values with a field-specific argument error. */
    public static void requireOptionalText(String value, String field) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(field + " must be null or non-blank");
        }
    }

    /** Returns a strictly positive duration without losing sub-millisecond precision. */
    public static Duration requirePositiveDuration(Duration value, String field) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    /** Returns the field value, or names the missing field in an argument error. */
    public static <T> T requireNonNullField(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " must not be null");
        }
        return value;
    }

    /**
     * Ensures that a value is strictly positive (greater than zero).
     *
     * @param value   the value to check
     * @param message the exception message if not positive
     * @throws IllegalArgumentException if {@code value} is zero or negative
     */
    public static void checkPositive(long value, String message) {
        if (value <= 0) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Ensures that a value is non-negative (zero or positive).
     *
     * @param value   the value to check
     * @param message the exception message if negative
     * @throws IllegalArgumentException if {@code value} is negative
     */
    public static void checkNonNegative(long value, String message) {
        if (value < 0) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Ensures that a collection is neither null nor empty.
     *
     * @param collection the collection to check
     * @param message    the exception message if null or empty
     * @param <T>        the element type
     * @return the validated non-empty collection
     * @throws IllegalArgumentException if {@code collection} is null or empty
     */
    public static <T> Collection<T> checkNotEmpty(Collection<T> collection, String message) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return collection;
    }

    /**
     * Ensures that a port number is in the valid client-facing range (1-65535).
     * <p>Port 0 (ephemeral) is NOT allowed; use {@link #checkBindPort(int, String)} for server-bind ports.
     *
     * @param port    the port number to check
     * @param message the exception message if out of range
     * @return the validated port number
     * @throws IllegalArgumentException if {@code port} is not in [1, 65535]
     */
    public static int checkPort(int port, String message) {
        if (port < 1 || port > MAX_PORT) {
            throw new IllegalArgumentException(message);
        }
        return port;
    }

    /**
     * Ensures that a port number is in the valid server-bind range (0-65535).
     * <p>Port 0 is allowed and means the OS will pick an ephemeral port.
     *
     * @param port    the port number to check
     * @param message the exception message if out of range
     * @return the validated port number
     * @throws IllegalArgumentException if {@code port} is not in [0, 65535]
     */
    public static int checkBindPort(int port, String message) {
        if (port < 0 || port > MAX_PORT) {
            throw new IllegalArgumentException(message);
        }
        return port;
    }
}
