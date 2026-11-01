package org.pangrova.spi.function.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/**
 * Describes the signature of a user-defined function.
 *
 * <p>Carries enough information for the engine to resolve, type-check, and
 * dispatch the function during query planning and execution.</p>
 *
 * @param name           function name as it appears in SQL (case-insensitive)
 * @param returnType     return type name (e.g. {@code "BIGINT"}, {@code "VARCHAR"})
 * @param argumentTypes  ordered list of argument type names
 * @param deterministic  whether repeated calls with the same arguments produce
 *                       the same result; defaults to {@code true}
 * @param defaultNullBehavior whether any NULL input returns NULL before invoking the implementation
 * @param description    optional human-readable description for SHOW FUNCTIONS
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public record FunctionMetadata(
        String name,
        String returnType,
        List<String> argumentTypes,
        boolean deterministic,
        boolean defaultNullBehavior,
        String description) {

    private static final boolean DEFAULT_NULL_BEHAVIOR = true;

    public FunctionMetadata {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(returnType, "returnType must not be null");
        argumentTypes = argumentTypes == null ? List.of() : List.copyOf(argumentTypes);
        description = description == null ? "" : description;
    }

    /** Pure functions use the same strict NULL-input default as the execution engine. */
    public FunctionMetadata(String name, String returnType, List<String> argumentTypes,
                            boolean deterministic, String description) {
        this(name, returnType, argumentTypes, deterministic, DEFAULT_NULL_BEHAVIOR, description);
    }

    /**
     * Convenience constructor for a deterministic function with no description.
     */
    public FunctionMetadata(String name, String returnType, List<String> argumentTypes) {
        this(name, returnType, argumentTypes, true, DEFAULT_NULL_BEHAVIOR, "");
    }
}
