package org.pangrova.spi.function;

import org.pangrova.spi.annotation.Experimental;

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
 * @param description    optional human-readable description for SHOW FUNCTIONS
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public record FunctionMetadata(
        String name,
        String returnType,
        List<String> argumentTypes,
        boolean deterministic,
        String description) {

    public FunctionMetadata {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(returnType, "returnType must not be null");
        argumentTypes = argumentTypes == null ? List.of() : List.copyOf(argumentTypes);
        description = description == null ? "" : description;
    }

    /**
     * Convenience constructor for a deterministic function with no description.
     */
    public FunctionMetadata(String name, String returnType, List<String> argumentTypes) {
        this(name, returnType, argumentTypes, true, "");
    }
}
