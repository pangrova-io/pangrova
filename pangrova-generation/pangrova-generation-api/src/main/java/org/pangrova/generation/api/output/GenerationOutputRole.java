package org.pangrova.generation.api.output;

import org.pangrova.generation.api.enums.GenerationOutputScope;

import java.util.Objects;

/** Required ordinal and distribution scope for one object role. */
public record GenerationOutputRole(int ordinal, GenerationOutputScope scope) {
    public GenerationOutputRole {
        Objects.requireNonNull(scope, "scope must not be null");
        if (ordinal < 0) {
            throw new IllegalArgumentException("role ordinal must not be negative");
        }
    }
}
