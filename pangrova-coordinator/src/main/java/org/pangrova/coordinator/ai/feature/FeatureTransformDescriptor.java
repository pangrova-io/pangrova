package org.pangrova.coordinator.ai.feature;

import java.util.Objects;

/**
 * Describes the input/output contract of a built-in feature transform.
 *
 * <p>Each descriptor captures the transform's type signature: what input type
 * it accepts, what output type it produces, how many arguments it requires,
 * and a human-readable description for diagnostics.</p>
 *
 * @param type            the transform type enum value
 * @param inputTypeFamily the expected input column type family (e.g., "VARCHAR", "NUMERIC")
 * @param outputType      the output column type produced (e.g., "ARRAY&lt;VARCHAR&gt;", "DOUBLE", "INT")
 * @param argumentCount   the number of SQL arguments the function expects (including the source column)
 * @param description     human-readable description of the transform behavior
 */
public record FeatureTransformDescriptor(
        FeatureTransformType type,
        String inputTypeFamily,
        String outputType,
        int argumentCount,
        String description
) {

    public FeatureTransformDescriptor {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(inputTypeFamily, "inputTypeFamily must not be null");
        Objects.requireNonNull(outputType, "outputType must not be null");
        Objects.requireNonNull(description, "description must not be null");
        if (inputTypeFamily.isBlank()) {
            throw new IllegalArgumentException("inputTypeFamily must not be blank");
        }
        if (outputType.isBlank()) {
            throw new IllegalArgumentException("outputType must not be blank");
        }
        if (argumentCount < 1) {
            throw new IllegalArgumentException("argumentCount must be at least 1, got " + argumentCount);
        }
        if (description.isBlank()) {
            throw new IllegalArgumentException("description must not be blank");
        }
    }

    /** Returns the SQL function name for this transform. */
    public String functionName() {
        return type.functionName();
    }
}
