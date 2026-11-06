package org.pangrova.planner.core.validation;

import java.util.List;

/**
 * Immutable result of validating a Substrait plan against native engine capabilities.
 *
 * @param status           overall validation outcome
 * @param unsupportedNodes list of nodes the native engine cannot handle (empty when FULLY_NATIVE)
 */
public record PlanValidationResult(
        ValidationStatus status,
        List<UnsupportedNode> unsupportedNodes
) {
    /** Compact constructor enforces immutability via {@code List.copyOf()}. */
    public PlanValidationResult {
        unsupportedNodes = unsupportedNodes == null
                ? List.of()
                : List.copyOf(unsupportedNodes);
    }
}
