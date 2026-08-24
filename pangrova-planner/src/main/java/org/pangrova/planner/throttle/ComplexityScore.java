package org.pangrova.planner.throttle;

/**
 * Normalized query complexity score in [0.0, 1.0].
 */
public record ComplexityScore(double value) {

    public ComplexityScore {
        if (value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(
                    "Score must be in [0.0, 1.0], got: " + value);
        }
    }
}
