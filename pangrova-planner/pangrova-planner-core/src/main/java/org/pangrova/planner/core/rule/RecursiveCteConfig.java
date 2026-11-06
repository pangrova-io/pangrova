package org.pangrova.planner.core.rule;

/**
 * Configuration for recursive CTE execution.
 * Controls iteration limits to prevent infinite recursion.
 *
 * @param maxIterations maximum number of recursive iterations before aborting (default 1000)
 */
public record RecursiveCteConfig(int maxIterations) {

    /** Default iteration limit per SQL:1999 recommendation. */
    public static final int DEFAULT_MAX_ITERATIONS = 1000;

    public RecursiveCteConfig {
        if (maxIterations <= 0) {
            throw new IllegalArgumentException(
                    "maxIterations must be positive, got: " + maxIterations);
        }
    }

    /** Creates a config with the default iteration limit. */
    public static RecursiveCteConfig defaults() {
        return new RecursiveCteConfig(DEFAULT_MAX_ITERATIONS);
    }
}
