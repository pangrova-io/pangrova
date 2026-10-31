package org.pangrova.indexing.core.config;

/**
 * Configuration for index evaluation operations.
 *
 * @param parallelism               max parallel evaluations (default 8)
 * @param evalTimeoutMs             per-split evaluation timeout in ms (default 5000)
 * @param circuitFailureThreshold   failures before circuit opens (default 5)
 * @param circuitOpenDurationSeconds seconds circuit stays open (default 60)
 * @param includeStale              whether to include STALE indexes in evaluation (default true)
 */
public record EvalConfig(
        int parallelism,
        long evalTimeoutMs,
        int circuitFailureThreshold,
        int circuitOpenDurationSeconds,
        boolean includeStale
) {

    public static final EvalConfig DEFAULT = new EvalConfig(8, 5000, 5, 60, true);

    public EvalConfig {
        if (parallelism < 0) {
            throw new IllegalArgumentException("parallelism must be >= 0, got " + parallelism);
        }
        if (evalTimeoutMs < 1) {
            throw new IllegalArgumentException("evalTimeoutMs must be >= 1, got " + evalTimeoutMs);
        }
        if (circuitFailureThreshold < 1) {
            throw new IllegalArgumentException(
                    "circuitFailureThreshold must be >= 1, got " + circuitFailureThreshold);
        }
        if (circuitOpenDurationSeconds < 1) {
            throw new IllegalArgumentException(
                    "circuitOpenDurationSeconds must be >= 1, got " + circuitOpenDurationSeconds);
        }
    }
}
