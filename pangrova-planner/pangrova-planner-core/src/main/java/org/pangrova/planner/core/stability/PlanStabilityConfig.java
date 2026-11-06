package org.pangrova.planner.core.stability;

/**
 * Configuration for the plan stability subsystem
 * ({@link PlanCaptureRegistry} + {@link PlanBaselineRegistry}, orchestrated
 * by {@link PlanBaselineController}).
 *
 * @param maxCapturedQueries maximum distinct query hashes retained by the
 *                           bounded LRU capture registry; oldest queries are
 *                           evicted when the cap is exceeded.
 * @param maxPlansPerQuery   maximum recent plan-hash records retained per
 *                           query hash (oldest are evicted when exceeded).
 *                           This bounds the memory cost of a query whose plan
 *                           keeps changing.
 * @param captureEnabled     when {@code false}, the capture registry is
 *                           bypassed entirely (no plan choices are recorded).
 * @param baselineEnabled    when {@code false}, baseline lookup is skipped
 *                           and no warnings are emitted on plan drift.
 *                           Capture can run independently of baseline.
 */
public record PlanStabilityConfig(
        int maxCapturedQueries,
        int maxPlansPerQuery,
        boolean captureEnabled,
        boolean baselineEnabled) {

    /** Default: 1000 queries × 8 plans, capture and baseline both on. */
    public static final PlanStabilityConfig DEFAULT =
            new PlanStabilityConfig(1000, 8, true, true);

    /** All features off — manager becomes a no-op. */
    public static final PlanStabilityConfig DISABLED =
            new PlanStabilityConfig(0, 0, false, false);

    public PlanStabilityConfig {
        if (maxCapturedQueries < 0) {
            throw new IllegalArgumentException(
                    "maxCapturedQueries must be >= 0, got " + maxCapturedQueries);
        }
        if (maxPlansPerQuery < 0) {
            throw new IllegalArgumentException(
                    "maxPlansPerQuery must be >= 0, got " + maxPlansPerQuery);
        }
    }
}
