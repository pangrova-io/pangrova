package org.pangrova.planner.core.stability;

import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Orchestrates the plan stability subsystem: hashing, capture, and baseline
 * comparison for optimized query plans.
 *
 * <p>Renamed from {@code PlanBaselineController} to {@code PlanBaselineController}
 * per C3 naming rules (ban on *Manager suffix). This class controls baseline
 * capture and comparison — not general "management".</p>
 *
 * <p>After the planner produces an optimized RelNode, the caller invokes
 * {@link #onPlanOptimized(String, RelNode)} which:
 * <ol>
 *   <li>Computes the canonical query hash and plan hash via {@link PlanHasher}.</li>
 *   <li>Records the plan choice in the {@link PlanCaptureRegistry} (bounded LRU).</li>
 *   <li>When baseline checking is enabled, compares the observed plan hash
 *       against the registered baseline in {@link PlanBaselineRegistry} and
 *       logs a structured warning when a plan regression is detected.</li>
 * </ol>
 *
 * <p><strong>Detect-and-warn only (per G-CBO-06):</strong> this manager does
 * not enforce or revert to a baseline plan. True enforcement requires a fully
 * serializable plan IR and substitution back into the optimizer pipeline — that
 * is deferred as future scope, matching Oracle SPM's staged rollout (capture
 * → baseline → enforce) where enforce is the final, opt-in stage.</p>
 *
 * <p>Thread safety: this class is thread-safe. The capture registry and baseline
 * registry are both internally synchronized.</p>
 */
public final class PlanBaselineController {

    private static final Logger LOG = LoggerFactory.getLogger(PlanBaselineController.class);

    private final PlanStabilityConfig config;
    private final PlanCaptureRegistry captureRegistry;
    private final PlanBaselineRegistry baselineRegistry;

    /**
     * Creates a manager with the given configuration and fresh registries.
     */
    public PlanBaselineController(PlanStabilityConfig config) {
        this(config, new PlanCaptureRegistry(config), new PlanBaselineRegistry());
    }

    /**
     * Creates a manager with externally-supplied registries (useful for testing
     * or sharing registries across coordinator components).
     */
    public PlanBaselineController(PlanStabilityConfig config,
                                PlanCaptureRegistry captureRegistry,
                                PlanBaselineRegistry baselineRegistry) {
        this.config = Objects.requireNonNull(config, "config");
        this.captureRegistry = Objects.requireNonNull(captureRegistry, "captureRegistry");
        this.baselineRegistry = Objects.requireNonNull(baselineRegistry, "baselineRegistry");
    }

    /**
     * Called after the optimizer produces a plan. Computes hashes, records the
     * plan choice, and checks against any registered baseline.
     *
     * @param sql  the original SQL text (used to compute the query hash)
     * @param plan the optimized RelNode
     * @return the result of the stability check
     */
    public StabilityResult onPlanOptimized(String sql, RelNode plan) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(plan, "plan");

        String queryHash = PlanHasher.queryHash(sql);
        String planHash = PlanHasher.planHash(plan);

        // 1. Capture
        if (config.captureEnabled()) {
            PlanCaptureRecord captured = captureRegistry.record(queryHash, planHash);
            if (captured != null) {
                LOG.debug("event=plan_captured queryHash={} planHash={} ts={}",
                        queryHash, planHash, captured.capturedAt());
            }
        }

        // 2. Baseline comparison
        if (!config.baselineEnabled()) {
            return new StabilityResult(queryHash, planHash,
                    PlanBaselineRegistry.Verdict.NO_BASELINE, null);
        }

        PlanBaselineRegistry.Verdict verdict =
                baselineRegistry.compareAgainstBaseline(queryHash, planHash);

        if (verdict == PlanBaselineRegistry.Verdict.MISMATCH) {
            String baselinePlanHash = baselineRegistry.getBaseline(queryHash).orElse("?");
            LOG.warn("event=plan_baseline_mismatch queryHash={} "
                            + "baselinePlanHash={} observedPlanHash={} "
                            + "canonicalDigest={}",
                    queryHash, baselinePlanHash, planHash,
                    PlanHasher.canonicalDigest(plan));
            return new StabilityResult(queryHash, planHash, verdict, baselinePlanHash);
        }

        if (verdict == PlanBaselineRegistry.Verdict.MATCH) {
            LOG.debug("event=plan_baseline_match queryHash={} planHash={}",
                    queryHash, planHash);
        }

        return new StabilityResult(queryHash, planHash, verdict, null);
    }

    /**
     * Sets (or replaces) the baseline plan for the given SQL text.
     * Typically called when {@code plan_baseline_capture} is enabled.
     */
    public void captureBaseline(String sql, RelNode plan) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(plan, "plan");
        String queryHash = PlanHasher.queryHash(sql);
        String planHash = PlanHasher.planHash(plan);
        baselineRegistry.setBaseline(queryHash, planHash);
        LOG.info("event=plan_baseline_captured queryHash={} planHash={}", queryHash, planHash);
    }

    public PlanCaptureRegistry captureRegistry() {
        return captureRegistry;
    }

    public PlanBaselineRegistry baselineRegistry() {
        return baselineRegistry;
    }

    public PlanStabilityConfig config() {
        return config;
    }

    /**
     * Result of a single plan stability check, returned by
     * {@link #onPlanOptimized(String, RelNode)}.
     *
     * @param queryHash        the normalized SHA-256 of the SQL text
     * @param observedPlanHash the SHA-256 of the optimized plan structure
     * @param verdict          the comparison result against the baseline
     * @param baselinePlanHash the expected baseline hash (non-null only on MISMATCH)
     */
    public record StabilityResult(
            String queryHash,
            String observedPlanHash,
            PlanBaselineRegistry.Verdict verdict,
            String baselinePlanHash) {

        public boolean isRegression() {
            return verdict == PlanBaselineRegistry.Verdict.MISMATCH;
        }
    }
}
