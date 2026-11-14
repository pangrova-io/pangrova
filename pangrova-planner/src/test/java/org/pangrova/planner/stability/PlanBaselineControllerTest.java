package org.pangrova.planner.stability;

import org.pangrova.planner.enums.PlanBaselineVerdict;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlanResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PlanBaselineController}: plan capture, baseline comparison,
 * and regression detection through the planner integration.
 */
class PlanBaselineControllerTest {

    private PlanStabilityConfig config;
    private PlanCaptureRegistry captureRegistry;
    private PlanBaselineRegistry baselineRegistry;
    private PlanBaselineController manager;

    @BeforeEach
    void setUp() {
        config = PlanStabilityConfig.DEFAULT;
        captureRegistry = new PlanCaptureRegistry(config);
        baselineRegistry = new PlanBaselineRegistry();
        manager = new PlanBaselineController(config, captureRegistry, baselineRegistry);
    }

    @Test
    void queryHashesRetainLiteralContentsQuotedNamesAndCommentBoundaries() {
        assertThat(PlanHasher.queryHash("SELECT 'A  B'")).isNotEqualTo(PlanHasher.queryHash("select 'a b'"));
        assertThat(PlanHasher.queryHash("SELECT \"ID\"")).isNotEqualTo(PlanHasher.queryHash("SELECT \"id\""));
        assertThat(PlanHasher.queryHash("SELECT `a  b`")).isNotEqualTo(PlanHasher.queryHash("SELECT `a b`"));
        assertThat(PlanHasher.queryHash("SELECT 1 -- note\n + 1")).isNotEqualTo(PlanHasher.queryHash("SELECT 1 -- note + 1"));
    }

    @Test
    void onPlanOptimized_capturesAndReturnsNoBaseline() {
        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 1");

        PlanBaselineController.StabilityResult sr =
                manager.onPlanOptimized("SELECT 1", result.plan());

        assertThat(sr.queryHash()).isNotNull().hasSize(64);
        assertThat(sr.observedPlanHash()).isNotNull().hasSize(64);
        assertThat(sr.verdict()).isEqualTo(PlanBaselineVerdict.NO_BASELINE);
        assertThat(sr.isRegression()).isFalse();
        assertThat(captureRegistry.totalRecorded()).isEqualTo(1);
    }

    @Test
    void baselineMatch_whenPlanMatchesRegisteredBaseline() {
        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 1 + 1");

        // Capture baseline
        manager.captureBaseline("SELECT 1 + 1", result.plan());

        // Same plan should match
        PlanBaselineController.StabilityResult sr =
                manager.onPlanOptimized("SELECT 1 + 1", result.plan());

        assertThat(sr.verdict()).isEqualTo(PlanBaselineVerdict.MATCH);
        assertThat(sr.isRegression()).isFalse();
        assertThat(baselineRegistry.matchCount()).isEqualTo(1);
    }

    @Test
    void baselineMismatch_whenPlanDiffers() {
        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 1");

        // Register a fake baseline that won't match
        String queryHash = PlanHasher.queryHash("SELECT 1");
        baselineRegistry.setBaseline(queryHash, "fake-plan-hash-that-wont-match");

        PlanBaselineController.StabilityResult sr =
                manager.onPlanOptimized("SELECT 1", result.plan());

        assertThat(sr.verdict()).isEqualTo(PlanBaselineVerdict.MISMATCH);
        assertThat(sr.isRegression()).isTrue();
        assertThat(sr.baselinePlanHash()).isEqualTo("fake-plan-hash-that-wont-match");
        assertThat(baselineRegistry.mismatchCount()).isEqualTo(1);
    }

    @Test
    void captureDisabled_doesNotRecord() {
        var disabledConfig = new PlanStabilityConfig(1000, 8, false, true);
        var disabledCapture = new PlanCaptureRegistry(disabledConfig);
        var mgr = new PlanBaselineController(disabledConfig, disabledCapture, baselineRegistry);

        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 42");
        mgr.onPlanOptimized("SELECT 42", result.plan());

        assertThat(disabledCapture.totalRecorded()).isZero();
    }

    @Test
    void baselineDisabled_skipsComparison() {
        var noBaselineConfig = new PlanStabilityConfig(1000, 8, true, false);
        var mgr = new PlanBaselineController(noBaselineConfig, captureRegistry, baselineRegistry);

        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 1");

        // Set a baseline that would mismatch
        String queryHash = PlanHasher.queryHash("SELECT 1");
        baselineRegistry.setBaseline(queryHash, "fake-hash");

        PlanBaselineController.StabilityResult sr =
                mgr.onPlanOptimized("SELECT 1", result.plan());

        // Even though baseline is set, comparison is skipped
        assertThat(sr.verdict()).isEqualTo(PlanBaselineVerdict.NO_BASELINE);
        assertThat(sr.isRegression()).isFalse();
        // But capture still works
        assertThat(captureRegistry.totalRecorded()).isEqualTo(1);
    }

    @Test
    void captureBaseline_setsBaselineAndSubsequentPlanMatches() {
        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 2 + 3");

        manager.captureBaseline("SELECT 2 + 3", result.plan());
        assertThat(baselineRegistry.size()).isEqualTo(1);

        // Re-plan the same query — should match
        PlanResult result2 = planner.plan("SELECT 2 + 3");
        PlanBaselineController.StabilityResult sr =
                manager.onPlanOptimized("SELECT 2 + 3", result2.plan());

        assertThat(sr.verdict()).isEqualTo(PlanBaselineVerdict.MATCH);
    }

    @Test
    void planHashDeterministic_sameQueryProducesSameHash() {
        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult r1 = planner.plan("SELECT 1 AS x");
        PlanResult r2 = planner.plan("SELECT 1 AS x");

        String h1 = PlanHasher.planHash(r1.plan());
        String h2 = PlanHasher.planHash(r2.plan());

        assertThat(h1).isEqualTo(h2);
    }

    @Test
    void queryHashNormalized_whitespaceInsensitive() {
        String h1 = PlanHasher.queryHash("SELECT 1");
        String h2 = PlanHasher.queryHash("  select   1  ");

        assertThat(h1).isEqualTo(h2);
    }

    @Test
    void captureRegistry_tracksPlanDrift() {
        PangrovaPlanner planner = new PangrovaPlanner();

        PlanResult r1 = planner.plan("SELECT 1");
        PlanResult r2 = planner.plan("SELECT 2");

        // Record same query hash with two different plans
        String queryHash = PlanHasher.queryHash("SELECT drift");
        String planHash1 = PlanHasher.planHash(r1.plan());
        String planHash2 = PlanHasher.planHash(r2.plan());

        captureRegistry.record(queryHash, planHash1);
        captureRegistry.record(queryHash, planHash2);

        assertThat(captureRegistry.totalRecorded()).isEqualTo(2);
        assertThat(captureRegistry.distinctPlanShifts()).isEqualTo(1);
        assertThat(captureRegistry.recentPlans(queryHash)).hasSize(2);
    }

    @Test
    void baselineRegistry_removeBaseline() {
        String queryHash = PlanHasher.queryHash("SELECT remove_me");
        baselineRegistry.setBaseline(queryHash, "some-plan-hash");
        assertThat(baselineRegistry.size()).isEqualTo(1);

        boolean removed = baselineRegistry.removeBaseline(queryHash);
        assertThat(removed).isTrue();
        assertThat(baselineRegistry.size()).isZero();
    }

    @Test
    void disabledConfig_managerBecomesNoOp() {
        var disabledConfig = PlanStabilityConfig.DISABLED;
        var disabledCapture = new PlanCaptureRegistry(disabledConfig);
        var mgr = new PlanBaselineController(disabledConfig, disabledCapture, baselineRegistry);

        PangrovaPlanner planner = new PangrovaPlanner();
        PlanResult result = planner.plan("SELECT 1");
        PlanBaselineController.StabilityResult sr =
                mgr.onPlanOptimized("SELECT 1", result.plan());

        assertThat(sr.verdict()).isEqualTo(PlanBaselineVerdict.NO_BASELINE);
        assertThat(disabledCapture.totalRecorded()).isZero();
    }
}
