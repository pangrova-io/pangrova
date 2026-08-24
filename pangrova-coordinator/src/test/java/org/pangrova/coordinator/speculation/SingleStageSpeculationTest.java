package org.pangrova.coordinator.speculation;

import org.pangrova.coordinator.task.TaskDuration;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies speculative execution for single-stage queries with enhanced heuristics:
 * progress-rate-based detection, data skew triggers, and configurable thresholds.
 */
class SingleStageSpeculationTest {

    @Test
    void singleStageSlowTask_elapsedTimeDetection_triggersSpeculation() {
        var detector = new StragglerDetector();
        // 3 fast tasks completed in ~5s, 1 slow task running for 50s
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false),
                new TaskDuration("t2", 6_000, true, false),
                new TaskDuration("t3", 5_500, true, false),
                new TaskDuration("slow", 50_000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).containsExactly("slow");
    }

    @Test
    void progressRateDetection_slowProgressTask_triggersSpeculation() {
        var config = new SpeculationConfig(true, 5_000L, true, false,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER, Map.of(),
                SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER);
        var detector = new StragglerDetector(config);

        // Completed tasks: 100% progress in 5s → rate = 0.2/s
        // Slow task: 5% progress in 12s → rate = 0.004/s (well below 0.5 * 0.2 = 0.1)
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false, 1.0, -1L),
                new TaskDuration("t2", 5_000, true, false, 1.0, -1L),
                new TaskDuration("t3", 5_000, true, false, 1.0, -1L),
                new TaskDuration("slow", 12_000, false, false, 0.05, -1L));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).containsExactly("slow");
    }

    @Test
    void dataSkewDetection_largeInputTask_triggersSpeculation() {
        var config = new SpeculationConfig(true, 5_000L, false, true,
                3.0, Map.of(), SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER);
        var detector = new StragglerDetector(config);

        // Median input = 100MB. Slow task has 500MB (5x median, above 3x threshold)
        long mb100 = 100L * 1024 * 1024;
        long mb500 = 500L * 1024 * 1024;
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false, -1.0, mb100),
                new TaskDuration("t2", 5_000, true, false, -1.0, mb100),
                new TaskDuration("t3", 5_000, true, false, -1.0, mb100),
                new TaskDuration("skewed", 12_000, false, false, -1.0, mb500));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).containsExactly("skewed");
    }

    @Test
    void configurableThreshold_perQueryType_usesCustomMultiplier() {
        // SCAN queries use a tighter threshold (1.2x) than default (1.5x)
        var config = new SpeculationConfig(true, 5_000L, true, true,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER,
                Map.of("SCAN", 1.2), SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER);
        var detector = new StragglerDetector(config);

        // Median completed = 5000ms. Threshold for SCAN = 5000 * 1.2 = 6000ms
        // Task at 11000ms exceeds both SCAN threshold (6000) and default (7500)
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false),
                new TaskDuration("t2", 5_000, true, false),
                new TaskDuration("t3", 5_000, true, false),
                new TaskDuration("borderline", 11_000, false, false));

        // With SCAN type: threshold = 5000 * 1.2 = 6000 → detected
        List<String> scanStragglers = detector.detectStragglers(tasks, "SCAN");
        assertThat(scanStragglers).containsExactly("borderline");

        // With default type: threshold = 5000 * 1.5 = 7500 → still detected (11000 > 7500)
        List<String> defaultStragglers = detector.detectStragglers(tasks, "AGGREGATE");
        assertThat(defaultStragglers).containsExactly("borderline");
    }

    @Test
    void configurableThreshold_tighterScanThreshold_detectsMoreStragglers() {
        var config = new SpeculationConfig(true, 5_000L, false, false,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER,
                Map.of("SCAN", 1.1), 2.0);
        var detector = new StragglerDetector(config);

        // Median = 10000ms. SCAN threshold = 10000 * 1.1 = 11000. Default = 10000 * 2.0 = 20000
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 10_000, true, false),
                new TaskDuration("t2", 10_000, true, false),
                new TaskDuration("t3", 10_000, true, false),
                new TaskDuration("mid", 15_000, false, false));

        // SCAN: 15000 > 11000 → detected
        assertThat(detector.detectStragglers(tasks, "SCAN")).containsExactly("mid");

        // Default (AGGREGATE): 15000 < 20000 → NOT detected
        assertThat(detector.detectStragglers(tasks, "AGGREGATE")).isEmpty();
    }

    @Test
    void noProgressInfo_fallsBackToElapsedTime() {
        var config = new SpeculationConfig(true, 5_000L, true, true,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER, Map.of(),
                SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER);
        var detector = new StragglerDetector(config);

        // All tasks have unknown progress (-1.0) and unknown input (-1L)
        // Falls back to elapsed-time detection only
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false),
                new TaskDuration("t2", 5_000, true, false),
                new TaskDuration("t3", 5_000, true, false),
                new TaskDuration("slow", 50_000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).containsExactly("slow");
    }

    @Test
    void dataSkewDisabled_doesNotTriggerOnLargeInput() {
        var config = new SpeculationConfig(true, 5_000L, false, false,
                SpeculationConfig.DEFAULT_DATA_SKEW_MULTIPLIER, Map.of(),
                SpeculationConfig.DEFAULT_MEDIAN_MULTIPLIER);
        var detector = new StragglerDetector(config);

        long mb100 = 100L * 1024 * 1024;
        long mb500 = 500L * 1024 * 1024;
        // Task has large input but duration is below elapsed threshold
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 10_000, true, false, -1.0, mb100),
                new TaskDuration("t2", 10_000, true, false, -1.0, mb100),
                new TaskDuration("t3", 10_000, true, false, -1.0, mb100),
                new TaskDuration("big", 12_000, false, false, -1.0, mb500));

        // Duration 12000 < threshold 10000*1.5=15000, and skew disabled → not detected
        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).isEmpty();
    }

    @Test
    void belowCompletionThreshold_noDetection() {
        var detector = new StragglerDetector();
        // Only 50% completed (below 75% threshold)
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false),
                new TaskDuration("t2", 50_000, false, false),
                new TaskDuration("t3", 40_000, false, false),
                new TaskDuration("t4", 30_000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).isEmpty();
    }

    @Test
    void existingSpeculativeCopy_skipped() {
        var detector = new StragglerDetector();
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 5_000, true, false),
                new TaskDuration("t2", 5_000, true, false),
                new TaskDuration("t3", 5_000, true, false),
                new TaskDuration("already-spec", 50_000, false, true));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).isEmpty();
    }

    @Test
    void belowMinDuration_notDetected() {
        var detector = new StragglerDetector();
        // Task running for only 5s (below 10s minimum)
        List<TaskDuration> tasks = List.of(
                new TaskDuration("t1", 1_000, true, false),
                new TaskDuration("t2", 1_000, true, false),
                new TaskDuration("t3", 1_000, true, false),
                new TaskDuration("short", 5_000, false, false));

        List<String> stragglers = detector.detectStragglers(tasks);

        assertThat(stragglers).isEmpty();
    }

    @Test
    void configValidation_rejectsInvalidValues() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SpeculationConfig(true, -1));

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SpeculationConfig(true, 5000, true, true, -1.0, Map.of(), 1.5));

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SpeculationConfig(true, 5000, true, true, 3.0, Map.of(), 0.0));
    }

    @Test
    void medianMultiplierForQueryType_fallsBackToDefault() {
        var config = new SpeculationConfig(true, 5_000L, true, true,
                3.0, Map.of("SCAN", 1.2), 1.5);

        assertThat(config.medianMultiplierForQueryType("SCAN")).isEqualTo(1.2);
        assertThat(config.medianMultiplierForQueryType("AGGREGATE")).isEqualTo(1.5);
        assertThat(config.medianMultiplierForQueryType(null)).isEqualTo(1.5);
    }

    @Test
    void taskDuration_progressRateCalculation() {
        // 50% progress in 10s → 0.05/s
        var task = new TaskDuration("t1", 10_000, false, false, 0.5, -1L);
        assertThat(task.progressRatePerSecond()).isCloseTo(0.05, org.assertj.core.data.Offset.offset(0.001));

        // Unknown progress → -1.0
        var unknown = new TaskDuration("t2", 10_000, false, false, -1.0, -1L);
        assertThat(unknown.progressRatePerSecond()).isEqualTo(-1.0);

        // Zero duration → -1.0
        var zeroDuration = new TaskDuration("t3", 0, false, false, 0.5, -1L);
        assertThat(zeroDuration.progressRatePerSecond()).isEqualTo(-1.0);
    }
}
