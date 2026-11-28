package org.pangrova.planner.adaptivequery;

import org.pangrova.planner.api.enums.JoinStrategy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AdaptiveQueryExecutorTest {

    private AdaptiveQueryExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new AdaptiveQueryExecutor();
    }

    // --- shouldReOptimize ---

    @Test
    void actualGreaterThan2x_triggersReOptimization() {
        var stats = stats(100, 201);
        assertThat(executor.shouldReOptimize(stats)).isTrue();
    }

    @Test
    void actualLessThan0_5x_triggersReOptimization() {
        var stats = stats(100, 49);
        assertThat(executor.shouldReOptimize(stats)).isTrue();
    }

    @Test
    void actualWithinRange_noReOptimization() {
        var stats = stats(100, 150);
        assertThat(executor.shouldReOptimize(stats)).isFalse();
    }

    @Test
    void exactBoundary2x_notTriggered() {
        var stats = stats(100, 200);
        assertThat(executor.shouldReOptimize(stats)).isFalse();
    }

    @Test
    void exactBoundary0_5x_notTriggered() {
        var stats = stats(100, 50);
        assertThat(executor.shouldReOptimize(stats)).isFalse();
    }

    @Test
    void estimatedZero_actualNonZero_triggersReOptimization() {
        var stats = stats(0, 100);
        assertThat(executor.shouldReOptimize(stats)).isTrue();
    }

    @Test
    void estimatedZero_actualZero_noReOptimization() {
        var stats = stats(0, 0);
        assertThat(executor.shouldReOptimize(stats)).isFalse();
    }

    // --- evaluateJoinStrategy ---

    @Test
    void buildSideBelowThreshold_switchToBroadcast() {
        long belowThreshold = AdaptiveQueryExecutor.BROADCAST_THRESHOLD - 1;
        assertThat(executor.evaluateJoinStrategy(belowThreshold, JoinStrategy.SORT_MERGE_JOIN))
                .isEqualTo(JoinStrategy.BROADCAST_HASH_JOIN);
    }

    @Test
    void buildSideAtThreshold_keepCurrent() {
        assertThat(executor.evaluateJoinStrategy(
                AdaptiveQueryExecutor.BROADCAST_THRESHOLD, JoinStrategy.SORT_MERGE_JOIN))
                .isEqualTo(JoinStrategy.SORT_MERGE_JOIN);
    }

    @Test
    void buildSideAboveThreshold_keepCurrent() {
        long aboveThreshold = AdaptiveQueryExecutor.BROADCAST_THRESHOLD + 1;
        assertThat(executor.evaluateJoinStrategy(aboveThreshold, JoinStrategy.HASH_JOIN))
                .isEqualTo(JoinStrategy.HASH_JOIN);
    }

    // --- processStageCompletion ---

    @Test
    void stageCompletion_triggered_withJoinSwitch() {
        var stats = stats(100, 5);
        var result = executor.processStageCompletion(stats, JoinStrategy.SORT_MERGE_JOIN);

        assertThat(result.triggered()).isTrue();
        assertThat(result.reason()).isNotEmpty();
        assertThat(result.newJoinStrategy()).contains(JoinStrategy.BROADCAST_HASH_JOIN);
    }

    @Test
    void stageCompletion_triggered_noJoinSwitch() {
        long bigActual = AdaptiveQueryExecutor.BROADCAST_THRESHOLD * 3;
        var stats = stats(AdaptiveQueryExecutor.BROADCAST_THRESHOLD, bigActual);
        var result = executor.processStageCompletion(stats, JoinStrategy.SORT_MERGE_JOIN);

        assertThat(result.triggered()).isTrue();
        assertThat(result.newJoinStrategy()).isEmpty();
    }

    @Test
    void stageCompletion_notTriggered() {
        var stats = stats(100, 150);
        var result = executor.processStageCompletion(stats, JoinStrategy.SORT_MERGE_JOIN);

        assertThat(result.triggered()).isFalse();
        assertThat(result.reason()).isEmpty();
        assertThat(result.newJoinStrategy()).isEmpty();
    }

    @Test
    void parallelismRecommendationSaturatesBeforeNarrowingToInt() {
        long targetTaskBytes = 256L * 1024 * 1024;
        long intOverflowBytes = (1L + Integer.MAX_VALUE) * targetTaskBytes;
        assertThat(executor.evaluateParallelism(stats(intOverflowBytes, intOverflowBytes), 16))
                .isEqualTo(1024);
        assertThat(executor.evaluateParallelism(stats(Long.MAX_VALUE, Long.MAX_VALUE), 16))
                .isEqualTo(1024);
    }

    // --- Helpers ---

    private ShuffleStageStats stats(long estimated, long actual) {
        return new ShuffleStageStats("stage-1", actual, actual, estimated, estimated);
    }
}
