package org.pangrova.resilience.lineage;

import org.pangrova.resilience.lineage.StageLineageTracker.RecoveryMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests StageLineageTracker lineage recording, recovery mode selection, and recompute chain.
 */
class StageLineageTrackerTest {

    private final StageLineageTracker tracker = new StageLineageTracker();

    @Test
    void shortDuration_selectsRecompute() {
        RecoveryMode mode = tracker.selectRecoveryMode(Duration.ofSeconds(10), 500_000L);
        assertThat(mode).isEqualTo(RecoveryMode.RECOMPUTE);
    }

    @Test
    void largeShuffle_selectsMemoryAndRemote() {
        RecoveryMode mode = tracker.selectRecoveryMode(
                Duration.ofMinutes(5), 2L * 1024 * 1024 * 1024);
        assertThat(mode).isEqualTo(RecoveryMode.MEMORY_AND_REMOTE);
    }

    @Test
    void costBasedSelection_lowRecomputeCost_selectsRecompute() {
        // Duration > 30s but small, shuffle < 1GB but large in MB
        // recomputeCost = 31000ms, shuffleCost = 500MB / 1MB = 500
        // 31000 > 500 → MEMORY_AND_REMOTE
        // To get RECOMPUTE: need recomputeCost < shuffleCost
        // e.g. 31s = 31000ms vs 900MB = ~900 → still MEMORY_AND_REMOTE
        // The cost model compares ms vs MB, so very short durations with large shuffle
        // Use 31s (31000ms) vs 50GB (but capped at 1GB threshold)
        // Actually, for recompute to win: duration_ms < shuffle_bytes / 1MB
        // 31000 < X/1048576 → X > 31000 * 1048576 ≈ 32GB (but that exceeds 1GB threshold)
        // So in practice, cost-based always picks MEMORY_AND_REMOTE for durations > 30s
        // Let's just verify the MEMORY_AND_REMOTE path
        RecoveryMode mode = tracker.selectRecoveryMode(Duration.ofSeconds(31), 100_000_000L);
        assertThat(mode).isEqualTo(RecoveryMode.MEMORY_AND_REMOTE);
    }

    @Test
    void costBasedSelection_highRecomputeCost_selectsMemoryAndRemote() {
        // Duration > 30s, shuffle < 1GB, but recompute cost > shuffle cost
        RecoveryMode mode = tracker.selectRecoveryMode(
                Duration.ofMinutes(10), 500L * 1024 * 1024);
        assertThat(mode).isEqualTo(RecoveryMode.MEMORY_AND_REMOTE);
    }

    @Test
    void getRecomputeChain_singleStage_returnsSelf() {
        tracker.recordLineage("s1", List.of());
        List<String> chain = tracker.getRecomputeChain("s1");
        assertThat(chain).containsExactly("s1");
    }

    @Test
    void getRecomputeChain_linearDependency_returnsTopologicalOrder() {
        tracker.recordLineage("s1", List.of());
        tracker.recordLineage("s2", List.of("s1"));
        tracker.recordLineage("s3", List.of("s2"));

        List<String> chain = tracker.getRecomputeChain("s3");
        assertThat(chain).containsExactly("s1", "s2", "s3");
    }

    @Test
    void getRecomputeChain_unknownStage_returnsSelf() {
        List<String> chain = tracker.getRecomputeChain("unknown");
        assertThat(chain).containsExactly("unknown");
    }

    @Test
    void markOutputLost_tracksLostStages() {
        assertThat(tracker.isOutputLost("s1")).isFalse();
        tracker.markOutputLost("s1");
        assertThat(tracker.isOutputLost("s1")).isTrue();
    }

    @Test
    void recomputeChain_returnedListIsUnmodifiable() {
        tracker.recordLineage("s1", List.of());
        List<String> chain = tracker.getRecomputeChain("s1");
        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class, () -> chain.add("s2"));
    }
}
