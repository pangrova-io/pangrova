package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.execution.split.StageSplitQueue;
import org.junit.jupiter.api.Test;

/**
 * Tests for the steal-delay mechanism in split assignment.
 * When steal-delay is configured, non-affine Workers cannot steal a split
 * until the hold period has expired, giving the cache-affine Worker a head start.
 */
class StealDelayMechanismTest {

    @Test
    void splitNotAvailableBeforeDelayExpires() {
        // Enqueue time = now, steal delay = 10 seconds — split should NOT be available
        long now = System.currentTimeMillis();
        SplitAssignment split = new SplitAssignment(0, "data-0", List.of(), List.of("s3://f1"), now);
        StageSplitQueue queue = new StageSplitQueue(List.of(split), 60_000, 10_000);

        // pollAnySplit should return empty because steal delay hasn't expired
        Optional<SplitAssignment> result = queue.pollAnySplit();
        assertThat(result).isEmpty();
        assertThat(queue.pendingCount()).isEqualTo(1);
    }

    @Test
    void splitAvailableAfterDelayExpires() {
        // Enqueue time = 2 seconds ago, steal delay = 1 second — split should be available
        long twoSecondsAgo = System.currentTimeMillis() - 2_000;
        SplitAssignment split = new SplitAssignment(0, "data-0", List.of(), List.of("s3://f1"), twoSecondsAgo);
        StageSplitQueue queue = new StageSplitQueue(List.of(split), 60_000, 1_000);

        Optional<SplitAssignment> result = queue.pollAnySplit();
        assertThat(result).isPresent();
        assertThat(result.get().splitIndex()).isEqualTo(0);
        assertThat(queue.pendingCount()).isZero();
    }

    @Test
    void stealDelayZeroDisablesDelay() {
        // stealDelayMs = 0 means no delay — split immediately available
        long now = System.currentTimeMillis();
        SplitAssignment split = new SplitAssignment(0, "data-0", List.of(), List.of("s3://f1"), now);
        StageSplitQueue queue = new StageSplitQueue(List.of(split), 60_000, 0);

        Optional<SplitAssignment> result = queue.pollAnySplit();
        assertThat(result).isPresent();
        assertThat(result.get().splitIndex()).isEqualTo(0);
    }

    @Test
    void localitySplitBypassesStealDelay() {
        // Even with steal-delay, locality-aware polling should still work
        long now = System.currentTimeMillis();
        SplitAssignment split = new SplitAssignment(0, "data-0", List.of("node-1"), List.of("s3://f1"), now);
        StageSplitQueue queue = new StageSplitQueue(List.of(split), 60_000, 10_000);

        // pollAnySplit blocked by steal delay
        assertThat(queue.pollAnySplit()).isEmpty();

        // pollLocalSplit should still work (locality bypasses steal delay)
        Optional<SplitAssignment> local = queue.pollLocalSplit(List.of("node-1"));
        assertThat(local).isPresent();
        assertThat(local.get().splitIndex()).isEqualTo(0);
    }

    @Test
    void stealDelayViaManager() {
        // Verify SplitAssignmentManager passes stealDelayMs to StageSplitQueue
        SplitAssignmentManager manager = new SplitAssignmentManager(
                60_000, (CacheAffinityAssigner) null, 10_000);

        long now = System.currentTimeMillis();
        SplitAssignment split = new SplitAssignment(0, "data-0", List.of(), List.of(), now);
        manager.registerSplits("q1", 0, List.of(split));

        // Should not be available — steal delay hasn't expired
        Optional<SplitAssignment> result = manager.requestSplit("q1", 0, "w-1", List.of());
        assertThat(result).isEmpty();
    }

    @Test
    void stealDelayExpiredSplitAvailableViaManager() {
        SplitAssignmentManager manager = new SplitAssignmentManager(
                60_000, (CacheAffinityAssigner) null, 500);

        long pastDelay = System.currentTimeMillis() - 1_000;
        SplitAssignment split = new SplitAssignment(0, "data-0", List.of(), List.of(), pastDelay);
        manager.registerSplits("q1", 0, List.of(split));

        // Steal delay expired — split should be available
        Optional<SplitAssignment> result = manager.requestSplit("q1", 0, "w-1", List.of());
        assertThat(result).isPresent();
    }
}
