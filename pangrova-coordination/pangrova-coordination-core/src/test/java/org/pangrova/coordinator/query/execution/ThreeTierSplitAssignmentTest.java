package org.pangrova.coordinator.query.execution;

import org.pangrova.scheduler.api.placement.HealthState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests for 3-tier split assignment: cache-affine → locality → any.
 */
class ThreeTierSplitAssignmentTest {

    @Test
    void cacheAffinePreferredOverLocality() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w1"), buildWorker("w2")));

        // Find which worker file-A hashes to
        String preferredForFileA = assigner.preferredWorker("/data/file-A.parquet");

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of("host-w1"),
                        List.of("/data/file-A.parquet"), System.currentTimeMillis()),
                new SplitAssignment(1, "s1", List.of("host-w2"),
                        List.of("/data/file-B.parquet"), System.currentTimeMillis())));

        // Request from the preferred worker for file-A — should get cache-affine match
        Optional<SplitAssignment> s = manager.requestSplit("q1", 0, preferredForFileA, List.of());
        assertTrue(s.isPresent());
        assertTrue(s.get().filePaths().contains("/data/file-A.parquet"),
                "Should get cache-affine split for file-A");
    }

    @Test
    void fallbackToLocalityWhenNoCacheAffineMatch() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w1"), buildWorker("w2"), buildWorker("w3")));

        // Create splits with no file paths (no cache affinity possible)
        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of("host-a"), List.of(), System.currentTimeMillis()),
                new SplitAssignment(1, "s1", List.of("host-b"), List.of(), System.currentTimeMillis())));

        // Request with locality hint — should fall back to locality match
        Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of("host-a"));
        assertTrue(s.isPresent());
        assertTrue(s.get().dataLocations().contains("host-a"),
                "Should fall back to locality-aware split");
    }

    @Test
    void fallbackToAnyWhenNoLocalityMatch() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w1"), buildWorker("w2")));

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of("host-x"), List.of(), System.currentTimeMillis())));

        // Request with no matching locality — should fall back to any
        Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of("host-z"));
        assertTrue(s.isPresent(), "Should fall back to any available split");
    }

    @Test
    void noCacheAffinityAssignerFallsBackToLocalityThenAny() {
        // No assigner — should behave like 2-tier (locality → any)
        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, null);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of("host-a"),
                        List.of("/data/file.parquet"), System.currentTimeMillis())));

        Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of("host-a"));
        assertTrue(s.isPresent());
    }

    @Test
    void allThreeTiersExhausted() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w1")));

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of(), List.of(), System.currentTimeMillis())));

        // First request gets the split
        assertTrue(manager.requestSplit("q1", 0, "w1", List.of()).isPresent());
        // Second request — empty
        assertFalse(manager.requestSplit("q1", 0, "w1", List.of()).isPresent());
    }

    private static WorkerInfo buildWorker(String workerId) {
        return new WorkerInfo(
                workerId, "host-" + workerId, 8080, 8081, 0,
                org.pangrova.scheduler.api.placement.HealthState.ACTIVE,
                org.pangrova.common.value.MemoryPressure.NONE,
                0.0, 0, 1_000_000_000L,
                Instant.now(), 1.0, "default", 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }
}
