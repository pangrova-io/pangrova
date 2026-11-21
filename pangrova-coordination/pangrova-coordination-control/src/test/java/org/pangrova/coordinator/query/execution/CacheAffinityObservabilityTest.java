package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.common.enums.MemoryPressure;
import org.junit.jupiter.api.Test;

/**
 * Tests for cache affinity observability metrics.
 * Verifies that metrics are correctly incremented on cache hit/miss events.
 */
class CacheAffinityObservabilityTest {

    @Test
    void cacheAffinityHitMetricIncremented() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w-1"), buildWorker("w-2")));

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner, 0);

        // Create a split with a file path that hashes to w-1
        String filePath = findFileForWorker(assigner, "w-1");
        long now = System.currentTimeMillis();
        SplitAssignment split = new SplitAssignment(0, "data", List.of(), List.of(filePath), now);
        manager.registerSplits("q1", 0, List.of(split));

        // Request from w-1 (the affine worker) — should be a cache affinity hit
        manager.requestSplit("q1", 0, "w-1", List.of());

        assertThat(manager.cacheAffinityHits()).isEqualTo(1);
        assertThat(manager.cacheAffinityMisses()).isZero();
    }

    @Test
    void cacheAffinityMissMetricIncremented() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w-1"), buildWorker("w-2")));

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner, 0);

        // Create a split with a file path that hashes to w-1
        String filePath = findFileForWorker(assigner, "w-1");
        long now = System.currentTimeMillis();
        SplitAssignment split = new SplitAssignment(0, "data", List.of(), List.of(filePath), now);
        manager.registerSplits("q1", 0, List.of(split));

        // Request from w-2 (not the affine worker) — should be a miss
        manager.requestSplit("q1", 0, "w-2", List.of());

        assertThat(manager.cacheAffinityHits()).isZero();
        assertThat(manager.cacheAffinityMisses()).isEqualTo(1);
    }

    @Test
    void stealDelayExpiredMetricIncremented() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w-1"), buildWorker("w-2")));

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner, 500);

        // Create a split with expired steal delay
        String filePath = findFileForWorker(assigner, "w-1");
        long pastDelay = System.currentTimeMillis() - 1_000;
        SplitAssignment split = new SplitAssignment(0, "data", List.of(), List.of(filePath), pastDelay);
        manager.registerSplits("q1", 0, List.of(split));

        // Request from w-2 (non-affine, steal delay expired)
        manager.requestSplit("q1", 0, "w-2", List.of());

        assertThat(manager.stealDelayExpiredCount()).isEqualTo(1);
    }

    /**
     * Finds a file path that the assigner maps to the given worker.
     * Brute-force search over candidate paths.
     */
    private static String findFileForWorker(CacheAffinityAssigner assigner, String targetWorkerId) {
        for (int i = 0; i < 1000; i++) {
            String candidate = "s3://bucket/table/part-" + i + ".parquet";
            if (targetWorkerId.equals(assigner.preferredWorker(candidate))) {
                return candidate;
            }
        }
        throw new AssertionError("Could not find a file path that hashes to " + targetWorkerId);
    }

    private static WorkerInfo buildWorker(String workerId) {
        return new WorkerInfo(
                workerId, "localhost", 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1_000_000_000L, Instant.now(),
                1.0, null, 2, null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }
}
