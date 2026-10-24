package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.CacheCatalogSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests cache-aware affinity scheduling: splits route to workers
 * consistently via consistent hashing.
 */
class CacheAffinityWithCacheStateTest {

    private CacheAffinityAssigner assigner;
    private List<WorkerInfo> workers;

    @BeforeEach
    void setUp() {
        assigner = new CacheAffinityAssigner();
        workers = List.of(
                buildWorker("w1"),
                buildWorker("w2"),
                buildWorker("w3"));
        assigner.updateTopology(workers);
    }

    @Test
    void preferredWorkerReturnsConsistentResult() {
        String filePath = "/data/table1/file1.parquet";
        String first = assigner.preferredWorker(filePath);
        String second = assigner.preferredWorker(filePath);

        assertThat(first).isNotNull();
        assertThat(first).isEqualTo(second);
    }

    @Test
    void nullFilePathReturnsNull() {
        assertThat(assigner.preferredWorker(null)).isNull();
    }

    @Test
    void emptyRingReturnsNull() {
        CacheAffinityAssigner empty = new CacheAffinityAssigner();
        assertThat(empty.preferredWorker("/data/file.parquet")).isNull();
    }

    @Test
    void preferredWorkersRanksMultipleFiles() {
        List<String> filePaths = List.of(
                "/data/table1/file1.parquet",
                "/data/table1/file2.parquet",
                "/data/table1/file3.parquet");

        List<String> ranked = assigner.preferredWorkers(filePaths, 3);

        assertThat(ranked).isNotEmpty();
        assertThat(ranked.size()).isLessThanOrEqualTo(3);
        // All returned workers should be from our topology
        assertThat(ranked).allMatch(w -> w.startsWith("w"));
    }

    @Test
    void preferredWorkersRespectsMaxCandidates() {
        List<String> filePaths = List.of("/data/table1/file1.parquet");
        List<String> ranked = assigner.preferredWorkers(filePaths, 1);
        assertThat(ranked).hasSize(1);
    }

    @Test
    void sameFileDeterministicallyRoutesToSameWorker() {
        String file = "/data/table1/file1.parquet";
        String first = assigner.preferredWorker(file);
        String second = assigner.preferredWorker(file);
        String third = assigner.preferredWorker(file);
        assertThat(first).isEqualTo(second).isEqualTo(third);
    }

    @Test
    void hasWorkersReturnsTrueAfterTopologyUpdate() {
        assertThat(assigner.hasWorkers()).isTrue();
    }

    @Test
    void hasWorkersReturnsFalseForEmptyRing() {
        CacheAffinityAssigner empty = new CacheAffinityAssigner();
        assertThat(empty.hasWorkers()).isFalse();
    }

    @Test
    void topologyChangeRebuildRing() {
        String file = "/data/test/file.parquet";
        String before = assigner.preferredWorker(file);
        assertThat(before).isNotNull();

        // Change topology to different workers
        assigner.updateTopology(List.of(
                buildWorker("w4"),
                buildWorker("w5")));

        // preferredWorker should still return a valid worker
        String after = assigner.preferredWorker(file);
        assertThat(after).isNotNull();
        assertThat(after).isIn("w4", "w5");
    }

    private static WorkerInfo buildWorker(String workerId) {
        return new WorkerInfo(
                workerId, "host-" + workerId, 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 1_000_000_000L,
                Instant.now(), 1.0, "default", 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }
}
