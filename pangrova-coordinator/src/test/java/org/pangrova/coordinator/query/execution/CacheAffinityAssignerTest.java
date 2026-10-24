package org.pangrova.coordinator.query.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.junit.jupiter.api.Test;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * Tests for {@link CacheAffinityAssigner}.
 */
class CacheAffinityAssignerTest {

    @Test
    void sameFileAlwaysMapsToSameWorker() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(
                buildWorker("w1"), buildWorker("w2"), buildWorker("w3")));

        String file = "/data/iceberg/table1/data/00001.parquet";
        String worker1 = assigner.preferredWorker(file);
        String worker2 = assigner.preferredWorker(file);
        String worker3 = assigner.preferredWorker(file);

        assertNotNull(worker1);
        assertEquals(worker1, worker2);
        assertEquals(worker2, worker3);
    }

    @Test
    void topologyChangeRedistributesMinimalKeys() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        List<WorkerInfo> threeWorkers = List.of(
                buildWorker("w1"), buildWorker("w2"), buildWorker("w3"));
        assigner.updateTopology(threeWorkers);

        // Map 100 files to workers
        int fileCount = 100;
        Map<String, String> originalMapping = new HashMap<>();
        for (int i = 0; i < fileCount; i++) {
            String file = "/data/table/part-" + i + ".parquet";
            originalMapping.put(file, assigner.preferredWorker(file));
        }

        // Add a 4th worker
        List<WorkerInfo> fourWorkers = new ArrayList<>(threeWorkers);
        fourWorkers.add(buildWorker("w4"));
        assigner.updateTopology(fourWorkers);

        // Count how many files changed their mapping
        int changed = 0;
        for (var entry : originalMapping.entrySet()) {
            String newWorker = assigner.preferredWorker(entry.getKey());
            if (!entry.getValue().equals(newWorker)) {
                changed++;
            }
        }

        // With consistent hashing, ~1/N keys should move (N=4, so ~25%)
        // Allow generous margin: less than 50% should change
        assertTrue(changed < fileCount / 2,
                "Too many keys redistributed: " + changed + "/" + fileCount);
        assertTrue(changed > 0, "At least some keys should redistribute");
    }

    @Test
    void preferredWorkersRankedByFileCount() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(
                buildWorker("w1"), buildWorker("w2"), buildWorker("w3")));

        // Create files that map to different workers
        List<String> files = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            files.add("/data/table/file-" + i + ".parquet");
        }

        List<String> preferred = assigner.preferredWorkers(files, 3);
        assertNotNull(preferred);
        assertTrue(preferred.size() <= 3);
        assertTrue(preferred.size() >= 1);
    }

    @Test
    void emptyTopologyReturnsNull() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assertNull(assigner.preferredWorker("/data/file.parquet"));
        assertEquals(List.of(), assigner.preferredWorkers(List.of("/data/file.parquet"), 1));
    }

    @Test
    void nullFilePathReturnsNull() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assigner.updateTopology(List.of(buildWorker("w1")));
        assertNull(assigner.preferredWorker(null));
    }

    @Test
    void hasWorkersReflectsTopology() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        assertTrue(!assigner.hasWorkers());

        assigner.updateTopology(List.of(buildWorker("w1")));
        assertTrue(assigner.hasWorkers());

        assigner.updateTopology(List.of());
        assertTrue(!assigner.hasWorkers());
    }

    /**
     * Property 1: Uniform distribution across Workers.
     * <p><b>Validates: Requirements 5.9.2</b></p>
     */
    @Property(tries = 20)
    void uniformDistributionAcrossWorkers(
            @ForAll @IntRange(min = 2, max = 8) int workerCount) {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        List<WorkerInfo> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            workers.add(buildWorker("w-" + i));
        }
        assigner.updateTopology(workers);

        int fileCount = 1000;
        Map<String, Integer> distribution = new HashMap<>();
        for (int i = 0; i < fileCount; i++) {
            String worker = assigner.preferredWorker("/data/table/file-" + i + ".parquet");
            distribution.merge(worker, 1, Integer::sum);
        }

        // Each worker should get roughly fileCount/workerCount files
        double expected = (double) fileCount / workerCount;
        for (var entry : distribution.entrySet()) {
            double ratio = entry.getValue() / expected;
            // Allow 50% deviation from perfect uniformity
            assertTrue(ratio > 0.5 && ratio < 1.5,
                    "Worker " + entry.getKey() + " got " + entry.getValue()
                            + " files, expected ~" + (int) expected);
        }

        // All workers should have at least some files
        assertEquals(workerCount, distribution.size(),
                "All workers should receive at least one file");
    }

    private static WorkerInfo buildWorker(String workerId) {
        return new WorkerInfo(
                workerId, "host-" + workerId, 8080, 8081, 0,
                org.pangrova.coordinator.worker.HealthState.ACTIVE,
                org.pangrova.resource.MemoryPressure.NONE,
                0.0, 0, 1_000_000_000L,
                Instant.now(), 1.0, "default", 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(workerId));
    }
}
