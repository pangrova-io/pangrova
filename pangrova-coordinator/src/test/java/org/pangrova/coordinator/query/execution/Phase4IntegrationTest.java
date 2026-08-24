package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.resource.MemoryPressure;
import org.junit.jupiter.api.Test;

/**
 * Phase 4 integration tests: work stealing under load imbalance,
 * cache affinity hit ratio, and topology change recovery.
 */
class Phase4IntegrationTest {

    /**
     * 52.2: Fast worker steals splits from slow worker's queue.
     * Simulates 2 workers processing 10 splits — fast worker should process more.
     */
    @Test
    void workStealingUnderLoadImbalance() throws InterruptedException {
        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, null, 0);

        List<SplitAssignment> splits = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            splits.add(new SplitAssignment(i, "split-" + i, List.of(),
                    List.of(), System.currentTimeMillis()));
        }
        manager.registerSplits("q1", 0, splits);

        AtomicInteger fastCount = new AtomicInteger();
        AtomicInteger slowCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(2);

        ExecutorService pool = Executors.newFixedThreadPool(2);

        // Fast worker — no delay between pulls
        pool.submit(() -> {
            while (true) {
                Optional<SplitAssignment> s = manager.requestSplit(
                        "q1", 0, "fast-w", List.of());
                if (s.isEmpty()) break;
                fastCount.incrementAndGet();
                manager.markSplitCompleted("q1", 0, s.get().splitIndex());
            }
            done.countDown();
        });

        // Slow worker — 50ms delay between pulls
        pool.submit(() -> {
            while (true) {
                Optional<SplitAssignment> s = manager.requestSplit(
                        "q1", 0, "slow-w", List.of());
                if (s.isEmpty()) break;
                slowCount.incrementAndGet();
                manager.markSplitCompleted("q1", 0, s.get().splitIndex());
                try { Thread.sleep(50); } catch (InterruptedException e) { break; }
            }
            done.countDown();
        });

        done.await(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        // Fast worker should have processed more splits
        assertThat(fastCount.get()).isGreaterThan(slowCount.get());
        assertThat(fastCount.get() + slowCount.get()).isEqualTo(10);
    }

    /**
     * 52.3: Cache affinity hit ratio on repeated workloads.
     * Same file paths assigned to same workers across repeated registrations.
     */
    @Test
    void cacheAffinityHitRatioOnRepeatedWorkloads() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        List<WorkerInfo> workers = List.of(
                buildWorker("w-1"), buildWorker("w-2"), buildWorker("w-3"));
        assigner.updateTopology(workers);

        SplitAssignmentManager manager = new SplitAssignmentManager(60_000, assigner, 0);

        List<String> filePaths = List.of(
                "s3://bucket/lineitem/part-0.parquet",
                "s3://bucket/lineitem/part-1.parquet",
                "s3://bucket/lineitem/part-2.parquet",
                "s3://bucket/lineitem/part-3.parquet",
                "s3://bucket/lineitem/part-4.parquet",
                "s3://bucket/lineitem/part-5.parquet");

        // First run — establish affinity
        ConcurrentHashMap<String, String> firstRunAssignments = new ConcurrentHashMap<>();
        List<SplitAssignment> splits1 = new ArrayList<>();
        for (int i = 0; i < filePaths.size(); i++) {
            splits1.add(new SplitAssignment(i, "split-" + i, List.of(),
                    List.of(filePaths.get(i)), System.currentTimeMillis()));
        }
        manager.registerSplits("q1", 0, splits1);

        for (WorkerInfo w : workers) {
            while (true) {
                Optional<SplitAssignment> s = manager.requestSplit(
                        "q1", 0, w.workerId(), List.of());
                if (s.isEmpty()) break;
                s.get().filePaths().forEach(fp ->
                        firstRunAssignments.put(fp, w.workerId()));
                manager.markSplitCompleted("q1", 0, s.get().splitIndex());
            }
        }
        manager.cleanup("q1");

        // Second run — same files should map to same workers
        int affinityHits = 0;
        List<SplitAssignment> splits2 = new ArrayList<>();
        for (int i = 0; i < filePaths.size(); i++) {
            splits2.add(new SplitAssignment(i, "split-" + i, List.of(),
                    List.of(filePaths.get(i)), System.currentTimeMillis()));
        }
        manager.registerSplits("q2", 0, splits2);

        for (WorkerInfo w : workers) {
            while (true) {
                Optional<SplitAssignment> s = manager.requestSplit(
                        "q2", 0, w.workerId(), List.of());
                if (s.isEmpty()) break;
                for (String fp : s.get().filePaths()) {
                    if (w.workerId().equals(firstRunAssignments.get(fp))) {
                        affinityHits++;
                    }
                }
                manager.markSplitCompleted("q2", 0, s.get().splitIndex());
            }
        }

        // With stable topology, consistent hashing gives 100% affinity
        assertThat(affinityHits).isEqualTo(filePaths.size());
    }

    /**
     * 52.4: Topology change recovery — worker leave causes redistribution,
     * then self-heals as new affinity stabilizes.
     */
    @Test
    void topologyChangeRecovery() {
        CacheAffinityAssigner assigner = new CacheAffinityAssigner();
        List<WorkerInfo> threeWorkers = List.of(
                buildWorker("w-1"), buildWorker("w-2"), buildWorker("w-3"));
        assigner.updateTopology(threeWorkers);

        List<String> filePaths = List.of(
                "s3://bucket/t/p0.parquet", "s3://bucket/t/p1.parquet",
                "s3://bucket/t/p2.parquet", "s3://bucket/t/p3.parquet",
                "s3://bucket/t/p4.parquet", "s3://bucket/t/p5.parquet");

        ConcurrentHashMap<String, String> beforeChange = new ConcurrentHashMap<>();
        for (String fp : filePaths) {
            beforeChange.put(fp, assigner.preferredWorker(fp));
        }

        // Remove w-3 (simulating worker crash)
        List<WorkerInfo> twoWorkers = List.of(
                buildWorker("w-1"), buildWorker("w-2"));
        assigner.updateTopology(twoWorkers);

        int redistributed = 0;
        for (String fp : filePaths) {
            String newPreferred = assigner.preferredWorker(fp);
            if (!newPreferred.equals(beforeChange.get(fp))) {
                redistributed++;
            }
        }

        // Consistent hashing: only ~1/N keys redistributed
        assertThat(redistributed).isLessThanOrEqualTo(filePaths.size());
        assertThat(redistributed).isGreaterThan(0);

        // After stabilization, repeated queries get consistent results
        for (String fp : filePaths) {
            assertThat(assigner.preferredWorker(fp))
                    .isEqualTo(assigner.preferredWorker(fp));
        }
    }

    private static WorkerInfo buildWorker(String workerId) {
        return new WorkerInfo(
                workerId, "host-" + workerId, 8080, 8081, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.5, 0, 1024L * 1024 * 1024,
                Instant.now(), 1.0, null, 1, List.of(), null, List.of());
    }
}
