package org.pangrova.coordinator.query.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.execution.split.StageSplitQueue;
import org.junit.jupiter.api.Test;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/**
 * Tests for {@link SplitAssignmentManager} and {@link StageSplitQueue}.
 */
class SplitAssignmentManagerTest {

    @Test
    void registerAndRequestSplits() {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        List<SplitAssignment> splits = List.of(
                new SplitAssignment(0, "split-0", List.of("host-a")),
                new SplitAssignment(1, "split-1", List.of("host-b")),
                new SplitAssignment(2, "split-2", List.of("host-a")));

        manager.registerSplits("q1", 0, splits);

        Optional<SplitAssignment> s1 = manager.requestSplit("q1", 0, "w1", List.of("host-a"));
        assertTrue(s1.isPresent());
        assertEquals("host-a", s1.get().dataLocations().get(0));

        Optional<SplitAssignment> s2 = manager.requestSplit("q1", 0, "w2", List.of("host-b"));
        assertTrue(s2.isPresent());
        assertEquals("host-b", s2.get().dataLocations().get(0));

        Optional<SplitAssignment> s3 = manager.requestSplit("q1", 0, "w1", List.of("host-c"));
        assertTrue(s3.isPresent()); // fallback to any

        Optional<SplitAssignment> s4 = manager.requestSplit("q1", 0, "w1", List.of());
        assertFalse(s4.isPresent()); // no more splits
    }

    @Test
    void localityPreference() {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        List<SplitAssignment> splits = List.of(
                new SplitAssignment(0, "split-0", List.of("host-a")),
                new SplitAssignment(1, "split-1", List.of("host-b")),
                new SplitAssignment(2, "split-2", List.of("host-a")));

        manager.registerSplits("q1", 0, splits);

        // Worker on host-a should get a host-a split first
        Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of("host-a"));
        assertTrue(s.isPresent());
        assertTrue(s.get().dataLocations().contains("host-a"));
    }

    @Test
    void leaseExpiryReturnsSplitToPendingQueue() {
        // Use a very short lease so it expires immediately
        SplitAssignmentManager manager = new SplitAssignmentManager(1);
        List<SplitAssignment> splits = List.of(
                new SplitAssignment(0, "split-0", List.of("host-a")),
                new SplitAssignment(1, "split-1", List.of("host-b")));

        manager.registerSplits("q1", 0, splits);

        // Request both splits (leases created with 1ms duration)
        Optional<SplitAssignment> s1 = manager.requestSplit("q1", 0, "w1", List.of());
        Optional<SplitAssignment> s2 = manager.requestSplit("q1", 0, "w2", List.of());
        assertTrue(s1.isPresent());
        assertTrue(s2.isPresent());

        // No more splits available
        assertFalse(manager.requestSplit("q1", 0, "w3", List.of()).isPresent());

        // Wait for leases to expire
        try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Reclaim expired leases
        int reclaimed = manager.reclaimExpiredLeases();
        assertEquals(2, reclaimed);

        // Splits should be available again
        assertTrue(manager.requestSplit("q1", 0, "w3", List.of()).isPresent());
        assertTrue(manager.requestSplit("q1", 0, "w3", List.of()).isPresent());
    }

    @Test
    void markCompletedPreventsReclaim() {
        SplitAssignmentManager manager = new SplitAssignmentManager(1);
        List<SplitAssignment> splits = List.of(
                new SplitAssignment(0, "split-0", List.of()));

        manager.registerSplits("q1", 0, splits);
        manager.requestSplit("q1", 0, "w1", List.of());

        // Complete the split before lease expires
        manager.markSplitCompleted("q1", 0, 0);

        try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Reclaim should find nothing — split was completed
        assertEquals(0, manager.reclaimExpiredLeases());
        assertTrue(manager.allSplitsCompleted("q1", 0));
    }

    @Test
    void cleanupRemovesAllStageQueuesForQuery() {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        manager.registerSplits("q1", 0, List.of(new SplitAssignment(0, "s0", List.of())));
        manager.registerSplits("q1", 1, List.of(new SplitAssignment(0, "s1", List.of())));
        manager.registerSplits("q2", 0, List.of(new SplitAssignment(0, "s2", List.of())));

        manager.cleanup("q1");

        // q1 stages should be gone
        assertFalse(manager.requestSplit("q1", 0, "w1", List.of()).isPresent());
        assertFalse(manager.requestSplit("q1", 1, "w1", List.of()).isPresent());

        // q2 should still be available
        assertTrue(manager.requestSplit("q2", 0, "w1", List.of()).isPresent());
    }

    @Test
    void allSplitsCompletedReturnsTrueOnlyWhenAllDone() {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        List<SplitAssignment> splits = List.of(
                new SplitAssignment(0, "s0", List.of()),
                new SplitAssignment(1, "s1", List.of()),
                new SplitAssignment(2, "s2", List.of()));

        manager.registerSplits("q1", 0, splits);
        assertFalse(manager.allSplitsCompleted("q1", 0));

        // Request and complete all splits
        for (int i = 0; i < 3; i++) {
            Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of());
            assertTrue(s.isPresent());
            manager.markSplitCompleted("q1", 0, s.get().splitIndex());
        }

        assertTrue(manager.allSplitsCompleted("q1", 0));
    }

    @Test
    void requestSplitFromUnknownStageReturnsEmpty() {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        assertFalse(manager.requestSplit("unknown", 99, "w1", List.of()).isPresent());
    }

    /**
     * Property 1: All registered splits are eventually assigned or reclaimed.
     * <p><b>Validates: Requirements 5.3, 5.5</b></p>
     */
    @Property(tries = 50)
    void allRegisteredSplitsEventuallyAssignedOrReclaimed(
            @ForAll @IntRange(min = 1, max = 20) int splitCount) {
        SplitAssignmentManager manager = new SplitAssignmentManager(1);
        List<SplitAssignment> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new SplitAssignment(i, "split-" + i, List.of()));
        }
        manager.registerSplits("q1", 0, splits);

        // Assign half the splits, complete them
        int assigned = 0;
        for (int i = 0; i < splitCount / 2; i++) {
            Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of());
            if (s.isPresent()) {
                manager.markSplitCompleted("q1", 0, s.get().splitIndex());
                assigned++;
            }
        }

        // Assign remaining but don't complete (let leases expire)
        Set<Integer> leased = ConcurrentHashMap.newKeySet();
        while (true) {
            Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w2", List.of());
            if (s.isEmpty()) break;
            leased.add(s.get().splitIndex());
        }

        // Wait for leases to expire
        try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        manager.reclaimExpiredLeases();

        // Now all reclaimed splits should be available again
        int reclaimedAndCompleted = 0;
        while (true) {
            Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w3", List.of());
            if (s.isEmpty()) break;
            manager.markSplitCompleted("q1", 0, s.get().splitIndex());
            reclaimedAndCompleted++;
        }

        assertEquals(splitCount, assigned + reclaimedAndCompleted,
                "All splits must be assigned and completed");
        assertTrue(manager.allSplitsCompleted("q1", 0));
    }

    /**
     * Property 2: Concurrent split requests never return the same split twice.
     * <p><b>Validates: Requirements 5.3</b></p>
     */
    @Property(tries = 20)
    void concurrentRequestsNeverReturnDuplicateSplits(
            @ForAll @IntRange(min = 5, max = 30) int splitCount) throws InterruptedException {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        List<SplitAssignment> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(new SplitAssignment(i, "split-" + i, List.of()));
        }
        manager.registerSplits("q1", 0, splits);

        int workerCount = 4;
        Set<Integer> assignedIndices = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);

        for (int w = 0; w < workerCount; w++) {
            String workerId = "w-" + w;
            executor.submit(() -> {
                try { latch.await(); } catch (InterruptedException e) { return; }
                while (true) {
                    Optional<SplitAssignment> s = manager.requestSplit("q1", 0, workerId, List.of());
                    if (s.isEmpty()) break;
                    boolean added = assignedIndices.add(s.get().splitIndex());
                    assertTrue(added, "Duplicate split assignment detected: " + s.get().splitIndex());
                    manager.markSplitCompleted("q1", 0, s.get().splitIndex());
                }
            });
        }

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(splitCount, assignedIndices.size());
        assertTrue(manager.allSplitsCompleted("q1", 0));
    }
}
