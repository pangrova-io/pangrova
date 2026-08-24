package org.pangrova.coordinator.query.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for lease reclamation scheduler in {@link SplitAssignmentManager}.
 */
class LeaseReclamationSchedulerTest {

    private SplitAssignmentManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.stopLeaseReclamation();
        }
    }

    @Test
    void expiredLeasesReclaimedByScheduler() throws InterruptedException {
        // 1ms lease duration so leases expire almost immediately
        manager = new SplitAssignmentManager(1);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of()),
                new SplitAssignment(1, "s1", List.of())));

        // Request both splits (creates leases)
        Optional<SplitAssignment> s1 = manager.requestSplit("q1", 0, "w1", List.of());
        Optional<SplitAssignment> s2 = manager.requestSplit("q1", 0, "w2", List.of());
        assertTrue(s1.isPresent());
        assertTrue(s2.isPresent());

        // No more splits available
        assertFalse(manager.requestSplit("q1", 0, "w3", List.of()).isPresent());

        // Start scheduler with 10ms interval
        manager.startLeaseReclamation(10);

        // Wait for at least one reclamation cycle
        Thread.sleep(100);

        // Splits should be available again after reclamation
        Optional<SplitAssignment> reclaimed1 = manager.requestSplit("q1", 0, "w3", List.of());
        Optional<SplitAssignment> reclaimed2 = manager.requestSplit("q1", 0, "w3", List.of());
        assertTrue(reclaimed1.isPresent(), "First reclaimed split should be available");
        assertTrue(reclaimed2.isPresent(), "Second reclaimed split should be available");
    }

    @Test
    void completedSplitsNotReclaimedByScheduler() throws InterruptedException {
        manager = new SplitAssignmentManager(1);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of())));

        // Request and immediately complete
        Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w1", List.of());
        assertTrue(s.isPresent());
        manager.markSplitCompleted("q1", 0, 0);

        // Start scheduler
        manager.startLeaseReclamation(10);
        Thread.sleep(100);

        // No splits should be available (completed, not reclaimed)
        assertFalse(manager.requestSplit("q1", 0, "w2", List.of()).isPresent());
        assertTrue(manager.allSplitsCompleted("q1", 0));
    }

    @Test
    void manualReclaimExpiredLeases() {
        manager = new SplitAssignmentManager(1);
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of()),
                new SplitAssignment(1, "s1", List.of()),
                new SplitAssignment(2, "s2", List.of())));

        // Request all splits
        for (int i = 0; i < 3; i++) {
            manager.requestSplit("q1", 0, "w1", List.of());
        }

        // Wait for leases to expire
        try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Manual reclaim
        int reclaimed = manager.reclaimExpiredLeases();
        assertEquals(3, reclaimed);

        // All splits available again
        for (int i = 0; i < 3; i++) {
            assertTrue(manager.requestSplit("q1", 0, "w2", List.of()).isPresent());
        }
    }

    @Test
    void stopLeaseReclamationIsIdempotent() {
        manager = new SplitAssignmentManager();
        // Stop without start should not throw
        manager.stopLeaseReclamation();
        manager.stopLeaseReclamation();
    }
}
