package org.pangrova.coordinator.query.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.execution.split.SplitAssignment;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.junit.jupiter.api.Test;

/**
 * Tests for pull-mode split dispatch via {@link SplitAssignmentManager}.
 * All leaf stages use pull mode — workers request splits on demand.
 */
class PullModeDispatchTest {

    @Test
    void pullModeRegistersAllSplitsAndWorkersCanPullThem() {
        SplitAssignmentManager manager = new SplitAssignmentManager();

        List<SplitAssignment> splits = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            splits.add(new SplitAssignment(i, "split-" + i, List.of()));
        }
        manager.registerSplits("q1", 0, splits);

        // Simulate 4 workers pulling splits until empty (work stealing)
        int[] workerPulled = new int[4];
        boolean anyAvailable = true;
        while (anyAvailable) {
            anyAvailable = false;
            for (int w = 0; w < 4; w++) {
                Optional<SplitAssignment> s = manager.requestSplit("q1", 0, "w-" + w, List.of());
                if (s.isPresent()) {
                    workerPulled[w]++;
                    manager.markSplitCompleted("q1", 0, s.get().splitIndex());
                    anyAvailable = true;
                }
            }
        }

        int totalPulled = 0;
        for (int count : workerPulled) totalPulled += count;
        assertEquals(10, totalPulled, "All 10 splits should be pulled");
        assertTrue(manager.allSplitsCompleted("q1", 0));
    }

    @Test
    void pullModeTaskCarriesNoEmbeddedSplitsInCanonicalContract() {
        var req = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "task-1", "q1", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertTrue(req.taskRequest().getSplitsList().isEmpty(),
                "Pull mode tasks should have no embedded splits");
    }

    @Test
    void unregisteredStageReturnsEmptyOnRequest() {
        SplitAssignmentManager manager = new SplitAssignmentManager();

        // No splits registered — requestSplit returns empty
        assertFalse(manager.requestSplit("q1", 0, "w1", List.of()).isPresent(),
                "No splits should be available for unregistered stage");
    }

    @Test
    void cleanupRemovesAllStageQueuesForQuery() {
        SplitAssignmentManager manager = new SplitAssignmentManager();
        manager.registerSplits("q1", 0, List.of(
                new SplitAssignment(0, "s0", List.of())));
        manager.registerSplits("q1", 1, List.of(
                new SplitAssignment(0, "s1", List.of())));

        manager.cleanup("q1");

        assertFalse(manager.requestSplit("q1", 0, "w1", List.of()).isPresent());
        assertFalse(manager.requestSplit("q1", 1, "w1", List.of()).isPresent());
    }
}
