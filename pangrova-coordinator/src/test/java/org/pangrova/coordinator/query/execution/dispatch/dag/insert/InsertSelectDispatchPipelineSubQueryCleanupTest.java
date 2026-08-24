package org.pangrova.coordinator.query.execution.dispatch.dag.insert;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates BUG-D38 fix: {@link InsertSelectDispatchPipeline} must clean up sub-query
 * tasks when any step after dispatch fails.
 */
class InsertSelectDispatchPipelineSubQueryCleanupTest {

    private CoordinatorTaskTracker taskTracker;

    @BeforeEach
    void setUp() {
        taskTracker = new CoordinatorTaskTracker();
    }

    // --- cleanupSubQuery unit tests ---

    @Test
    @DisplayName("D38: cleanupSubQuery clears task tracking metadata for all sub-query tasks")
    void cleanupClearsTrackingMetadata() {
        String subQueryId = "isq-cleanup-test";

        taskTracker.registerTask(subQueryId, "task-1");
        taskTracker.registerTask(subQueryId, "task-2");
        taskTracker.recordTaskStart("task-1", "stage-0");
        taskTracker.recordTaskStart("task-2", "stage-0");
        taskTracker.setDeadline("task-1", Instant.now().plusSeconds(60));
        taskTracker.setDeadline("task-2", Instant.now().plusSeconds(60));

        assertFalse(taskTracker.getTimedOutTasks(Instant.now().plusSeconds(120)).isEmpty(),
                "Deadlines should exist before cleanup");

        InsertSelectDispatchPipeline.cleanupSubQuery(taskTracker, subQueryId);

        assertTrue(taskTracker.getTimedOutTasks(Instant.now().plusSeconds(120)).isEmpty(),
                "Deadlines should be cleared after cleanup");
        assertEquals(2, taskTracker.getTaskIds(subQueryId).size(),
                "Task state machines still exist after cleanup");
    }

    @Test
    @DisplayName("D38: cleanupSubQuery handles empty task set gracefully")
    void cleanupHandlesNoTasks() {
        assertDoesNotThrow(() ->
                InsertSelectDispatchPipeline.cleanupSubQuery(taskTracker, "isq-nonexistent"));
    }

    @Test
    @DisplayName("D38: cleanupSubQuery handles single task")
    void cleanupSingleTask() {
        String subQueryId = "isq-single";

        taskTracker.registerTask(subQueryId, "task-only");
        taskTracker.recordTaskStart("task-only", "stage-0");
        taskTracker.setDeadline("task-only", Instant.now().plusSeconds(30));

        InsertSelectDispatchPipeline.cleanupSubQuery(taskTracker, subQueryId);

        assertTrue(taskTracker.getTimedOutTasks(Instant.now().plusSeconds(120)).isEmpty(),
                "Deadline should be cleared after cleanup");
    }

}
