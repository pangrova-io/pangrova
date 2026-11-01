package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.persistence.memory.coordination.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that in-flight queries are identified and drained
 * when a Worker is detected as DEAD.
 */
class DeadWorkerDrainTest {

    private WorkerManager workerManager;
    private CoordinatorTaskTracker taskTracker;
    private List<String> deadWorkerIds;
    private ScheduledExecutorService executor;
    private WorkerHealthScheduler scheduler;

    @BeforeEach
    void setUp() {
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        taskTracker = new CoordinatorTaskTracker();
        deadWorkerIds = Collections.synchronizedList(new ArrayList<>());
        executor = Executors.newSingleThreadScheduledExecutor();
        scheduler = new WorkerHealthScheduler(
                workerManager, deadWorkerIds::add, executor);
    }

    @AfterEach
    void tearDown() {
        scheduler.stop();
        executor.shutdownNow();
    }

    @Test
    void deadWorkerQueriesIdentifiedViTaskTracker() {
        taskTracker.registerTask("q1", "t1");
        taskTracker.assignWorker("t1", "w-dead");
        taskTracker.markScheduled("q1", "t1");
        taskTracker.updateTaskState("q1", "t1", "RUNNING");

        Set<String> affected = taskTracker.getQueriesForWorker("w-dead");

        assertEquals(Set.of("q1"), affected);
    }

    @Test
    void terminalTasksNotIncludedInDrain() {
        taskTracker.registerTask("q1", "t1");
        taskTracker.assignWorker("t1", "w-dead");
        taskTracker.markScheduled("q1", "t1");
        taskTracker.updateTaskState("q1", "t1", "RUNNING");
        taskTracker.updateTaskState("q1", "t1", "FINISHED");

        Set<String> affected = taskTracker.getQueriesForWorker("w-dead");

        assertTrue(affected.isEmpty(), "Finished tasks should not appear in drain set");
    }

    @Test
    void multipleQueriesOnSameDeadWorker() {
        taskTracker.registerTask("q1", "t1");
        taskTracker.assignWorker("t1", "w-dead");
        taskTracker.markScheduled("q1", "t1");
        taskTracker.updateTaskState("q1", "t1", "RUNNING");

        taskTracker.registerTask("q2", "t2");
        taskTracker.assignWorker("t2", "w-dead");
        taskTracker.markScheduled("q2", "t2");
        taskTracker.updateTaskState("q2", "t2", "RUNNING");

        Set<String> affected = taskTracker.getQueriesForWorker("w-dead");

        assertEquals(Set.of("q1", "q2"), affected);
    }

    @Test
    void healthSchedulerRemovesDeadWorkerAfterCallback() {
        WorkerInfo dead = workerAt("w-dead", Instant.now().minusSeconds(120));
        workerManager.registerWorker(dead);

        scheduler.evaluateAll();

        assertEquals(1, deadWorkerIds.size());
        assertEquals("w-dead", deadWorkerIds.get(0));
        assertNull(workerManager.getWorker("w-dead"),
                "Dead worker should be removed from registry");
    }

    @Test
    void activeWorkerQueriesNotAffected() {
        taskTracker.registerTask("q1", "t1");
        taskTracker.assignWorker("t1", "w-active");
        taskTracker.markScheduled("q1", "t1");
        taskTracker.updateTaskState("q1", "t1", "RUNNING");

        Set<String> affected = taskTracker.getQueriesForWorker("w-dead");

        assertTrue(affected.isEmpty());
    }

    private static WorkerInfo workerAt(String id, Instant lastHeartbeat) {
        return TestWorkerInfoBuilder.withHeartbeat(id, "localhost", 8080, 9090, lastHeartbeat);
    }
}
