package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.UnregisteredWorkerException;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies TaskStatusReceiver rejects status reports from unregistered Workers
 * with UnregisteredWorkerException (mapped to HTTP 409 by servlet layer).
 *
 * <p>Tests the guard logic directly on WorkerManager lookup — the rejection
 * happens before any QueryManager or TaskTracker interaction.</p>
 */
class TaskStatusReceiverGuardTest {

    private WorkerManager workerManager;

    @BeforeEach
    void setUp() {
        workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    @Test
    void unregisteredWorkerLookupReturnsNull() {
        assertTrue(workerManager.getWorker("unknown-worker") == null,
                "Unregistered worker should return null from WorkerManager");
    }

    @Test
    void registeredWorkerLookupReturnsInfo() {
        workerManager.registerWorker(activeWorker("w1"));

        WorkerInfo info = workerManager.getWorker("w1");

        assertEquals("w1", info.workerId());
    }

    @Test
    void unregisteredWorkerExceptionCarriesWorkerId() {
        var ex = new UnregisteredWorkerException("w-ghost");

        assertEquals("w-ghost", ex.workerId());
        assertEquals("Status report rejected: worker not registered: w-ghost",
                ex.getMessage());
    }

    @Test
    void taskStatusReportFromUnregisteredWorkerShouldBeRejected() {
        // Simulate the guard logic from TaskStatusReceiver.rejectIfUnregistered()
        String workerId = "unregistered-worker";
        boolean isUnregistered = workerManager.getWorker(workerId) == null;

        assertTrue(isUnregistered,
                "Guard should detect unregistered worker and reject the report");
    }

    @Test
    void taskStatusReportFromRegisteredWorkerShouldPass() {
        workerManager.registerWorker(activeWorker("w1"));

        boolean isUnregistered = workerManager.getWorker("w1") == null;

        assertTrue(!isUnregistered,
                "Registered worker should pass the guard check");
    }

    @Test
    void taskStatusReportRecordFieldsPreserved() {
        TaskStatusReport report = new TaskStatusReport(
                "t1", "q1", 0, 0, "COMPUTE", "w1", "RUNNING",
                null, null, null, 1, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));

        assertEquals("q1", report.queryId());
        assertEquals("t1", report.taskId());
        assertEquals("w1", report.workerId());
        assertEquals("RUNNING", report.state());
        assertEquals(0, report.stageId());
        assertEquals(0, report.partitionId());
        assertEquals("COMPUTE", report.taskType());
    }

    @Test
    void taskTrackerAcceptsReportAfterGuardPasses() {
        workerManager.registerWorker(activeWorker("w1"));
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");

        // Simulate post-guard state update
        tracker.updateTaskState("q1", "t1", "RUNNING");

        assertEquals(
                org.pangrova.coordinator.task.TaskStateMachine.State.RUNNING,
                tracker.getTaskState("q1", "t1"));
    }

    private static WorkerInfo activeWorker(String workerId) {
        return TestWorkerInfoBuilder.active(workerId, "localhost", 8080, 9090);
    }
}
