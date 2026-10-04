package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.http.endpoint.TaskResource;
import org.pangrova.worker.task.memory.MemoryRefs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exact pre-registration budget admission and actual Native result-owner release. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerMemoryAllocationTest {
    private static final long MB = 1024L * 1024;

    @Test
    void taskProceedsWhenAllocationSufficient() {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = admission(runtime, 4 * 1024 * MB, 4 * 1024 * MB)) {
            var info = tasks.submit(request(100 * MB, 0));
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.executorHandle()).isZero();
            assertThat(tasks.terminalReport(info).error()).isNull();
        }
    }

    @Test
    void nativeCapLargerThanFreeMemoryIsAcceptedAtAdmission() {
        // This small budget verifies control-plane admission only. Actual Native allocations
        // use the fixture's real process coordinator, not this separate admission arbitrator.
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = admission(runtime, MB, MB)) {
            var info = tasks.submit(request(10 * MB, 0));
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.executorHandle()).isZero();
        }
    }

    @Test
    void taskRejectedWhenBufferCannotBeReserved() {
        verifyRejectedBeforeRegistration(MB, 4 * 1024 * MB, 10 * MB, 2 * MB);
    }

    @Test
    void canonicalTaskRequiresCompleteBufferEvenWhenRatioIsZero() {
        verifyRejectedBeforeRegistration(MB, 4 * 1024 * MB, 10 * MB, 2 * MB);
    }

    @Test
    void taskRejectedWhenNodePoolBudgetExceeded() {
        verifyRejectedBeforeRegistration(4 * 1024 * MB, 50 * MB, 100 * MB, 60 * MB);
    }

    @Test
    void httpAdmissionRejectsInsufficientBufferBeforeRegisteringTask() {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = admission(runtime, 4 * 1024 * MB, 50 * MB);
             var resource = new TaskResource(tasks.manager(), runtime.transports().exchangeTransport(),
                     "127.0.0.1", runtime.resultPort())) {
            var response = resource.submitTask(SubmitTaskRequest.from(request(100 * MB, 60 * MB)));
            assertThat(response.getStatus()).isEqualTo(503);
            assertUnregistered(tasks);
        }
    }

    @Test
    void nodePoolBudgetReleasedAfterTaskCompletion() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = admission(runtime, 4 * 1024 * MB, 100 * MB)) {
            for (int index = 0; index < 2; index++) {
                var info = tasks.submit(request(50 * MB, 50 * MB));
                tasks.awaitState(info, TaskManager.TaskState.FINISHED);
                assertThat(info.allocatedBufferMemory()).isEqualTo(50 * MB);
                assertThat(tasks.consume(info).values()).containsExactly(42L);
                assertThat(info.allocatedBufferMemory()).isZero();
                assertThat(tasks.manager().getTaskState(info.taskId())).hasValue(TaskManager.TaskState.FINISHED);
            }
        }
    }

    private static void verifyRejectedBeforeRegistration(long arbitratorBytes, long nodeBytes,
                                                          long nativeBytes, long bufferBytes) {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = admission(runtime, arbitratorBytes, nodeBytes)) {
            var tracker = new org.pangrova.resource.tenant.TenantResourceTracker();
            tasks.manager().setQuotaEnforcer(new TenantExecutionQuotaEnforcer(tracker));
            var actualRequest = request(nativeBytes, bufferBytes);
            String tenant = actualRequest.getSecurity().getTenantId();
            assertThatThrownBy(() -> tasks.submit(actualRequest))
                    .isInstanceOf(PangrovaException.class)
                    .satisfies(error -> assertThat(((PangrovaException) error).getErrorCode())
                            .isEqualTo(PangrovaErrorCode.RESOURCE_MEMORY_EXCEEDED));
            assertUnregistered(tasks);
            assertThat(tracker.getActiveQueryCount(tenant)).isZero();
        }
    }

    private static void assertUnregistered(ActualTaskManagerFixture tasks) {
        assertThat(tasks.manager().tasks).isEmpty();
        assertThat(tasks.manager().taskAdmissions.size()).isZero();
        assertThat(tasks.reports().reports()).isEmpty();
    }

    private static ActualTaskManagerFixture admission(ActualNativeTaskFixture runtime, long budget, long nodeBytes) {
        var arbitrator = new MemoryArbitrator(budget, 0);
        var node = new RootMemoryPool("actual-admission-root", nodeBytes).createNodePool("actual-admission-node", nodeBytes);
        var resources = new ResourceManager(arbitrator, new ThreadBudgetTracker(8, 4, 2), new SpillManager());
        return new ActualTaskManagerFixture(runtime, "actual-memory-admission",
                new RecordingTaskStatusReporter(), new MemoryRefs(resources, node, null));
    }

    private static TaskRequest request(long nativeBytes, long bufferBytes) {
        var request = ActualNativeTaskFixture.valuesTaskRequest(bufferBytes > 0).toBuilder();
        request.getResourceGrantBuilder().setNativeMemoryBytes(nativeBytes).setShuffleBufferBytes(bufferBytes);
        return request.build();
    }
}
