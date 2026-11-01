package org.pangrova.worker.task;

import org.pangrova.worker.task.state.TaskState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.spi.query.identity.QueryId;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.junit.jupiter.api.Test;

/**
 * Verifies task submission deduplication: submitting the same coordinator-assigned
 * taskId twice returns the same worker taskId without creating a duplicate task.
 *
 * <p>Validates: Requirements 54.1, 54.2 (Task scheduling dedup)</p>
 */
class TaskSubmitDedupTest {

    @Test
    void duplicateSubmitReturnsSameTaskId() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest request = buildRequest("coord-task-1", "q-dedup-1", manager);
        TaskDescriptor descriptor = toDescriptor(request);

        String firstId = manager.submitTask(descriptor, request);
        String secondId = manager.submitTask(descriptor, request);

        assertThat(secondId).isEqualTo(firstId);
    }

    @Test
    void differentCoordinatorTaskIdsCreateSeparateTasks() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest req1 = buildRequest("coord-task-1", "q-dedup-2", manager);
        SubmitTaskRequest req2 = buildRequest("coord-task-2", "q-dedup-2", manager);
        TaskDescriptor desc1 = toDescriptor(req1);
        TaskDescriptor desc2 = toDescriptor(req2);

        String id1 = manager.submitTask(desc1, req1);
        String id2 = manager.submitTask(desc2, req2);

        assertThat(id2).isNotEqualTo(id1);
    }

    @Test
    void nullTaskRequestRejectedByAdmission() {
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TaskRequest must not be null");
    }

    @Test
    void nullCoordinatorTaskIdRejectedByProtobufBuilder() {
        assertThatThrownBy(() -> buildRequest(null, "q-dedup-null", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void blankCoordinatorTaskIdRejectedByContract() {
        var canonical = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                        "valid-task", "q-dedup-4", 0, 0,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        new byte[]{0x01});
        var request = canonical
                .setAttempt(canonical.getAttempt().toBuilder().clearTaskId())
                .build();
        assertThatThrownBy(() -> CanonicalTaskRequests.validateForAdmission(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt and execution-owner identity are incomplete");
    }

    @Test
    void delayedSubmitCannotRecreateCancelledAttempt() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest request = buildRequest("coord-task-cancel", "q-dedup-5", manager);
        TaskDescriptor descriptor = toDescriptor(request);

        String firstId = manager.submitTask(descriptor, request);
        manager.cancelTask(firstId);

        String secondId = manager.submitTask(descriptor, request);
        assertThat(secondId).isEqualTo(firstId);
        assertThat(manager.getTaskState(secondId)).contains(TaskState.CANCELLED);
        assertThat(manager.tasks).hasSize(1);
    }

    @Test
    void taskCountUnchangedOnDuplicateSubmit() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest request = buildRequest("coord-task-count", "q-dedup-6", manager);
        TaskDescriptor descriptor = toDescriptor(request);

        manager.submitTask(descriptor, request);
        int countAfterFirst = manager.getRunningTaskCount();

        manager.submitTask(descriptor, request);
        int countAfterSecond = manager.getRunningTaskCount();

        assertThat(countAfterSecond).isEqualTo(countAfterFirst);
    }

    private static SubmitTaskRequest buildRequest(String taskId, String queryId, TaskManager manager) {
        var builder = org.pangrova.testing.testkit.CanonicalTaskRequestTestFixture.builder(
                                taskId, queryId, 0, 0,
                                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                                new byte[]{0x01})
                        .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                                .setGrantId(taskId + ":grant")
                                .setCpuSlots(1)
                                .setNativeMemoryBytes(64L * 1024 * 1024))
                        .build();
        return SubmitTaskRequest.from(builder.toBuilder().setAttempt(builder.getAttempt().toBuilder()
                .setWorkerExecutionIdentity(manager.workerExecutionIdentity().toProto())).build());
    }

    private static TaskDescriptor toDescriptor(SubmitTaskRequest request) {
        PlanFragment fragment = new PlanFragment() {
            @Override
            public PlanNodeId getId() { return new PlanNodeId("test"); }
            @Override
            public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
        };
        return new TaskDescriptor(
                new QueryId(request.taskRequest().getAttempt().getQueryOrJobId()),
                fragment, List.of(),
                request.taskRequest().getExecutionOptions().getSessionPropertiesMap());
    }
}
