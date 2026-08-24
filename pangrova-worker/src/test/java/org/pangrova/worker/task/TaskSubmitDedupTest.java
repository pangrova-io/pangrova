package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.PlanFragment;
import org.pangrova.spi.execution.PlanNodeId;
import org.pangrova.spi.execution.QueryId;
import org.pangrova.spi.execution.TaskDescriptor;
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

        SubmitTaskRequest request = buildRequest("coord-task-1", "q-dedup-1");
        TaskDescriptor descriptor = toDescriptor(request);

        String firstId = manager.submitTask(descriptor, request);
        String secondId = manager.submitTask(descriptor, request);

        assertThat(secondId).isEqualTo(firstId);
    }

    @Test
    void differentCoordinatorTaskIdsCreateSeparateTasks() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest req1 = buildRequest("coord-task-1", "q-dedup-2");
        SubmitTaskRequest req2 = buildRequest("coord-task-2", "q-dedup-2");
        TaskDescriptor desc1 = toDescriptor(req1);
        TaskDescriptor desc2 = toDescriptor(req2);

        String id1 = manager.submitTask(desc1, req1);
        String id2 = manager.submitTask(desc2, req2);

        assertThat(id2).isNotEqualTo(id1);
    }

    @Test
    void nullCoordinatorTaskIdRejectedByContract() {
        assertThatThrownBy(() -> buildRequest(null, "q-dedup-3"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taskId must not be null or blank");
    }

    @Test
    void blankCoordinatorTaskIdRejectedByContract() {
        assertThatThrownBy(() -> buildRequest("", "q-dedup-4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("taskId must not be null or blank");
    }

    @Test
    void dedupIndexClearedAfterCancel() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest request = buildRequest("coord-task-cancel", "q-dedup-5");
        TaskDescriptor descriptor = toDescriptor(request);

        String firstId = manager.submitTask(descriptor, request);
        manager.cancelTask(firstId);

        String secondId = manager.submitTask(descriptor, request);
        assertThat(secondId).isNotEqualTo(firstId);
    }

    @Test
    void taskCountUnchangedOnDuplicateSubmit() {
        TaskManager manager = TaskManager.forHttpWiringOnly();

        SubmitTaskRequest request = buildRequest("coord-task-count", "q-dedup-6");
        TaskDescriptor descriptor = toDescriptor(request);

        manager.submitTask(descriptor, request);
        int countAfterFirst = manager.getRunningTaskCount();

        manager.submitTask(descriptor, request);
        int countAfterSecond = manager.getRunningTaskCount();

        assertThat(countAfterSecond).isEqualTo(countAfterFirst);
    }

    private static SubmitTaskRequest buildRequest(String taskId, String queryId) {
        String plan = Base64.getEncoder().encodeToString(new byte[]{0x01});
        return SubmitTaskRequest.builder()
                .taskId(taskId)
                .queryId(queryId)
                .stageId(0)
                .partitionId(0)
                .taskType("COMPUTE")
                .planFragment(plan)
                .splits(List.of())
                .exchangeSources(List.of())
                .resourceLimit(null)
                .sessionProperties(Map.of())
                .outputConfig(null)
                .writeConfig(null)
                .build();
    }

    private static TaskDescriptor toDescriptor(SubmitTaskRequest request) {
        PlanFragment fragment = new PlanFragment() {
            @Override
            public PlanNodeId getId() { return new PlanNodeId("test"); }
            @Override
            public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
        };
        return new TaskDescriptor(
                new QueryId(request.queryId()), fragment, List.of(),
                request.sessionProperties() != null ? request.sessionProperties() : Map.of());
    }
}
