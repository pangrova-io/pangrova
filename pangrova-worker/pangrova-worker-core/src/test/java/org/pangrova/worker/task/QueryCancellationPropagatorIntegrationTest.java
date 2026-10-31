package org.pangrova.worker.task;

import org.pangrova.worker.task.state.TaskState;

import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.protocol.execution.QueryId;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for F-WORKER-EXEC-06: Proves that the cancellation propagator
 * index is populated by task submissions and cleaned up on terminal transitions,
 * enabling query-level cancellation to reach all belonging tasks.
 */
class QueryCancellationPropagatorIntegrationTest {

    private TaskManager taskManager;
    private QueryCancellationPropagator propagator;

    @BeforeEach
    void setUp() {
        taskManager = TaskManager.forHttpWiringOnly();
        propagator = new QueryCancellationPropagator(taskManager);
        taskManager.setCancellationPropagator(propagator);
    }

    @Test
    void submitTask_registersWithPropagator() {
        String taskId = taskManager.submitTask(descriptorForQuery("query-1"));

        assertThat(propagator.trackedQueryCount()).isEqualTo(1);
        assertThat(propagator.taskCountForQuery("query-1")).isEqualTo(1);
    }

    @Test
    void submitMultipleTasks_sameQuery_allRegistered() {
        taskManager.submitTask(descriptorForQuery("query-1"));
        taskManager.submitTask(descriptorForQuery("query-1"));
        taskManager.submitTask(descriptorForQuery("query-1"));

        assertThat(propagator.trackedQueryCount()).isEqualTo(1);
        assertThat(propagator.taskCountForQuery("query-1")).isEqualTo(3);
    }

    @Test
    void submitTasks_differentQueries_trackedSeparately() {
        taskManager.submitTask(descriptorForQuery("query-A"));
        taskManager.submitTask(descriptorForQuery("query-A"));
        taskManager.submitTask(descriptorForQuery("query-B"));

        assertThat(propagator.trackedQueryCount()).isEqualTo(2);
        assertThat(propagator.taskCountForQuery("query-A")).isEqualTo(2);
        assertThat(propagator.taskCountForQuery("query-B")).isEqualTo(1);
    }

    @Test
    void cancelQuery_cancelsAllBelongingTasks() {
        String task1 = taskManager.submitTask(descriptorForQuery("query-1"));
        String task2 = taskManager.submitTask(descriptorForQuery("query-1"));

        // Both tasks are in INITIALIZING — cancellable
        int cancelled = propagator.cancelQuery("query-1");

        assertThat(cancelled).isEqualTo(2);
        assertThat(taskManager.getTaskState(task1))
                .hasValue(TaskState.CANCELLED);
        assertThat(taskManager.getTaskState(task2))
                .hasValue(TaskState.CANCELLED);
    }

    @Test
    void cancelQuery_doesNotAffectOtherQueries() {
        String taskA = taskManager.submitTask(descriptorForQuery("query-A"));
        String taskB = taskManager.submitTask(descriptorForQuery("query-B"));

        propagator.cancelQuery("query-A");

        assertThat(taskManager.getTaskState(taskA))
                .hasValue(TaskState.CANCELLED);
        assertThat(taskManager.getTaskState(taskB))
                .hasValue(TaskState.INITIALIZING);
        assertThat(propagator.taskCountForQuery("query-B")).isEqualTo(1);
    }

    @Test
    void taskTerminalTransition_deregistersFromPropagator() {
        String taskId = taskManager.submitTask(descriptorForQuery("query-1"));

        // Verify registered
        assertThat(propagator.taskCountForQuery("query-1")).isEqualTo(1);

        // Cancel the task directly (transitions to CANCELLED terminal state)
        TaskCancelResult result = taskManager.cancelTask(taskId);
        assertThat(result.isSuccess()).isTrue();

        // The deregistration should have removed the task from propagator index
        assertThat(propagator.taskCountForQuery("query-1")).isEqualTo(0);
        assertThat(propagator.trackedQueryCount()).isEqualTo(0);
    }

    @Test
    void partialTaskCompletion_preservesRemainingTasks() {
        String task1 = taskManager.submitTask(descriptorForQuery("query-1"));
        String task2 = taskManager.submitTask(descriptorForQuery("query-1"));

        // Cancel only one task
        taskManager.cancelTask(task1);

        // The remaining task should still be tracked
        assertThat(propagator.taskCountForQuery("query-1")).isEqualTo(1);
        assertThat(propagator.trackedQueryCount()).isEqualTo(1);

        // Cancel the query — should still find and cancel the remaining task
        int cancelled = propagator.cancelQuery("query-1");
        assertThat(cancelled).isEqualTo(1);
        assertThat(taskManager.getTaskState(task2))
                .hasValue(TaskState.CANCELLED);
    }

    @Test
    void cancelQuery_returnsZero_afterAllTasksAlreadyTerminal() {
        String task1 = taskManager.submitTask(descriptorForQuery("query-1"));
        String task2 = taskManager.submitTask(descriptorForQuery("query-1"));

        // Cancel both tasks individually (terminal transitions deregister them)
        taskManager.cancelTask(task1);
        taskManager.cancelTask(task2);

        // Query-level cancel should find nothing in the index
        int cancelled = propagator.cancelQuery("query-1");
        assertThat(cancelled).isEqualTo(0);
    }

    @Test
    void noPropagator_submitSucceedsWithoutError() {
        // TaskManager without propagator — validates null-safety
        TaskManager bare = TaskManager.forHttpWiringOnly();
        String taskId = bare.submitTask(descriptorForQuery("query-1"));
        assertThat(taskId).isNotNull();
        assertThat(bare.getTaskState(taskId))
                .hasValue(TaskState.INITIALIZING);
    }

    private static TaskDescriptor descriptorForQuery(String queryId) {
        return new TaskDescriptor(
                new QueryId(queryId),
                new TestPlanFragment(),
                List.of(),
                Map.of());
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() {
            return new PlanNodeId("test-node");
        }

        @Override
        public List<PlanNodeId> getChildFragmentIds() {
            return Collections.emptyList();
        }
    }
}
