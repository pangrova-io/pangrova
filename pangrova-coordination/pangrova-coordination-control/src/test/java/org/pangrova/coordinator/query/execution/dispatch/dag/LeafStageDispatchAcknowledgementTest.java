package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.execution.task.StageDispatchRecord;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LeafStageDispatchAcknowledgementTest {
    @Test
    void keepsAcknowledgementOrderAndFirstResult() {
        var dispatched = new CopyOnWriteArrayList<StageDispatchRecord>();
        var results = new ConcurrentHashMap<Integer, DispatchResult>();
        var first = acknowledged("first");
        var second = acknowledged("second");

        dispatcher().populateDispatchRecords(List.of(first, second), stage(), dispatched, results);

        assertThat(dispatched).extracting(record -> record.result().taskId()).containsExactly("first", "second");
        assertThat(dispatched).extracting(StageDispatchRecord::request)
                .containsExactly(first.request(), second.request());
        assertThat(results).containsEntry(2, first.future().join());
    }

    @Test
    void retainsPreviouslyAcknowledgedTasksForCleanupWhenACohortFails() {
        var dispatched = new CopyOnWriteArrayList<StageDispatchRecord>();
        var results = new ConcurrentHashMap<Integer, DispatchResult>();
        var first = acknowledged("first");
        var failure = new IllegalStateException("Worker response failed");
        var failed = new DAGDispatchEngine.PartitionDispatch(
                CompletableFuture.failedFuture(failure), 8080, 9090, "worker", null, null);

        assertThatThrownBy(() -> dispatcher().populateDispatchRecords(
                List.of(first, failed), stage(), dispatched, results))
                .isInstanceOf(CompletionException.class).hasCause(failure);

        assertThat(dispatched).singleElement().satisfies(record ->
                assertThat(record.request()).isSameAs(first.request()));
        assertThat(results).containsEntry(2, first.future().join());
    }

    private static LeafStageDispatcher dispatcher() {
        TaskDispatcher unused = (request, worker) -> {
            throw new AssertionError("Acknowledgement publication should not dispatch a task");
        };
        return new LeafStageDispatcher(unused, null, 15,
                new DispatchCanceller(unused, new CoordinatorTaskTracker()), (query, task, stage) -> {},
                new LeafStageDispatcher.LeafDispatchOptions(null, false, null, false, UnaryOperator.identity()));
    }

    private static Stage stage() {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        var fragment = new PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L);
        return Stage.create(new StageNode(2, fragment, 2, "COMPUTE", List.of()), "query");
    }

    private static DAGDispatchEngine.PartitionDispatch acknowledged(String id) {
        var request = SubmitTaskRequest.from(TaskRequest.newBuilder()
                .setAttempt(TaskAttemptId.newBuilder().setTaskId(id)).build());
        return new DAGDispatchEngine.PartitionDispatch(
                CompletableFuture.completedFuture(new DispatchResult("localhost", id, null)),
                8080, 9090, "worker", request, null);
    }
}
