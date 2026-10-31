package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageRetryPolicy;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.client.core.error.ErrorDetail;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the task→stage→DAG completion bridge.
 *
 * <p>Verifies that when task terminal events are routed to Stage entities,
 * the stage transitions correctly (RUNNING → FLUSHING → FINISHED or → FAILED),
 * and when all stages in a DAG finish, the DAG completion future resolves.
 *
 * <p>Tests the bridge at the Stage + StageDAG level with state change listeners
 * wired the same way as {@code DAGDispatchEngine.registerListeners}, without
 * requiring the full engine (which needs SqlCompiler, CatalogRegistry, etc.).
 */
class TaskStageBridgeIntegrationTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RelOptCluster CLUSTER = RelOptCluster.create(
            new VolcanoPlanner(), new RexBuilder(TYPE_FACTORY));
    private static final String QUERY_ID = "bridge-test-q1";

    private static StageNode outputNode(int id) {
        RelNode plan = LogicalValues.createOneRow(CLUSTER);
        return new StageNode(id, new org.pangrova.planner.PlanFragment(plan, -1L, -1L), 1, "OUTPUT", List.of());
    }

    private static StageNode computeNode(int id) {
        RelNode plan = LogicalValues.createOneRow(CLUSTER);
        return new StageNode(id, new org.pangrova.planner.PlanFragment(plan, -1L, -1L), 1, "COMPUTE", List.of());
    }

    /**
     * Creates a Stage with NO_RETRY policy so that a single task failure
     * immediately transitions the stage to FAILED (no retry masking).
     */
    private static Stage noRetryStage(StageNode node, String queryId) {
        return new Stage(node, new StageStateMachine(
                String.valueOf(node.stageId()), queryId), StageRetryPolicy.NO_RETRY);
    }

    // ── Test 1: Single-stage SELECT → task FINISHED → stage FINISHED → DAG complete ──

    @Test
    void singleStageTaskFinished_stageFinishes_dagCompletes() throws Exception {
        // Build a 1-stage DAG (degenerate case — single OUTPUT stage, no edges)
        StageNode node0 = outputNode(0);
        Stage stage0 = Stage.create(node0, QUERY_ID);
        StageDAG dag = StageDAG.create(List.of(stage0), List.of(node0), Map.of(), List.of());

        // Wire a completion future that resolves when the stage reaches FINISHED
        // (mirrors DAGDispatchEngine.registerListeners + onStageFinished logic)
        CompletableFuture<Boolean> dagComplete = new CompletableFuture<>();
        stage0.stateMachine().addStateChangeListener((id, old, next) -> {
            if (next == StageStateMachine.State.FINISHED
                    && DAGCompletionEvaluator.allStagesFinished(dag)) {
                dagComplete.complete(true);
            }
        });

        // Simulate dispatch: PLANNED → SCHEDULING → RUNNING, register one task
        stage0.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage0.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        stage0.addTaskId("task-0");

        // Simulate worker reporting task completion (the bridge under test)
        stage0.onTaskFinished("task-0");

        // Verify: stage transitioned to FINISHED, DAG completion future resolved
        assertThat(stage0.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(dagComplete.get(2, TimeUnit.SECONDS)).isTrue();
    }

    // ── Test 2: Multi-stage JOIN → all stages FINISHED → DAG complete ──

    @Test
    void multiStageAllTasksFinished_allStagesFinish_dagCompletes() throws Exception {
        // Build a 3-stage DAG: stage0 (scan-left) + stage1 (scan-right) → stage2 (join/output)
        // Edges: stage2 depends on [stage0, stage1]
        StageNode node0 = computeNode(0);
        StageNode node1 = computeNode(1);
        StageNode node2 = outputNode(2);

        Stage stage0 = Stage.create(node0, QUERY_ID);
        Stage stage1 = Stage.create(node1, QUERY_ID);
        Stage stage2 = Stage.create(node2, QUERY_ID);

        Map<Integer, List<Integer>> edges = Map.of(
                0, List.of(),
                1, List.of(),
                2, List.of(0, 1)
        );
        StageDAG dag = StageDAG.create(
                List.of(stage0, stage1, stage2),
                List.of(node0, node1, node2),
                edges, List.of());

        // Wire DAG completion listener
        CompletableFuture<Boolean> dagComplete = new CompletableFuture<>();
        for (Stage stage : dag.stageEntities()) {
            stage.stateMachine().addStateChangeListener((id, old, next) -> {
                if (next == StageStateMachine.State.FINISHED
                        && DAGCompletionEvaluator.allStagesFinished(dag)) {
                    dagComplete.complete(true);
                }
            });
        }

        // Simulate dispatch for all stages
        for (Stage stage : dag.stageEntities()) {
            stage.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
            stage.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
            stage.addTaskId("task-" + stage.stageId());
        }

        // Complete root stages first (stage0, stage1)
        stage0.onTaskFinished("task-0");
        assertThat(stage0.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(dagComplete.isDone()).isFalse();

        stage1.onTaskFinished("task-1");
        assertThat(stage1.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(dagComplete.isDone()).isFalse();

        // Complete the join/output stage last
        stage2.onTaskFinished("task-2");
        assertThat(stage2.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(dagComplete.get(2, TimeUnit.SECONDS)).isTrue();
    }

    // ── Test 3: Task FAILED → stage FAILED → DAG exception (not timeout) ──

    @Test
    void taskFailed_stageTransitionsToFailed_dagCompletesExceptionally() throws Exception {
        // Build a 1-stage DAG with NO_RETRY so failure is immediate
        StageNode node0 = outputNode(0);
        Stage stage0 = noRetryStage(node0, QUERY_ID);
        StageDAG dag = StageDAG.create(List.of(stage0), List.of(node0), Map.of(), List.of());

        // Wire a future that captures the stage failure
        CompletableFuture<Void> dagFailed = new CompletableFuture<>();
        AtomicReference<StageStateMachine.State> capturedState = new AtomicReference<>();
        stage0.stateMachine().addStateChangeListener((id, old, next) -> {
            if (next == StageStateMachine.State.FAILED) {
                capturedState.set(next);
                dagFailed.complete(null);
            }
        });

        // Simulate dispatch
        stage0.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage0.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        stage0.addTaskId("task-0");

        // Simulate worker reporting task failure
        ErrorDetail error = new ErrorDetail("RUNTIME-0001", "RuntimeException", "OOM on worker");
        stage0.onTaskFailed("task-0", error);

        // Verify: stage is FAILED (not stuck in RUNNING waiting for timeout)
        dagFailed.get(2, TimeUnit.SECONDS);
        assertThat(capturedState.get()).isEqualTo(StageStateMachine.State.FAILED);
        assertThat(stage0.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
        assertThat(stage0.lastError()).isNotNull();
        assertThat(stage0.lastError().message()).isEqualTo("OOM on worker");
    }

    // ── Test 4: DAGCompletionEvaluator.allStagesFinished correctness ──

    @Test
    void allStagesFinished_returnsFalseWhenAnyStageNotFinished() {
        StageNode node0 = computeNode(0);
        StageNode node1 = outputNode(1);
        Stage stage0 = Stage.create(node0, QUERY_ID);
        Stage stage1 = Stage.create(node1, QUERY_ID);
        StageDAG dag = StageDAG.create(
                List.of(stage0, stage1), List.of(node0, node1),
                Map.of(1, List.of(0)), List.of());

        assertThat(DAGCompletionEvaluator.allStagesFinished(dag)).isFalse();

        // Finish stage0 only
        stage0.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage0.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        stage0.addTaskId("t0");
        stage0.onTaskFinished("t0");
        assertThat(stage0.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(DAGCompletionEvaluator.allStagesFinished(dag)).isFalse();

        // Finish stage1
        stage1.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage1.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        stage1.addTaskId("t1");
        stage1.onTaskFinished("t1");
        assertThat(DAGCompletionEvaluator.allStagesFinished(dag)).isTrue();
    }

    // ── Test 5: Multi-task stage — partial completion does not trigger FINISHED ──

    @Test
    void multiTaskStage_partialCompletion_stageRemainsRunning() {
        StageNode node0 = outputNode(0);
        Stage stage0 = Stage.create(node0, QUERY_ID);

        stage0.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage0.stateMachine().transitionTo(StageStateMachine.State.RUNNING);
        stage0.addTaskId("task-a");
        stage0.addTaskId("task-b");
        stage0.addTaskId("task-c");

        // Complete 2 of 3 tasks
        stage0.onTaskFinished("task-a");
        stage0.onTaskFinished("task-b");
        assertThat(stage0.stateMachine().getState()).isEqualTo(StageStateMachine.State.RUNNING);

        // Complete the last task
        stage0.onTaskFinished("task-c");
        assertThat(stage0.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
    }
}
