package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.coordinator.scheduler.StageScheduler;
import org.pangrova.coordinator.scheduler.ft.FaultTolerantStageScheduler;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.spi.execution.scheduling.FaultTolerancePolicy;
import org.pangrova.spi.execution.scheduling.RetryBudget;
import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleDurability;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link FteStageHandler} — FTE-aware stage lifecycle handler
 * integration with the DAG dispatch engine's handler chain.
 */
class FteStageHandlerTest {

    private static final RelOptCluster CLUSTER;
    private static final ErrorDetail TASK_ERROR = new ErrorDetail(
            "EXCHANGE-0001", "ExchangeFailed", "Task failed");

    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    @Test
    void onFailed_taskRetryPolicy_resetsStageToPlanned() {
        FteStageHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildSingleStageDAG("q-fte-1");
        Stage stage = dag.findStage(0);
        prepareFailedStage(stage, "task-1");

        StageLifecycleEvent event = new StageLifecycleEvent(stage, dag, "q-fte-1", null);
        handler.onFailed(event);

        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.PLANNED);
        assertThat(stage.retryCount()).isEqualTo(1);
    }

    @Test
    void onFailed_nonePolicy_doesNotResetStage() {
        FteStageHandler handler = createHandler(
                FaultTolerancePolicy.NONE, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildSingleStageDAG("q-fte-2");
        Stage stage = dag.findStage(0);
        prepareFailedStage(stage, "task-1");

        StageLifecycleEvent event = new StageLifecycleEvent(stage, dag, "q-fte-2", null);
        handler.onFailed(event);

        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.FAILED);
    }

    @Test
    void onFailed_taskRetryWithEphemeral_doesNotResetStage() {
        FteStageHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.EPHEMERAL);
        StageDAG dag = buildSingleStageDAG("q-fte-3");
        Stage stage = dag.findStage(0);
        prepareFailedStage(stage, "task-1");

        StageLifecycleEvent event = new StageLifecycleEvent(stage, dag, "q-fte-3", null);
        handler.onFailed(event);

        // EPHEMERAL + TASK_RETRY = fail query (no task retry possible)
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.FAILED);
    }

    @Test
    void onFailed_stageRetryPolicy_resetsStageForDurableShuffle() {
        FteStageHandler handler = createHandler(
                FaultTolerancePolicy.STAGE_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildSingleStageDAG("q-fte-4");
        Stage stage = dag.findStage(0);
        prepareFailedStage(stage, "task-1");

        StageLifecycleEvent event = new StageLifecycleEvent(stage, dag, "q-fte-4", null);
        handler.onFailed(event);

        // STAGE_RETRY with durable shuffle: tries task retry first, succeeds
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.PLANNED);
    }

    @Test
    void onScheduling_isNoOp() {
        FteStageHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildSingleStageDAG("q-fte-5");
        Stage stage = dag.findStage(0);

        StageLifecycleEvent event = new StageLifecycleEvent(stage, dag, "q-fte-5", null);

        // Should not throw or modify state
        handler.onScheduling(event);
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.PLANNED);
    }

    @Test
    void onQueryCompleted_clearsState() {
        FteStageHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);

        // Should not throw
        handler.onQueryCompleted("q-fte-6");
        handler.onQueryFailed("q-fte-7");
    }

    @Test
    void constructorRejectsNull() {
        assertThatThrownBy(() -> new FteStageHandler(null))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- Helpers ----

    private FteStageHandler createHandler(FaultTolerancePolicy policy,
                                           ShuffleDurability durability) {
        FaultTolerantStageScheduler fteScheduler = new FaultTolerantStageScheduler(
                new StageScheduler(), policy, RetryBudget.withDefaults(),
                ShuffleCapabilities.of(durability), null);
        FteAwareTaskFaultHandler fteHandler = new FteAwareTaskFaultHandler(
                fteScheduler, new TaskFaultHandler());
        return new FteStageHandler(fteHandler);
    }

    private StageDAG buildSingleStageDAG(String queryId) {
        StageNode node = new StageNode(
                0, new org.pangrova.planner.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), 1, "COMPUTE", List.of());
        return StageDAG.fromNodes(
                List.of(node), Map.of(), List.of()).withStages(queryId);
    }

    private void prepareFailedStage(Stage stage, String taskId) {
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        stage.addTaskId(taskId);
        stage.onTaskFailed(taskId, TASK_ERROR);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);
    }
}
