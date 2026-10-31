package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.TaskFaultHandler;
import org.pangrova.client.core.error.ErrorDetail;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for shuffle fault recovery: upstream re-computation
 * when downstream encounters EXCHANGE_FETCH_FAILED.
 */
class ShuffleFaultRecoveryIntegrationTest {

    private static final RelOptCluster CLUSTER;
    private static final ErrorDetail SHUFFLE_ERROR = new ErrorDetail(
            "EXCHANGE-0004", "ExchangeFetchFailed",
            "Shuffle data unavailable from upstream stage");

    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    @Test
    void upstreamRecomputedOnShuffleFetchFailure() {
        StageDAG dag = buildTwoStageDAG("q-shuffle-test");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        finishStage(upstream, "up-task-0");
        failStage(downstream, "down-task-0", SHUFFLE_ERROR);

        TaskFaultHandler faultHandler = new TaskFaultHandler();
        assertThat(faultHandler.isShuffleFetchFailure(SHUFFLE_ERROR)).isTrue();

        boolean recovered = faultHandler.tryUpstreamRecomputation(
                "q-shuffle-test", downstream, dag);
        assertThat(recovered).isTrue();
        assertThat(upstream.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.PLANNED);
        assertThat(downstream.stateMachine().getState())
                .isEqualTo(StageStateMachine.State.PLANNED);
        assertThat(upstream.retryCount()).isEqualTo(1);
        assertThat(upstream.taskIds()).isEmpty();
    }

    @Test
    void upstreamRecomputationLimitedToOncePerStage() {
        StageDAG dag = buildTwoStageDAG("q-limit-test");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        finishStage(upstream, "up-task-0");
        failStage(downstream, "down-task-0", SHUFFLE_ERROR);

        TaskFaultHandler faultHandler = new TaskFaultHandler();
        boolean firstRecovery = faultHandler.tryUpstreamRecomputation(
                "q-limit-test", downstream, dag);
        assertThat(firstRecovery).isTrue();

        finishStage(upstream, "up-task-1");
        failStage(downstream, "down-task-1", SHUFFLE_ERROR);

        boolean secondRecovery = faultHandler.tryUpstreamRecomputation(
                "q-limit-test", downstream, dag);
        assertThat(secondRecovery)
                .as("second re-computation should be rejected (max 1 per stage)")
                .isFalse();
    }

    @Test
    void clearQueryRemovesRecomputationTracking() {
        StageDAG dag = buildTwoStageDAG("q-clear-test");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        finishStage(upstream, "up-task-0");
        failStage(downstream, "down-task-0", SHUFFLE_ERROR);

        TaskFaultHandler faultHandler = new TaskFaultHandler();
        faultHandler.tryUpstreamRecomputation("q-clear-test", downstream, dag);
        faultHandler.clearQuery("q-clear-test");

        finishStage(upstream, "up-task-1");
        failStage(downstream, "down-task-1", SHUFFLE_ERROR);

        boolean recovered = faultHandler.tryUpstreamRecomputation(
                "q-clear-test", downstream, dag);
        assertThat(recovered)
                .as("after clearQuery, re-computation should be allowed again")
                .isTrue();
    }

    private StageDAG buildTwoStageDAG(String queryId) {
        StageNode upstreamNode = new StageNode(
                0, new org.pangrova.planner.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), 1, "COMPUTE", List.of());
        StageNode downstreamNode = new StageNode(
                1, new org.pangrova.planner.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(
                List.of(upstreamNode, downstreamNode),
                Map.of(1, List.of(0)), List.of()).withStages(queryId);
    }

    private void finishStage(Stage stage, String... taskIds) {
        StageStateMachine.State current = stage.stateMachine().getState();
        if (current == StageStateMachine.State.PLANNED) {
            stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
            stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        }
        for (String taskId : taskIds) {
            stage.addTaskId(taskId);
        }
        for (String taskId : taskIds) {
            stage.onTaskFinished(taskId);
        }
    }

    private void failStage(Stage stage, String taskId, ErrorDetail error) {
        StageStateMachine.State current = stage.stateMachine().getState();
        if (current == StageStateMachine.State.PLANNED) {
            stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
            stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        }
        stage.addTaskId(taskId);
        stage.onTaskFailed(taskId, error);
        // Stage with DEFAULT retry policy may not auto-transition to FAILED
        // (shouldRetry returns true). Force FAILED for test setup.
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);
    }
}
