package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.execution.dispatch.dag.handler.FteAwareTaskFaultHandler.RecoveryAction;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.scheduler.core.admission.StageScheduler;
import org.pangrova.coordinator.scheduler.ft.FaultTolerantStageScheduler;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.scheduler.api.placement.FaultTolerancePolicy;
import org.pangrova.scheduler.api.placement.RetryBudget;
import org.pangrova.shuffle.api.ShuffleCapabilities;
import org.pangrova.shuffle.api.ShuffleDurability;
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
 * Tests for {@link FteAwareTaskFaultHandler} — FTE policy-based task failure
 * recovery with spool re-read and speculative execution consistency.
 */
class FteAwareTaskFaultHandlerTest {

    private static final RelOptCluster CLUSTER;
    private static final ErrorDetail GENERIC_ERROR = new ErrorDetail(
            "EXCHANGE-0001", "ExchangeFailed", "Generic exchange failure");
    private static final ErrorDetail SHUFFLE_FETCH_ERROR = new ErrorDetail(
            "EXCHANGE-0004", "ExchangeFetchFailed",
            "Shuffle data unavailable from upstream stage");

    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    // ---- TASK_RETRY with durable shuffle ----

    @Test
    void taskRetryPolicy_durableShuffle_retriesTask() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildTwoStageDAG("q1");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        RecoveryAction action = handler.handleTaskFailure(
                "q1", "task-1", downstream, dag, GENERIC_ERROR);

        assertThat(action).isEqualTo(RecoveryAction.RETRY_TASK);
    }

    @Test
    void taskRetryPolicy_ephemeralShuffle_failsQuery() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.EPHEMERAL);
        StageDAG dag = buildTwoStageDAG("q2");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        RecoveryAction action = handler.handleTaskFailure(
                "q2", "task-1", downstream, dag, GENERIC_ERROR);

        assertThat(action).isEqualTo(RecoveryAction.FAIL_QUERY);
    }

    // ---- STAGE_RETRY policy ----

    @Test
    void stageRetryPolicy_ephemeralShuffle_retriesStage() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.STAGE_RETRY, ShuffleDurability.EPHEMERAL);
        StageDAG dag = buildTwoStageDAG("q3");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);
        finishStage(upstream, "up-task-0");
        prepareRunning(downstream, "task-1");
        // Force downstream to FAILED for the handler
        downstream.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);

        RecoveryAction action = handler.handleTaskFailure(
                "q3", "task-1", downstream, dag, SHUFFLE_FETCH_ERROR);

        assertThat(action).isEqualTo(RecoveryAction.RETRY_STAGE);
    }

    // ---- NONE policy ----

    @Test
    void nonePolicy_failsQueryImmediately() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.NONE, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildTwoStageDAG("q4");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        RecoveryAction action = handler.handleTaskFailure(
                "q4", "task-1", downstream, dag, GENERIC_ERROR);

        assertThat(action).isEqualTo(RecoveryAction.FAIL_QUERY);
    }

    // ---- Speculative execution consistency (F-COORD-SCHED-07) ----

    @Test
    void speculativeLoser_isSkipped() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildTwoStageDAG("q5");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        // Register task-1 as a speculative task
        handler.registerSpeculativeTask("q5", "task-1");

        RecoveryAction action = handler.handleTaskFailure(
                "q5", "task-1", downstream, dag, GENERIC_ERROR);

        assertThat(action).isEqualTo(RecoveryAction.SKIP);
    }

    @Test
    void nonSpeculativeTask_notSkipped() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildTwoStageDAG("q6");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        // Register a DIFFERENT task as speculative
        handler.registerSpeculativeTask("q6", "task-2");

        RecoveryAction action = handler.handleTaskFailure(
                "q6", "task-1", downstream, dag, GENERIC_ERROR);

        // task-1 is not speculative, should get normal FTE treatment
        assertThat(action).isEqualTo(RecoveryAction.RETRY_TASK);
    }

    // ---- Budget exhaustion ----

    @Test
    void taskRetryBudgetExhausted_failsQuery() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(1)
                .maxRetriesPerStage(10)
                .maxRetriesPerQuery(10)
                .build();
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL, budget);
        StageDAG dag = buildTwoStageDAG("q7");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        // First retry succeeds
        RecoveryAction first = handler.handleTaskFailure(
                "q7", "task-1", downstream, dag, GENERIC_ERROR);
        assertThat(first).isEqualTo(RecoveryAction.RETRY_TASK);

        // Second exceeds per-task budget
        RecoveryAction second = handler.handleTaskFailure(
                "q7", "task-1", downstream, dag, GENERIC_ERROR);
        assertThat(second).isEqualTo(RecoveryAction.FAIL_QUERY);
    }

    // ---- Cleanup ----

    @Test
    void clearQuery_resetsState() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildTwoStageDAG("q8");
        Stage downstream = dag.findStage(1);
        prepareRunning(downstream, "task-1");

        handler.registerSpeculativeTask("q8", "spec-1");
        handler.handleTaskFailure("q8", "task-1", downstream, dag, GENERIC_ERROR);
        handler.clearQuery("q8");

        // After cleanup, spec-1 is no longer tracked as speculative
        RecoveryAction action = handler.handleTaskFailure(
                "q8", "spec-1", downstream, dag, GENERIC_ERROR);
        assertThat(action).isNotEqualTo(RecoveryAction.SKIP);
    }

    // ---- Null safety ----

    @Test
    void nullQueryId_failsQuery() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        StageDAG dag = buildTwoStageDAG("q9");

        RecoveryAction action = handler.handleTaskFailure(
                null, "task-1", dag.findStage(1), dag, GENERIC_ERROR);
        assertThat(action).isEqualTo(RecoveryAction.FAIL_QUERY);
    }

    @Test
    void constructorRejectsNullScheduler() {
        assertThatThrownBy(() -> new FteAwareTaskFaultHandler(null, new TaskFaultHandler()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsNullBaseFaultHandler() {
        assertThatThrownBy(() -> new FteAwareTaskFaultHandler(
                createFteScheduler(FaultTolerancePolicy.NONE, ShuffleDurability.EPHEMERAL),
                null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---- Policy accessor ----

    @Test
    void policy_returnsConfiguredPolicy() {
        FteAwareTaskFaultHandler handler = createHandler(
                FaultTolerancePolicy.TASK_RETRY, ShuffleDurability.LOCAL_SPILL);
        assertThat(handler.policy()).isEqualTo(FaultTolerancePolicy.TASK_RETRY);
    }

    // ---- Helpers ----

    private FteAwareTaskFaultHandler createHandler(
            FaultTolerancePolicy policy, ShuffleDurability durability) {
        return createHandler(policy, durability, RetryBudget.withDefaults());
    }

    private FteAwareTaskFaultHandler createHandler(
            FaultTolerancePolicy policy, ShuffleDurability durability,
            RetryBudget budget) {
        FaultTolerantStageScheduler fteScheduler = createFteScheduler(policy, durability, budget);
        return new FteAwareTaskFaultHandler(fteScheduler, new TaskFaultHandler());
    }

    private FaultTolerantStageScheduler createFteScheduler(
            FaultTolerancePolicy policy, ShuffleDurability durability) {
        return createFteScheduler(policy, durability, RetryBudget.withDefaults());
    }

    private FaultTolerantStageScheduler createFteScheduler(
            FaultTolerancePolicy policy, ShuffleDurability durability,
            RetryBudget budget) {
        return new FaultTolerantStageScheduler(
                new StageScheduler(), policy, budget,
                ShuffleCapabilities.of(durability), null);
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
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        for (String taskId : taskIds) {
            stage.addTaskId(taskId);
        }
        for (String taskId : taskIds) {
            stage.onTaskFinished(taskId);
        }
    }

    private void prepareRunning(Stage stage, String taskId) {
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        stage.addTaskId(taskId);
    }
}
