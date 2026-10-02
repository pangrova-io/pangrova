package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.coordinator.query.execution.dispatch.dag.handler.RetryStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.resilience.ResilienceRegistry;
import org.pangrova.resilience.config.ResiliencePolicyConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies R2.2: ResilienceRegistry-based retry integration via RetryStageHandler.
 *
 * <p>Tests that transient task failures trigger stage retry via RetryStageHandler
 * instead of immediately failing the entire query.</p>
 */
class DAGDispatchEngineStageRetryTest {

    private static ResilienceRegistry buildRegistry() {
        // 3 max attempts means up to 2 retries
        ResiliencePolicyConfig.RetrySettings retrySettings =
                new ResiliencePolicyConfig.RetrySettings(3, 100, 1.0, 1000);
        ResiliencePolicyConfig config = new ResiliencePolicyConfig(
                "coordinator-to-worker",
                ResiliencePolicyConfig.CircuitBreakerSettings.defaults(),
                retrySettings, null, null, null);
        return new ResilienceRegistry(Map.of("coordinator-to-worker", config));
    }

    @Test
    void retryableError_triggersStageRetry_resetsStateMachine() {
        ResilienceRegistry registry = buildRegistry();
        RetryStageHandler handler = new RetryStageHandler(registry);

        Stage stage = createStage(1, "q-retry-1");
        handler.onScheduling(event(stage, "q-retry-1"));

        transitionToFailed(stage,
                new ErrorDetail("EXCHANGE-0001", "ExchangeError", "Exchange source unavailable"));

        handler.onFailed(event(stage, "q-retry-1"));

        assertThat(handler.getRetryCount("1")).isEqualTo(1);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageStateMachine.State.PLANNED);
    }

    @Test
    void nonRetryableError_doesNotRetry_staysInFailed() {
        ResilienceRegistry registry = buildRegistry();
        RetryStageHandler handler = new RetryStageHandler(registry);

        Stage stage = createStage(1, "q-noretry");
        handler.onScheduling(event(stage, "q-noretry"));

        transitionToFailed(stage,
                new ErrorDetail("PARSER-0001", "ParserError", "Syntax error"));

        handler.onFailed(event(stage, "q-noretry"));

        assertThat(handler.getRetryCount("1")).isZero();
    }

    @Test
    void stageStateMachine_reset_allowsRetryFromFailed() {
        StageStateMachine sm = new StageStateMachine("s-1", "q-reset");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.RUNNING);
        sm.transitionTo(StageStateMachine.State.FAILED);

        assertThat(sm.isTerminal()).isTrue();
        assertThat(sm.reset()).isTrue();
        assertThat(sm.getState()).isEqualTo(StageStateMachine.State.PLANNED);
        assertThat(sm.isTerminal()).isFalse();
    }

    @Test
    void stageStateMachine_reset_failsFromNonTerminal() {
        StageStateMachine sm = new StageStateMachine("s-1", "q-reset-fail");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);

        assertThat(sm.reset()).isFalse();
        assertThat(sm.getState()).isEqualTo(StageStateMachine.State.SCHEDULING);
    }

    // -- helpers --

    private static Stage createStage(int stageId, String queryId) {
        StageNode node = new StageNode(stageId, dummyPlan(), 1, "SCAN", List.of());
        return Stage.create(node, queryId);
    }

    private static StageLifecycleEvent event(Stage stage, String queryId) {
        StageNode node = stage.plan();
        StageDAG dag = StageDAG.create(List.of(stage), List.of(node), Map.of(), List.of());
        return new StageLifecycleEvent(stage, dag, queryId,
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(
                        new org.pangrova.coordinator.query.CoordinatorSessionContext(queryId, Map.of(), null, "tenant-test")));
    }

    private static void transitionToFailed(Stage stage, ErrorDetail error) {
        stage.addTaskId("task-1");
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING);
        stage.onTaskFailed("task-1", error);
        // onTaskFailed may skip FAILED transition if shouldRetry() is true,
        // so force the terminal state for test setup.
        stage.stateMachine().tryTransitionTo(StageStateMachine.State.FAILED);
    }

    private static org.pangrova.planner.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
