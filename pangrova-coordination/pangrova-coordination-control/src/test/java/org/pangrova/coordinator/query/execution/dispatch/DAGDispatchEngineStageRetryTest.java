package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.query.api.enums.StageState;

import org.pangrova.coordinator.query.execution.dispatch.dag.handler.RetryStageHandler;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.StageLifecycleEvent;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.query.stage.StageStateMachine;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.common.resilience.policy.ResiliencePolicyConfig;
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
    void retryableErrorTriggersStageRetryResetsStateMachine() {
        ResilienceRegistry registry = buildRegistry();
        RetryStageHandler handler = new RetryStageHandler(registry);

        Stage stage = createStage(1, "q-retry-1");
        handler.onScheduling(event(stage, "q-retry-1"));

        transitionToFailed(stage,
                new QueryFailure("EXCHANGE-0001", "ExchangeError", "Exchange source unavailable"));

        handler.onFailed(event(stage, "q-retry-1"));

        assertThat(stage.retryCount()).isEqualTo(1);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.PLANNED);
    }

    @Test
    void nonRetryableErrorDoesNotRetryStaysInFailed() {
        ResilienceRegistry registry = buildRegistry();
        RetryStageHandler handler = new RetryStageHandler(registry);

        Stage stage = createStage(1, "q-noretry");
        handler.onScheduling(event(stage, "q-noretry"));

        transitionToFailed(stage,
                new QueryFailure("PARSER-0001", "ParserError", "Syntax error"));

        handler.onFailed(event(stage, "q-noretry"));

        assertThat(stage.retryCount()).isZero();
    }

    @Test
    void stageStateMachineResetAllowsRetryFromFailed() {
        StageStateMachine sm = new StageStateMachine("s-1", "q-reset");
        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.RUNNING);
        sm.transitionTo(StageState.FAILED);

        assertThat(sm.isTerminal()).isTrue();
        assertThat(sm.reset()).isTrue();
        assertThat(sm.getState()).isEqualTo(StageState.PLANNED);
        assertThat(sm.isTerminal()).isFalse();
    }

    @Test
    void stageStateMachineResetFailsFromNonTerminal() {
        StageStateMachine sm = new StageStateMachine("s-1", "q-reset-fail");
        sm.transitionTo(StageState.SCHEDULING);

        assertThat(sm.reset()).isFalse();
        assertThat(sm.getState()).isEqualTo(StageState.SCHEDULING);
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
                new org.pangrova.execution.authority.StageExecutionContext.Live(
                        new org.pangrova.query.api.session.QuerySession(queryId, Map.of(), null, "tenant-test", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())));
    }

    private static void transitionToFailed(Stage stage, QueryFailure error) {
        stage.addTaskId("task-1");
        stage.stateMachine().tryTransitionTo(StageState.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        stage.onTaskFailed("task-1", error);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FAILED);
    }

    private static org.pangrova.planner.analysis.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
