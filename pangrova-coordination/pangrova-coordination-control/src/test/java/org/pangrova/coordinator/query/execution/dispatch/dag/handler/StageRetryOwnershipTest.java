package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.pangrova.query.api.enums.StageState;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Stage/Resilience4j owner decisions; no Worker execution or invented runtime receipt. */
class StageRetryOwnershipTest {
    @Test
    void ordinaryControlCancellationDoesNotBecomeAnInternalRetry() {
        requireDeclined(new QueryFailure("TASK_CANCELLED", "Cancellation", "Control cancelled"));
    }

    @Test
    void unknownErrorDoesNotBecomeAnInternalRetry() {
        requireDeclined(new QueryFailure("UNKNOWN_FAILURE", "Unknown", "Unclassified input"));
    }

    @Test
    void missingErrorDoesNotBecomeAnInternalRetry() {
        requireDeclined(null);
    }

    @Test
    void missingCodeDoesNotBecomeAnInternalRetry() {
        requireDeclined(new QueryFailure(null, "Unknown", "Missing code"));
    }

    @Test
    void emptyCodeDoesNotBecomeAnInternalRetry() {
        requireDeclined(new QueryFailure("", "Unknown", "Empty code"));
    }

    @Test
    void actualMemoryPressureCategoryDoesNotRequestTheSameBudgetAgain() {
        requireDeclined(new QueryFailure("RUNTIME-0004", "ERROR_CLASS_BUDGET_EXHAUSTED", "Node pressure"));
    }

    @Test
    void retryBudgetsBelongToDifferentQueryStagesEvenWhenTheirNumericIdsMatch() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var first = stage("first-query");
        var second = stage("second-query");
        for (int i = 0; i < 2; i++) {
            fail(first, exchangeError());
            handler.onFailed(event(first));
            assertThat(first.retryCount()).isEqualTo(i + 1);
            assertThat(first.stateMachine().getState()).isEqualTo(StageState.PLANNED);
        }
        fail(second, exchangeError());
        handler.onFailed(event(second));
        assertThat(second.retryCount()).isEqualTo(1);
        assertThat(second.stateMachine().getState()).isEqualTo(StageState.PLANNED);
    }

    @Test
    void aRepeatedFailureNotificationCannotResetTheNewPlannedAttempt() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("one-query");
        fail(owner, exchangeError());
        handler.onFailed(event(owner));
        assertThat(owner.retryCount()).isEqualTo(1);
        handler.onFailed(event(owner));
        assertThat(owner.retryCount()).isEqualTo(1);
        assertThat(owner.currentGeneration()).isEqualTo(1);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.PLANNED);
    }

    @Test
    void classifiedExchangeFailureKeepsItsBoundedRetryAndTerminalLimit() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("bounded-query");
        for (int i = 0; i < 2; i++) {
            fail(owner, exchangeError());
            handler.onFailed(event(owner));
            assertThat(owner.retryCount()).isEqualTo(i + 1);
        }
        fail(owner, exchangeError());
        handler.onFailed(event(owner));
        assertThat(owner.retryCount()).isEqualTo(2);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.FAILED);
    }

    @Test
    void aDifferentExecutionCannotPrepareTheFailedOwner() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("owned-query");
        fail(owner, exchangeError());
        var original = event(owner);
        var wrong = new StageLifecycleEvent(owner, original.dag(), "other-query",
                new StageExecutionContext.Live(
                        new QuerySession("other-query", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())));
        assertThatThrownBy(() -> handler.onFailed(wrong)).isInstanceOf(IllegalArgumentException.class);
        assertThat(owner.retryCount()).isZero();
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.FAILED);
    }

    @Test
    void aDifferentDagOwnerCannotRegisterTheSameNumericStage() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("owned-query");
        var other = stage("owned-query");
        var wrong = new StageLifecycleEvent(owner, event(other).dag(), "owned-query", event(owner).execution());
        assertThatThrownBy(() -> handler.onScheduling(wrong)).isInstanceOf(IllegalArgumentException.class);
        assertThat(owner.retryCount()).isZero();
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.PLANNED);
    }

    private static void requireDeclined(QueryFailure error) {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("declined-query");
        fail(owner, error);
        handler.onFailed(event(owner));
        assertThat(owner.retryCount()).isZero();
        assertThat(owner.currentGeneration()).isZero();
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.FAILED);
        assertThat(owner.lastError()).isSameAs(error);
    }

    private static Stage stage(String queryId) {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        var plan = new PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L);
        return Stage.create(new StageNode(1, plan, 1, "SCAN", List.of()), queryId);
    }

    private static StageLifecycleEvent event(Stage owner) {
        String queryId = owner.stateMachine().getQueryId();
        var dag = StageDAG.create(List.of(owner), List.of(owner.plan()), Map.of(), List.of());
        return new StageLifecycleEvent(owner, dag, queryId,
                new StageExecutionContext.Live(new QuerySession(queryId, Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture())));
    }

    private static void fail(Stage owner, QueryFailure error) {
        owner.addTaskId("task-" + owner.currentGeneration());
        assertThat(owner.stateMachine().tryTransitionTo(StageState.SCHEDULING)).isTrue();
        assertThat(owner.stateMachine().tryTransitionTo(StageState.RUNNING)).isTrue();
        owner.onTaskFailed("task-" + owner.currentGeneration(), error);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.FAILED);
    }

    private static QueryFailure exchangeError() {
        return new QueryFailure("EXCHANGE-0001", "ExchangeError", "Source unavailable");
    }

    /** A dead worker must retry the stage, not fail the whole query. */
    @Test
    void workerLossIsClassifiedAndRetried() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("worker-lost-query");
        fail(owner, new QueryFailure(
                org.pangrova.common.enums.PangrovaErrorCode.WORKER_LOST.getCode(),
                "WORKER_LOST", "Worker declared dead"));
        handler.onFailed(event(owner));
        assertThat(owner.retryCount()).isEqualTo(1);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.PLANNED);
    }

    /**
     * Classification compares against getCode() ("WORKER-0002"), never the enum
     * name. Emitting the bare name silently declined every worker-loss retry,
     * which is exactly the bug this guards.
     */
    @Test
    void bareEnumNameIsNotAClassifiableErrorCode() {
        requireDeclined(new QueryFailure("WORKER_LOST", "WORKER_LOST", "Wrong code namespace"));
    }
}
