package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.planner.PlanFragment;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Stage/Resilience4j owner decisions; no Worker execution or invented runtime receipt. */
class StageRetryOwnershipTest {
    @Test
    void ordinaryControlCancellationDoesNotBecomeAnInternalRetry() {
        requireDeclined(new ErrorDetail("TASK_CANCELLED", "Cancellation", "Control cancelled"));
    }

    @Test
    void unknownErrorDoesNotBecomeAnInternalRetry() {
        requireDeclined(new ErrorDetail("UNKNOWN_FAILURE", "Unknown", "Unclassified input"));
    }

    @Test
    void missingErrorDoesNotBecomeAnInternalRetry() {
        requireDeclined(null);
    }

    @Test
    void missingCodeDoesNotBecomeAnInternalRetry() {
        requireDeclined(new ErrorDetail(null, "Unknown", "Missing code"));
    }

    @Test
    void emptyCodeDoesNotBecomeAnInternalRetry() {
        requireDeclined(new ErrorDetail("", "Unknown", "Empty code"));
    }

    @Test
    void actualMemoryPressureCategoryDoesNotRequestTheSameBudgetAgain() {
        requireDeclined(new ErrorDetail("RUNTIME-0004", "ERROR_CLASS_BUDGET_EXHAUSTED", "Node pressure"));
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
            assertThat(first.stateMachine().getState()).isEqualTo(StageStateMachine.State.PLANNED);
        }
        fail(second, exchangeError());
        handler.onFailed(event(second));
        assertThat(second.retryCount()).isEqualTo(1);
        assertThat(second.stateMachine().getState()).isEqualTo(StageStateMachine.State.PLANNED);
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
        assertThat(owner.stateMachine().getState()).isEqualTo(StageStateMachine.State.PLANNED);
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
        assertThat(owner.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
    }

    @Test
    void aDifferentExecutionCannotPrepareTheFailedOwner() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("owned-query");
        fail(owner, exchangeError());
        var original = event(owner);
        var wrong = new StageLifecycleEvent(owner, original.dag(), "other-query",
                new StageExecutionContext.Live(
                        new CoordinatorSessionContext("other-query", Map.of(), null, "tenant")));
        assertThatThrownBy(() -> handler.onFailed(wrong)).isInstanceOf(IllegalArgumentException.class);
        assertThat(owner.retryCount()).isZero();
        assertThat(owner.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
    }

    @Test
    void aDifferentDagOwnerCannotRegisterTheSameNumericStage() {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("owned-query");
        var other = stage("owned-query");
        var wrong = new StageLifecycleEvent(owner, event(other).dag(), "owned-query", event(owner).execution());
        assertThatThrownBy(() -> handler.onScheduling(wrong)).isInstanceOf(IllegalArgumentException.class);
        assertThat(owner.retryCount()).isZero();
        assertThat(owner.stateMachine().getState()).isEqualTo(StageStateMachine.State.PLANNED);
    }

    private static void requireDeclined(ErrorDetail error) {
        var handler = new RetryStageHandler(new ResilienceRegistry(Map.of()));
        var owner = stage("declined-query");
        fail(owner, error);
        handler.onFailed(event(owner));
        assertThat(owner.retryCount()).isZero();
        assertThat(owner.currentGeneration()).isZero();
        assertThat(owner.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
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
                new StageExecutionContext.Live(new CoordinatorSessionContext(queryId, Map.of(), null, "tenant")));
    }

    private static void fail(Stage owner, ErrorDetail error) {
        owner.addTaskId("task-" + owner.currentGeneration());
        assertThat(owner.stateMachine().tryTransitionTo(StageStateMachine.State.SCHEDULING)).isTrue();
        assertThat(owner.stateMachine().tryTransitionTo(StageStateMachine.State.RUNNING)).isTrue();
        owner.onTaskFailed("task-" + owner.currentGeneration(), error);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
    }

    private static ErrorDetail exchangeError() {
        return new ErrorDetail("EXCHANGE-0001", "ExchangeError", "Source unavailable");
    }
}
