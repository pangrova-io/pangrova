package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.junit.jupiter.api.Test;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.LocalShuffleLifecycleManager;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestObservability;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises real lifecycle state without dispatching or manufacturing task success. */
class DAGLifecycleCancellationTest {
    @Property(tries = 30, seed = "20261002")
    void registrationRacingCleanupAlwaysSettlesTheWait(
            @ForAll @IntRange(min = 0, max = 8) int contextReads) throws Exception {
        var threads = Executors.newFixedThreadPool(2);
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            var lifecycle = lifecycle(remote);
            var state = state();
            lifecycle.registerQuery(state.queryId(), state.dag(), null);
            var start = new CountDownLatch(1);
            var registration = threads.submit(() -> {
                start.await();
                for (int i = 0; i < contextReads; i++) {
                    lifecycle.queryContext(state.queryId());
                }
                lifecycle.registerLiveExecution(state);
                return null;
            });
            var cleanup = threads.submit(() -> {
                start.await();
                lifecycle.clearQuery(state.queryId());
                return null;
            });
            start.countDown();
            registration.get(5, TimeUnit.SECONDS);
            cleanup.get(5, TimeUnit.SECONDS);
            assertCancelled(state);
        } finally {
            threads.shutdownNow();
            assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void clearingActiveDispatchSettlesItExceptionallyAndIsIdempotent() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            var lifecycle = lifecycle(remote);
            var state = state();
            lifecycle.registerQuery(state.queryId(), state.dag(), null);
            lifecycle.registerLiveExecution(state);
            assertThat(state.completion().isDone()).isFalse();
            lifecycle.clearQuery(state.queryId());
            lifecycle.clearQuery(state.queryId());
            assertCancelled(state);
        }
    }

    @Test
    void cleanupBeforeRegistrationCannotLeaveAnUnownedDispatchWait() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            var lifecycle = lifecycle(remote);
            var state = state();
            lifecycle.registerQuery(state.queryId(), state.dag(), null);
            lifecycle.clearQuery(state.queryId());
            lifecycle.registerLiveExecution(state);
            assertCancelled(state);
        }
    }

    @Test
    void duplicateRegistrationCannotReplaceTheOriginalCompletionOwner() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            var lifecycle = lifecycle(remote);
            var state = state();
            lifecycle.registerQuery(state.queryId(), state.dag(), null);
            lifecycle.registerLiveExecution(state);
            assertThatThrownBy(() -> lifecycle.registerLiveExecution(state()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("already active");
            assertThat(state.completion().isDone()).isFalse();
            lifecycle.clearQuery(state.queryId());
            assertCancelled(state);
        }
    }

    @Test
    void clearingAlreadyFailedDispatchPreservesItsActualFailure() {
        try (var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            var lifecycle = lifecycle(remote);
            var state = state();
            lifecycle.registerQuery(state.queryId(), state.dag(), null);
            lifecycle.registerLiveExecution(state);
            var failure = new IllegalStateException("actual failure");
            state.completion().completeExceptionally(failure);
            lifecycle.clearQuery(state.queryId());
            assertThatThrownBy(state.completion()::join).hasCause(failure);
        }
    }

    private static DAGExecutionState state() {
        var compiler = new SqlCompiler();
        var query = compiler.compileToPlannedQuery("SELECT 1");
        var dag = StageDAG.fromFragmentedPlan(query.fragment()).withStages("lifecycle-q");
        return DAGExecutionState.create(new StageExecutionContext.Live(
                SourceFreeQueryFixture.session("lifecycle-q")), dag, compiler, null, null, ignored -> { });
    }

    private static DAGLifecycleController lifecycle(RemoteTaskDispatcher dispatcher) {
        return new DAGLifecycleController(dispatcher,
                new DispatchCanceller(dispatcher, new CoordinatorTaskTracker()),
                LocalShuffleLifecycleManager.INSTANCE, null, List.of());
    }

    private static void assertCancelled(DAGExecutionState state) {
        assertThat(state.cancelled().get()).isTrue();
        assertThat(state.completion().isCompletedExceptionally()).isTrue();
        assertThatThrownBy(state.completion()::join).isInstanceOf(CancellationException.class);
    }
}
