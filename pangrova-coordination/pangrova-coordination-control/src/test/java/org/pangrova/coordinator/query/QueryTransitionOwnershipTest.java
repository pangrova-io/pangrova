package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual local issuance/transition ownership. No Worker, Native execution or physical receipt is simulated. */
class QueryTransitionOwnershipTest {
    @Test
    void lateIllegalTransitionAfterActualScopeRetirementReturnsFalse() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        tracker.openQueryIssuance("late-transition");
        var state = new QueryStateMachine();
        var selected = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var selectedThread = new AtomicReference<Thread>();
        state.setTransitionOperations(() -> {
            if (Thread.currentThread() == selectedThread.get()) { selected.countDown(); await(resume); }
            return tracker.enterQueryMetadataOperation("late-transition");
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var transition = executor.submit(() -> {
                selectedThread.set(Thread.currentThread());
                return state.tryTransition(QueryState.PLANNING);
            });
            assertThat(selected.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(state.tryTransition(QueryState.FAILED)).isTrue();
            tracker.sealQueryIssuance("late-transition");
            tracker.retireQueryIssuance("late-transition");
            resume.countDown();
            assertThat(transition.get(3, TimeUnit.SECONDS)).isFalse();
            assertThat(state.getState()).isEqualTo(QueryState.FAILED);
        } finally {
            resume.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 3000);
        }
    }

    @Test
    void closedScopeCannotHideAStillLegalTransitionFailure() {
        var tracker = new CoordinatorTaskTracker();
        tracker.openQueryIssuance("closed-transition");
        var state = new QueryStateMachine();
        state.setTransitionOperations(() -> tracker.enterQueryMetadataOperation("closed-transition"));
        tracker.sealQueryIssuance("closed-transition");
        tracker.retireQueryIssuance("closed-transition");
        assertThatThrownBy(() -> state.tryTransition(QueryState.PLANNING))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("metadata admission is closed");
        assertThat(state.getState()).isEqualTo(QueryState.QUEUED);
    }

    @Test
    void actualTransitionListenerExitPrecedesScopeRetirement() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        tracker.openQueryIssuance("entered-transition");
        var state = new QueryStateMachine();
        state.setTransitionOperations(() -> tracker.enterQueryMetadataOperation("entered-transition"));
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        state.addStateChangeListener((previous, current) -> {
            if (current == QueryState.PLANNING) { entered.countDown(); await(resume); }
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var transition = executor.submit(() -> state.tryTransition(QueryState.PLANNING));
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(state.tryTransition(QueryState.FAILED)).isTrue();
            tracker.sealQueryIssuance("entered-transition");
            assertThatThrownBy(() -> tracker.retireQueryIssuance("entered-transition"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("operations have not exited");
            resume.countDown();
            assertThat(transition.get(3, TimeUnit.SECONDS)).isTrue();
            tracker.retireQueryIssuance("entered-transition");
        } finally {
            resume.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 3000);
        }
    }

    private static void await(CountDownLatch barrier) {
        try {
            if (!barrier.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("Actual transition owner did not exit");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
