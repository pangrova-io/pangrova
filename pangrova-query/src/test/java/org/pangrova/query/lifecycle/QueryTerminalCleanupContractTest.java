package org.pangrova.query.lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.lifecycle.QueryExecutionCleanup;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the actual lifecycle owner through its execution cleanup port. */
class QueryTerminalCleanupContractTest {
    @Test
    void failedQueryPublishesBeforeCancellationAndReleasesOnlyAfterCleanup() {
        var fixture = new Fixture();
        var state = terminal(QueryState.FAILED);
        fixture.cleanup.complete("failed", state, "test");
        fixture.cleanup.complete("failed", state, "test");
        assertThat(fixture.events).containsExactly("journal", "observability", "cancel", "cleanup", "release");
        assertThat(state.physicalCleanupApplied()).isTrue();
        assertThat(fixture.cleanup.isPending("failed")).isFalse();
    }

    @Test
    void finishedQueryDoesNotCancelCompletedExecution() {
        var fixture = new Fixture();
        fixture.cleanup.complete("finished", terminal(QueryState.FINISHED), "test");
        assertThat(fixture.events).containsExactly("journal", "observability", "cleanup", "release");
    }

    @Test
    void rejectedOwnershipUsesOnlyLocalCreationCleanup() {
        var fixture = new Fixture();
        var state = new QueryStateMachine(true);
        state.resolveOwnership(false);
        assertThat(state.tryTransition(QueryState.FAILED)).isTrue();
        fixture.cleanup.complete("unadmitted", state, "test");
        assertThat(fixture.events).containsExactly("journal", "observability", "local", "release");
    }

    @Test
    void cleanupFailureKeepsItsBackoffWhenAnOperationExits() {
        var fixture = new Fixture();
        fixture.execution.incomplete = true;
        var state = terminal(QueryState.FAILED);
        fixture.cleanup.complete("pending", state, "test");
        assertThat(fixture.cleanup.isPending("pending")).isTrue();
        assertThat(state.physicalCleanupApplied()).isFalse();
        assertThat(fixture.events).containsExactly("journal", "observability", "cancel", "cleanup");

        fixture.execution.incomplete = false;
        fixture.cleanup.onDrainProgress("pending");
        fixture.cleanup.retryPending();
        assertThat(fixture.events).hasSize(4);
        fixture.clock.set(TimeUnit.MILLISECONDS.toNanos(101));
        fixture.cleanup.retryPending();
        fixture.cleanup.retryPending();
        assertThat(fixture.events).containsExactly(
                "journal", "observability", "cancel", "cleanup", "cancel", "cleanup", "release");
        assertThat(state.physicalCleanupApplied()).isTrue();
        assertThat(fixture.cleanup.isPending("pending")).isFalse();
    }

    @Test
    void localDrainWaitDoesNoWorkUntilTheLastOperationExits() {
        var fixture = new Fixture();
        fixture.execution.awaitingExit = true;
        var state = terminal(QueryState.FINISHED);
        fixture.cleanup.complete("live", state, "test");
        fixture.clock.set(TimeUnit.SECONDS.toNanos(31));
        fixture.cleanup.retryPending();
        assertThat(fixture.events).containsExactly("journal", "observability", "cleanup");
        assertThat(state.physicalCleanupApplied()).isFalse();

        fixture.execution.awaitingExit = false;
        fixture.cleanup.onDrainProgress("live");
        fixture.cleanup.onDrainProgress("live");
        assertThat(fixture.events).hasSize(3);
        fixture.cleanup.retryPending();
        fixture.cleanup.retryPending();
        assertThat(fixture.events).containsExactly("journal", "observability", "cleanup", "cleanup", "release");
        assertThat(state.physicalCleanupApplied()).isTrue();
    }

    @Test
    void observesAnExitBetweenTheDrainCheckAndEnteringTheWait() {
        var fixture = new Fixture();
        fixture.execution.awaitingExit = true;
        fixture.execution.afterCheck = () -> {
            fixture.execution.awaitingExit = false;
            fixture.cleanup.onDrainProgress("racing");
        };
        var state = terminal(QueryState.FINISHED);
        fixture.cleanup.complete("racing", state, "test");
        assertThat(state.physicalCleanupApplied()).isFalse();
        fixture.cleanup.retryPending();
        assertThat(state.physicalCleanupApplied()).isTrue();
        assertThat(fixture.events).containsExactly("journal", "observability", "cleanup", "cleanup", "release");
    }

    @Test
    void rejectedImmediateDispatchRemainsDueForCleanupSweep() throws Exception {
        var fixture = new Fixture();
        fixture.execution.awaitingExit = true;
        var state = terminal(QueryState.FINISHED);
        fixture.cleanup.complete("rejected", state, "test");
        var rejected = new CountDownLatch(1);
        var timer = new ScheduledThreadPoolExecutor(1);
        timer.setRemoveOnCancelPolicy(true);
        fixture.cleanup.startRetryDispatch(timer, task -> {
            rejected.countDown();
            throw new RejectedExecutionException("Cleanup worker has reached its capacity");
        });
        try {
            fixture.execution.awaitingExit = false;
            fixture.cleanup.onDrainProgress("rejected");
            assertThat(rejected.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(state.physicalCleanupApplied()).isFalse();
            fixture.cleanup.retryPending();
            assertThat(state.physicalCleanupApplied()).isTrue();
        } finally {
            fixture.cleanup.stopRetryDispatch();
            timer.shutdownNow();
            assertThat(timer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static QueryStateMachine terminal(QueryState target) {
        var state = new QueryStateMachine();
        if (target == QueryState.FINISHED) state.toFinisher().run();
        else assertThat(state.tryTransition(target)).isTrue();
        return state;
    }

    private static final class Fixture {
        private final List<String> events = new ArrayList<>();
        private final AtomicLong clock = new AtomicLong();
        private final Execution execution = new Execution(events);
        private final QueryTerminalCleanup cleanup = new QueryTerminalCleanup(
                execution, ignored -> events.add("journal"), ignored -> events.add("observability"),
                context -> {
                    assertThat(context.state().claimPhysicalCleanup()).isTrue();
                    events.add("release");
                }, clock::get);
    }

    /** Test-only execution peer; actual tracker/Native ownership is tested separately. */
    private static final class Execution implements QueryExecutionCleanup {
        private final List<String> events;
        private boolean incomplete;
        private boolean awaitingExit;
        private Runnable afterCheck = () -> { };
        private Execution(List<String> events) { this.events = events; }
        @Override public void cancelDispatchedTasks(String queryId) { events.add("cancel"); }
        @Override public boolean tryCleanupQuery(String queryId) {
            events.add("cleanup");
            if (incomplete) throw new IllegalStateException("Required cleanup failed");
            boolean ready = !awaitingExit;
            afterCheck.run();
            return ready;
        }
        @Override public boolean tryCleanupUnadmittedQuery(String queryId) {
            events.add("local");
            return !awaitingExit;
        }
    }
}
