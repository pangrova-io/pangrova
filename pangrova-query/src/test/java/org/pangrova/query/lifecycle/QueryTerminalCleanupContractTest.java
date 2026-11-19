package org.pangrova.query.lifecycle;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.lifecycle.QueryExecutionCleanup;
import org.slf4j.LoggerFactory;

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
    void incompleteDrainRetainsOwnerAndNotificationsNeverRunCleanupInline() {
        var fixture = new Fixture();
        fixture.execution.incomplete = true;
        var state = terminal(QueryState.FAILED);
        fixture.cleanup.complete("pending", state, "test");
        assertThat(fixture.cleanup.isPending("pending")).isTrue();
        assertThat(state.physicalCleanupApplied()).isFalse();
        assertThat(fixture.events).containsExactly("journal", "observability", "cancel", "cleanup");

        fixture.execution.incomplete = false;
        fixture.cleanup.onExecutionDrain("pending");
        fixture.cleanup.onOperationExit("pending");
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dueRetryCannotBeLostWhileFailedAttemptIsStillExiting(boolean notifyDrain) throws Exception {
        var fixture = new Fixture();
        var state = terminal(QueryState.FAILED);
        var failureLogged = new CountDownLatch(1);
        var finishAttempt = new CountDownLatch(1);
        var logger = (Logger) LoggerFactory.getLogger(QueryTerminalCleanup.class);
        var pauseFailedAttempt = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (!event.getFormattedMessage().startsWith(
                        "event=query_terminal_cleanup_pending queryId=overlap ")) {
                    return;
                }
                failureLogged.countDown();
                try {
                    assertThat(finishAttempt.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }
        };
        var executor = Executors.newSingleThreadExecutor();
        pauseFailedAttempt.start();
        logger.addAppender(pauseFailedAttempt);
        try {
            fixture.execution.incomplete = true;
            var firstAttempt = executor.submit(() -> fixture.cleanup.complete("overlap", state, "test"));
            assertThat(failureLogged.await(10, TimeUnit.SECONDS)).isTrue();
            if (notifyDrain) fixture.cleanup.onExecutionDrain("overlap");
            fixture.clock.set(TimeUnit.SECONDS.toNanos(1));
            fixture.cleanup.retryPending();
            assertThat(state.physicalCleanupApplied()).isFalse();

            finishAttempt.countDown();
            firstAttempt.get(10, TimeUnit.SECONDS);

            fixture.execution.incomplete = false;
            fixture.clock.set(TimeUnit.SECONDS.toNanos(2));
            fixture.cleanup.retryPending();
            assertThat(fixture.events).containsExactly(
                    "journal", "observability", "cancel", "cleanup", "cancel", "cleanup", "release");
            assertThat(state.physicalCleanupApplied()).isTrue();
            assertThat(fixture.cleanup.isPending("overlap")).isFalse();
        } finally {
            finishAttempt.countDown();
            logger.detachAppender(pauseFailedAttempt);
            pauseFailedAttempt.stop();
            executor.shutdownNow();
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
        private Execution(List<String> events) { this.events = events; }
        @Override public void cancelDispatchedTasks(String queryId) { events.add("cancel"); }
        @Override public void cleanupQuery(String queryId) {
            events.add("cleanup");
            if (incomplete) throw new IllegalStateException("Execution still owns an active operation");
        }
        @Override public void cleanupUnadmittedQuery(String queryId) { events.add("local"); }
    }
}
