package org.pangrova.coordinator.query;

import org.pangrova.query.lifecycle.QueryStateMachine;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;
import org.pangrova.query.api.enums.QueryState;

import static org.assertj.core.api.Assertions.assertThat;

class QueryTimeoutOwnerTest {
    @Test
    void exactDeadlineTimesOutThroughJournalAndRetainsPhysicalOwnershipUntilExit() {
        var journal = new InMemoryQueryStateJournal();
        try (var owner = new QueryTerminalControlFixture(journal)) {
            String id = owner.registerRunningQuery();
            var state = owner.manager.getStateMachine(id);
            var operation = owner.enter(id);
            owner.manager.timeoutQueriesCreatedBy(state.getCreatedAt(), Duration.ofSeconds(30));
            assertThat(state.getState()).isEqualTo(QueryState.FAILED);
            assertThat(state.getError()).get().satisfies(error -> {
                assertThat(error.errorCode()).isEqualTo("QUERY_TIMEOUT");
                assertThat(error.errorType()).isEqualTo("TIMEOUT");
                assertThat(error.message()).contains(id).contains("PT30S");
            });
            assertThat(journal.latestQueryEvent(id)).isPresent();
            assertThat(owner.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(owner.manager.cleanupTerminalQueries(Duration.ZERO)).isZero();
            owner.manager.timeoutQueriesCreatedBy(state.getCreatedAt(), Duration.ofSeconds(60));
            assertThat(state.getError().orElseThrow().message()).contains("PT30S");
            operation.close();
            owner.retry();
            assertThat(owner.manager.getActiveQueryCount()).isZero();
            assertThat(owner.manager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        }
    }

    @Test
    void newerQueriesAndAlreadyFinishedQueriesRemainUnchanged() {
        try (var owner = new QueryTerminalControlFixture(new InMemoryQueryStateJournal())) {
            String id = owner.registerRunningQuery();
            var state = owner.manager.getStateMachine(id);
            owner.manager.timeoutQueriesCreatedBy(state.getCreatedAt().minusNanos(1), Duration.ofSeconds(1));
            assertThat(state.getState()).isEqualTo(QueryState.RUNNING);
            owner.manager.onAllTasksFinished(id);
            owner.manager.timeoutQueriesCreatedBy(state.getCreatedAt(), Duration.ofSeconds(1));
            assertThat(state.getState()).isEqualTo(QueryState.FINISHED);
            assertThat(state.getError()).isEmpty();
        }
    }

    @Test
    void repeatedSweepDoesNotApplyOldDeadlineToReplacementLifetime() {
        try (var owner = new QueryTerminalControlFixture(new InMemoryQueryStateJournal())) {
            String id = owner.registerRunningQuery();
            var original = owner.manager.getStateMachine(id);
            var cutoff = original.getCreatedAt();
            owner.manager.timeoutQueriesCreatedBy(cutoff, Duration.ofSeconds(1));
            owner.retry();
            assertThat(owner.manager.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
            var replacement = new QueryStateMachine();
            owner.manager.registerStateMachine(id, replacement);
            owner.manager.timeoutQueriesCreatedBy(cutoff, Duration.ofSeconds(1));
            assertThat(owner.manager.getStateMachine(id)).isSameAs(replacement);
            assertThat(replacement.getState()).isEqualTo(QueryState.QUEUED);
            assertThat(replacement.getError()).isEmpty();
        }
    }

    @Test
    void concurrentCancellationWinnerKeepsItsStateAndError() throws Exception {
        try (var owner = new QueryTerminalControlFixture(new InMemoryQueryStateJournal())) {
            String id = owner.registerRunningQuery();
            var state = owner.manager.getStateMachine(id);
            var reserved = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var timeoutEntered = new java.util.concurrent.CountDownLatch(1);
            var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
            state.setBeforeTerminalTransition(() -> {
                reserved.countDown();
                try {
                    if (!release.await(3, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Terminal reservation was not released");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            });
            try {
                var cancelled = executor.submit(() -> owner.manager.cancelQuery(id));
                assertThat(reserved.await(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var timedOut = executor.submit(() -> {
                    timeoutEntered.countDown();
                    owner.manager.timeoutQueriesCreatedBy(state.getCreatedAt(), Duration.ofSeconds(30));
                });
                assertThat(timeoutEntered.await(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                release.countDown();
                assertThat(cancelled.get(3, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                timedOut.get(3, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(state.getState()).isEqualTo(QueryState.CANCELLED);
                assertThat(state.getError()).isEmpty();
            } finally {
                release.countDown();
                org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(executor, 3000);
            }
        }
    }

}
