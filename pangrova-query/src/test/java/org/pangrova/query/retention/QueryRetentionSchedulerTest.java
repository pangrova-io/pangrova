package org.pangrova.query.retention;

import org.pangrova.query.lifecycle.QueryStateMachine;

import org.pangrova.query.api.enums.QueryState;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import org.pangrova.query.lifecycle.QueryRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link QueryRetentionScheduler}.
 */
class QueryRetentionSchedulerTest {

    @Test
    void physicalCompletionRetriesDoNotWaitForRetentionSweep() throws Exception {
        var retried = new java.util.concurrent.CountDownLatch(1);
        var sweeps = new java.util.concurrent.atomic.AtomicInteger();
        var scheduler = new QueryRetentionScheduler(new QueryRegistry(), ignored -> true, retried::countDown,
                sweeps::incrementAndGet);
        var timer = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        var cleanup = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            scheduler.startPeriodicCleanup(timer, cleanup, Duration.ofHours(1), Duration.ofHours(1));
            assertThat(retried.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(sweeps.get()).isZero();
        } finally {
            scheduler.stopPeriodicCleanup();
            timer.shutdownNow();
            cleanup.shutdownNow();
            assertThat(timer.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(cleanup.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void acceptanceRetentionFailureDoesNotObstructCompletedQueryEviction() {
        var queries = new QueryRegistry();
        var retried = new java.util.concurrent.atomic.AtomicInteger();
        var swept = new java.util.concurrent.atomic.AtomicInteger();
        var scheduler = new QueryRetentionScheduler(queries, ignored -> false, retried::incrementAndGet,
                () -> {
                    swept.incrementAndGet();
                    throw new IllegalStateException("retention store unavailable");
                });
        queries.register("terminal", terminalStateMachine(), () -> { });

        assertThat(scheduler.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
        assertThat(retried.get()).isEqualTo(1);
        assertThat(swept.get()).isEqualTo(1);
        assertThat(queries.snapshot()).isEmpty();
    }

    @Test
    void cleanupRemovesTerminalQueryState() {
        QueryRegistry queries = new QueryRegistry();
        QueryRetentionScheduler scheduler = new QueryRetentionScheduler(
                queries, ignored -> false, () -> { }, () -> { });

        QueryStateMachine sm = terminalStateMachine();
        queries.register("q1", sm, () -> { });

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(1);
        assertThat(queries.snapshot()).doesNotContainKey("q1");
    }

    @Test
    void cleanupPreservesActiveQuery() {
        QueryRegistry queries = new QueryRegistry();
        QueryRetentionScheduler scheduler = new QueryRetentionScheduler(
                queries, ignored -> false, () -> { }, () -> { });

        QueryStateMachine active = new QueryStateMachine();
        active.tryTransition(QueryState.PLANNING);
        queries.register("q-active", active, () -> { });

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(0);
        assertThat(queries.snapshot()).containsKey("q-active");
    }

    @Test
    void cleanupRemovesMultipleTerminalQueries() {
        QueryRegistry queries = new QueryRegistry();
        QueryRetentionScheduler scheduler = new QueryRetentionScheduler(
                queries, ignored -> false, () -> { }, () -> { });

        for (int i = 0; i < 3; i++) {
            String qid = "q-batch-" + i;
            queries.register(qid, terminalStateMachine(), () -> { });
        }

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(3);
        assertThat(queries.snapshot()).isEmpty();
    }

    @Test
    void logicalTerminalWithoutPhysicalCleanupIsRetained() {
        var queries = new QueryRegistry();
        var scheduler = new QueryRetentionScheduler(queries, ignored -> false, () -> { }, () -> { });
        // This is a lifecycle-state boundary test, not Native release evidence.
        var state = logicalTerminal();
        queries.register("physical-owner", state, () -> { });
        assertThat(scheduler.cleanupTerminalQueries(Duration.ZERO)).isZero();
        assertThat(queries.snapshot()).containsKey("physical-owner");
        assertThat(state.claimPhysicalCleanup()).isTrue();
        assertThat(scheduler.cleanupTerminalQueries(Duration.ZERO)).isEqualTo(1);
    }

    private static QueryStateMachine terminalStateMachine() {
        var sm = logicalTerminal();
        sm.claimPhysicalCleanup();
        return sm;
    }

    private static QueryStateMachine logicalTerminal() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryState.PLANNING);
        sm.tryTransition(QueryState.SCHEDULING);
        sm.tryTransition(QueryState.RUNNING);
        sm.tryTransition(QueryState.FINISHING);
        sm.tryTransition(QueryState.FINISHED);
        sm.claimTerminalEffects();
        return sm;
    }
}
