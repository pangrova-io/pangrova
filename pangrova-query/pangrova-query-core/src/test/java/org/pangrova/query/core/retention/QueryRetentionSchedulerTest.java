package org.pangrova.query.core.retention;

import org.pangrova.query.core.lifecycle.QueryStateMachine;

import org.pangrova.query.api.lifecycle.QueryState;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link QueryRetentionScheduler}.
 */
class QueryRetentionSchedulerTest {

    @Test
    void physicalCompletionRetriesDoNotWaitForRetentionSweep() throws Exception {
        var retried = new java.util.concurrent.CountDownLatch(1);
        var scheduler = new QueryRetentionScheduler(new ConcurrentHashMap<>(), ignored -> true, retried::countDown);
        var timer = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        var cleanup = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            scheduler.startPeriodicCleanup(timer, cleanup, Duration.ofHours(1), Duration.ofHours(1));
            assertThat(retried.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } finally {
            scheduler.stopPeriodicCleanup();
            timer.shutdownNow();
            cleanup.shutdownNow();
            assertThat(timer.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(cleanup.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void cleanupRemovesTerminalQueryState() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryRetentionScheduler scheduler = new QueryRetentionScheduler(
                queries, ignored -> false, () -> { });

        QueryStateMachine sm = terminalStateMachine();
        queries.put("q1", sm);

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(1);
        assertThat(queries).doesNotContainKey("q1");
    }

    @Test
    void cleanupPreservesActiveQuery() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryRetentionScheduler scheduler = new QueryRetentionScheduler(
                queries, ignored -> false, () -> { });

        QueryStateMachine active = new QueryStateMachine();
        active.tryTransition(QueryState.PLANNING);
        queries.put("q-active", active);

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(0);
        assertThat(queries).containsKey("q-active");
    }

    @Test
    void cleanupRemovesMultipleTerminalQueries() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryRetentionScheduler scheduler = new QueryRetentionScheduler(
                queries, ignored -> false, () -> { });

        for (int i = 0; i < 3; i++) {
            String qid = "q-batch-" + i;
            queries.put(qid, terminalStateMachine());
        }

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(3);
        assertThat(queries).isEmpty();
    }

    @Test
    void logicalTerminalWithoutPhysicalCleanupIsRetained() {
        var queries = new ConcurrentHashMap<String, QueryStateMachine>();
        var scheduler = new QueryRetentionScheduler(queries, ignored -> false, () -> { });
        // This is a lifecycle-state boundary test, not Native release evidence.
        var state = logicalTerminal();
        queries.put("physical-owner", state);
        assertThat(scheduler.cleanupTerminalQueries(Duration.ZERO)).isZero();
        assertThat(queries).containsKey("physical-owner");
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
