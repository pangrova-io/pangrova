package org.pangrova.coordinator.query;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link QueryCleanupScheduler}.
 */
class QueryCleanupSchedulerTest {

    @Test
    void cleanupRemovesTerminalQueryState() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(
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
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(
                queries, ignored -> false, () -> { });

        QueryStateMachine active = new QueryStateMachine();
        active.tryTransition(QueryStateMachine.State.PLANNING);
        queries.put("q-active", active);

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(0);
        assertThat(queries).containsKey("q-active");
    }

    @Test
    void cleanupRemovesMultipleTerminalQueries() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(
                queries, ignored -> false, () -> { });

        for (int i = 0; i < 3; i++) {
            String qid = "q-batch-" + i;
            queries.put(qid, terminalStateMachine());
        }

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(3);
        assertThat(queries).isEmpty();
    }

    private static QueryStateMachine terminalStateMachine() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
        sm.claimTerminalEffects();
        return sm;
    }
}
