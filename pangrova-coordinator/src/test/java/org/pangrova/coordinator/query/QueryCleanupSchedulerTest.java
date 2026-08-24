package org.pangrova.coordinator.query;

import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link QueryCleanupScheduler}.
 * Validates BUG-D46: unified cleanup removes both query state and result buffer entries.
 */
class QueryCleanupSchedulerTest {

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "test-cleanup-scheduler");
                t.setDaemon(true);
                return t;
            });

    private final CoordinatorResultBuffer resultBuffer =
            new CoordinatorResultBuffer(executor, CoordinatorResultBuffer.DEFAULT_RETENTION_MS);

    @AfterEach
    void tearDown() {
        resultBuffer.close();
        executor.shutdownNow();
    }

    @Test
    void cleanupRemovesResultBufferEntryAlongsideQueryState() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(queries, resultBuffer);

        QueryStateMachine sm = terminalStateMachine();
        queries.put("q1", sm);
        resultBuffer.put("q1", new byte[]{1, 2, 3});

        assertThat(resultBuffer.contains("q1")).isTrue();

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(1);
        assertThat(queries).doesNotContainKey("q1");
        assertThat(resultBuffer.contains("q1")).isFalse();
    }

    @Test
    void cleanupHandsMissingResultBufferEntryGracefully() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(queries, resultBuffer);

        QueryStateMachine sm = terminalStateMachine();
        queries.put("q2", sm);
        // No result buffer entry for q2

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(1);
        assertThat(queries).doesNotContainKey("q2");
    }

    @Test
    void cleanupWorksWithNullResultBuffer() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(queries, null);

        QueryStateMachine sm = terminalStateMachine();
        queries.put("q3", sm);

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(1);
        assertThat(queries).doesNotContainKey("q3");
    }

    @Test
    void cleanupPreservesActiveQueryResultBuffer() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(queries, resultBuffer);

        QueryStateMachine active = new QueryStateMachine();
        active.tryTransition(QueryStateMachine.State.PLANNING);
        queries.put("q-active", active);
        resultBuffer.put("q-active", new byte[]{4, 5, 6});

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(0);
        assertThat(queries).containsKey("q-active");
        assertThat(resultBuffer.contains("q-active")).isTrue();
    }

    @Test
    void cleanupRemovesMultipleTerminalQueriesAndTheirBuffers() {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryCleanupScheduler scheduler = new QueryCleanupScheduler(queries, resultBuffer);

        for (int i = 0; i < 3; i++) {
            String qid = "q-batch-" + i;
            queries.put(qid, terminalStateMachine());
            resultBuffer.put(qid, new byte[]{(byte) i});
        }

        int removed = scheduler.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed).isEqualTo(3);
        assertThat(queries).isEmpty();
        for (int i = 0; i < 3; i++) {
            assertThat(resultBuffer.contains("q-batch-" + i)).isFalse();
        }
    }

    private static QueryStateMachine terminalStateMachine() {
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
        return sm;
    }
}
