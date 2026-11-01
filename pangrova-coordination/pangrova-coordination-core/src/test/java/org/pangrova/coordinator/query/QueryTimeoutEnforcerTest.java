package org.pangrova.coordinator.query;

import org.pangrova.client.error.ErrorDetail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for QueryTimeoutEnforcer.
 * Validates: Requirements 44.1, 44.5
 */
class QueryTimeoutEnforcerTest {

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "test-timeout-enforcer");
                t.setDaemon(true);
                return t;
            });

    private QueryTimeoutEnforcer enforcer;

    @AfterEach
    void tearDown() {
        if (enforcer != null) {
            enforcer.stop();
        }
        scheduler.shutdownNow();
    }

    @Test
    void expiredQueryTransitionsToFailed() throws InterruptedException {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        queries.put("q-1", sm);

        enforcer = new QueryTimeoutEnforcer(
                Duration.ofMillis(50), Duration.ofMillis(20), scheduler);
        enforcer.start(() -> Map.copyOf(queries));

        Thread.sleep(200);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FAILED);
        assertThat(sm.getError()).isPresent();
        ErrorDetail error = sm.getError().get();
        assertThat(error.errorType()).isEqualTo("TIMEOUT");
        assertThat(error.message()).contains("q-1");
    }

    @Test
    void queryCompletedBeforeTimeoutIsNotAffected() throws InterruptedException {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        QueryStateMachine sm = new QueryStateMachine();
        sm.tryTransition(QueryStateMachine.State.PLANNING);
        sm.tryTransition(QueryStateMachine.State.SCHEDULING);
        sm.tryTransition(QueryStateMachine.State.RUNNING);
        sm.tryTransition(QueryStateMachine.State.FINISHING);
        sm.tryTransition(QueryStateMachine.State.FINISHED);
        queries.put("q-2", sm);

        enforcer = new QueryTimeoutEnforcer(
                Duration.ofMillis(50), Duration.ofMillis(20), scheduler);
        enforcer.start(() -> Map.copyOf(queries));

        Thread.sleep(200);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(sm.getError()).isEmpty();
    }

    @Test
    void stopPreventsSubsequentTimeoutChecks() throws InterruptedException {
        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();

        enforcer = new QueryTimeoutEnforcer(
                Duration.ofMillis(50), Duration.ofMillis(20), scheduler);
        enforcer.start(() -> Map.copyOf(queries));
        enforcer.stop();

        // Add a query after stop — it should never be timed out
        QueryStateMachine sm = new QueryStateMachine();
        queries.put("q-3", sm);

        Thread.sleep(200);

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.QUEUED);
    }
}
