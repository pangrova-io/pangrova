package org.pangrova.coordinator.property.query;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.QueryTimeoutEnforcer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for query timeout enforcement and terminal query cleanup.
 *
 * <p><b>Validates: Requirements 44.1, 44.2</b></p>
 */
class QueryTimeoutPropertyTest {

    private static final List<QueryStateMachine.State> NON_TERMINAL_STATES = List.of(
            QueryStateMachine.State.QUEUED,
            QueryStateMachine.State.PLANNING,
            QueryStateMachine.State.SCHEDULING,
            QueryStateMachine.State.RUNNING);

    private static final List<QueryStateMachine.State> TERMINAL_STATES = List.of(
            QueryStateMachine.State.FINISHED,
            QueryStateMachine.State.FAILED,
            QueryStateMachine.State.CANCELLED);

    // -- Arbitraries --

    @Provide
    Arbitrary<QueryStateMachine.State> nonTerminalStates() {
        return Arbitraries.of(NON_TERMINAL_STATES);
    }

    @Provide
    Arbitrary<QueryStateMachine.State> terminalStates() {
        return Arbitraries.of(TERMINAL_STATES);
    }

    @Provide
    Arbitrary<Integer> queryCounts() {
        return Arbitraries.integers().between(1, 20);
    }

    // -- Helpers --

    private static QueryStateMachine createInState(QueryStateMachine.State target) {
        QueryStateMachine sm = new QueryStateMachine();
        transitionTo(sm, target);
        return sm;
    }

    private static void transitionTo(QueryStateMachine sm, QueryStateMachine.State target) {
        for (QueryStateMachine.State step : pathTo(target)) {
            sm.tryTransition(step);
        }
    }

    private static List<QueryStateMachine.State> pathTo(QueryStateMachine.State target) {
        return switch (target) {
            case QUEUED -> List.of();
            case PLANNING -> List.of(QueryStateMachine.State.PLANNING);
            case SCHEDULING -> List.of(QueryStateMachine.State.PLANNING, QueryStateMachine.State.SCHEDULING);
            case RUNNING -> List.of(
                    QueryStateMachine.State.PLANNING, QueryStateMachine.State.SCHEDULING,
                    QueryStateMachine.State.RUNNING);
            case FINISHING -> List.of(
                    QueryStateMachine.State.PLANNING, QueryStateMachine.State.SCHEDULING,
                    QueryStateMachine.State.RUNNING, QueryStateMachine.State.FINISHING);
            case FINISHED -> List.of(
                    QueryStateMachine.State.PLANNING, QueryStateMachine.State.SCHEDULING,
                    QueryStateMachine.State.RUNNING, QueryStateMachine.State.FINISHING,
                    QueryStateMachine.State.FINISHED);
            case FAILED -> List.of(QueryStateMachine.State.FAILED);
            case CANCELLED -> List.of(QueryStateMachine.State.CANCELLED);
        };
    }

    private static QueryManager buildQueryManager() {
        var compiler = new org.pangrova.planner.SqlCompiler();
        var scheduler = new org.pangrova.scheduler.core.placement.TaskScheduler(
                new org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer());
        var orchestrator = new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                                new org.pangrova.observability.sdk.noop.NoOpTracingProvider()),
                        scheduler,
                        new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                        new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null, null);
        return new QueryManager(QueryManagerContext.minimal(orchestrator,
                new org.pangrova.observability.sdk.noop.NoOpTracingProvider()));
    }

    // -- Property 22a: Timeout enforcement completeness --

    /**
     * For any set of N queries (1-20) in non-terminal states, after timeout + check interval,
     * ALL should be in FAILED state with TIMEOUT error.
     *
     * <p><b>Validates: Requirements 44.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 22: Query timeout enforcement")
    void allExpiredNonTerminalQueriesTransitionToFailed(
            @ForAll("queryCounts") int count,
            @ForAll("nonTerminalStates") QueryStateMachine.State state) throws InterruptedException {

        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        List<QueryStateMachine> machines = IntStream.range(0, count)
                .mapToObj(i -> createInState(state))
                .toList();
        for (int i = 0; i < count; i++) {
            queries.put("q-" + i, machines.get(i));
        }

        QueryTimeoutEnforcer enforcer = new QueryTimeoutEnforcer(
                Duration.ofMillis(30), Duration.ofMillis(10));
        try {
            enforcer.start(() -> Map.copyOf(queries));
            Thread.sleep(150);
        } finally {
            enforcer.stop();
        }

        for (int i = 0; i < count; i++) {
            QueryStateMachine sm = machines.get(i);
            assertThat(sm.getState())
                    .as("Query q-%d in initial state %s must be FAILED after timeout", i, state)
                    .isEqualTo(QueryStateMachine.State.FAILED);

            Optional<ErrorDetail> error = sm.getError();
            assertThat(error).as("Query q-%d must have error detail", i).isPresent();
            assertThat(error.get().errorType()).isEqualTo("TIMEOUT");
        }
    }

    // -- Property 22b: Terminal query immunity --

    /**
     * For any set of N queries already in terminal states, timeout enforcement
     * never changes their state.
     *
     * <p><b>Validates: Requirements 44.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 22: Query timeout enforcement")
    void terminalQueriesAreImmuneToTimeout(
            @ForAll("queryCounts") int count,
            @ForAll("terminalStates") QueryStateMachine.State terminalState) throws InterruptedException {

        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        List<QueryStateMachine> machines = IntStream.range(0, count)
                .mapToObj(i -> createInState(terminalState))
                .toList();
        for (int i = 0; i < count; i++) {
            queries.put("q-" + i, machines.get(i));
        }

        QueryTimeoutEnforcer enforcer = new QueryTimeoutEnforcer(
                Duration.ofMillis(30), Duration.ofMillis(10));
        try {
            enforcer.start(() -> Map.copyOf(queries));
            Thread.sleep(150);
        } finally {
            enforcer.stop();
        }

        for (int i = 0; i < count; i++) {
            assertThat(machines.get(i).getState())
                    .as("Terminal query q-%d must remain in %s", i, terminalState)
                    .isEqualTo(terminalState);
        }
    }

    // -- Property 22c: Cleanup retention correctness --

    /**
     * For any mix of terminal and active queries, cleanup with Duration.ZERO
     * removes exactly the terminal ones and preserves all active ones.
     *
     * <p><b>Validates: Requirements 44.2</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 22: Query timeout enforcement")
    void cleanupRemovesExactlyTerminalQueries(
            @ForAll("queryCounts") int activeCount,
            @ForAll("queryCounts") int terminalCount,
            @ForAll("nonTerminalStates") QueryStateMachine.State activeState,
            @ForAll("terminalStates") QueryStateMachine.State terminalState) throws InterruptedException {

        QueryManager qm = buildQueryManager();

        for (int i = 0; i < activeCount; i++) {
            qm.registerStateMachine("active-" + i, createInState(activeState));
        }
        for (int i = 0; i < terminalCount; i++) {
            qm.registerStateMachine("terminal-" + i, createInState(terminalState));
        }

        // Ensure all createdAt timestamps are strictly before Instant.now()
        Thread.sleep(2);

        int removed = qm.cleanupTerminalQueries(Duration.ZERO);

        assertThat(removed)
                .as("Should remove exactly %d terminal queries", terminalCount)
                .isEqualTo(terminalCount);

        for (int i = 0; i < activeCount; i++) {
            assertThat(qm.getQueryState("active-" + i))
                    .as("Active query active-%d must be preserved", i)
                    .isPresent();
        }
        for (int i = 0; i < terminalCount; i++) {
            assertThat(qm.getQueryState("terminal-" + i))
                    .as("Terminal query terminal-%d must be removed", i)
                    .isEmpty();
        }
    }
}
