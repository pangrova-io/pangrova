package org.pangrova.coordinator.property.query;

import org.pangrova.coordinator.testinfra.QueryManagerTestContexts;

import org.pangrova.query.api.enums.QueryState;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.query.runtime.QueryManager;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;

import java.time.Duration;
import java.util.List;
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

    private static final List<QueryState> NON_TERMINAL_STATES = List.of(
            QueryState.QUEUED,
            QueryState.PLANNING,
            QueryState.SCHEDULING,
            QueryState.RUNNING,
            QueryState.FINISHING);

    private static final List<QueryState> TERMINAL_STATES = List.of(
            QueryState.FINISHED,
            QueryState.FAILED,
            QueryState.CANCELLED);

    // -- Arbitraries --

    @Provide
    Arbitrary<QueryState> nonTerminalStates() {
        return Arbitraries.of(NON_TERMINAL_STATES);
    }

    @Provide
    Arbitrary<QueryState> terminalStates() {
        return Arbitraries.of(TERMINAL_STATES);
    }

    @Provide
    Arbitrary<Integer> queryCounts() {
        return Arbitraries.integers().between(1, 20);
    }

    // -- Helpers --

    private static QueryStateMachine createInState(QueryState target) {
        QueryStateMachine sm = new QueryStateMachine();
        transitionTo(sm, target);
        return sm;
    }

    private static void transitionTo(QueryStateMachine sm, QueryState target) {
        for (QueryState step : pathTo(target)) {
            sm.tryTransition(step);
        }
    }

    private static List<QueryState> pathTo(QueryState target) {
        return switch (target) {
            case QUEUED -> List.of();
            case PLANNING -> List.of(QueryState.PLANNING);
            case SCHEDULING -> List.of(QueryState.PLANNING, QueryState.SCHEDULING);
            case RUNNING -> List.of(
                    QueryState.PLANNING, QueryState.SCHEDULING,
                    QueryState.RUNNING);
            case FINISHING -> List.of(
                    QueryState.PLANNING, QueryState.SCHEDULING,
                    QueryState.RUNNING, QueryState.FINISHING);
            case FINISHED -> List.of(
                    QueryState.PLANNING, QueryState.SCHEDULING,
                    QueryState.RUNNING, QueryState.FINISHING,
                    QueryState.FINISHED);
            case FAILED -> List.of(QueryState.FAILED);
            case CANCELLED -> List.of(QueryState.CANCELLED);
        };
    }

    private static TestQueryOwner buildQueryManager() {
        var transport = new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                new org.pangrova.observability.noop.NoOpTracingProvider());
        var compiler = new org.pangrova.planner.analysis.SqlCompiler();
        var scheduler = new org.pangrova.scheduler.placement.TaskScheduler(
                new org.pangrova.scheduler.strategies.placement.LoadBalancedStrategy(), ignoredTenantForPlacement -> java.util.Optional.empty());
        var orchestrator = new QueryDispatcher(
                QueryDispatcherContext.withDefaults(
                        compiler, null, null, null,
                        transport,
                        scheduler,
                        new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                        new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                        null, null, null, 0L),
                null, null);
        return new TestQueryOwner(new QueryManager(QueryManagerTestContexts.minimal(orchestrator,
                new org.pangrova.observability.noop.NoOpTracingProvider())), transport);
    }

    private record TestQueryOwner(QueryManager manager,
            org.pangrova.coordinator.scheduler.RemoteTaskDispatcher transport) implements AutoCloseable {
        @Override
        public void close() {
            try {
                manager.getActiveQueryIds().forEach(manager::cancelQuery);
                // FINISHING deliberately rejects cancellation; this fixture has no
                // in-flight commit, so expire its remaining actual owners explicitly.
                manager.timeoutQueriesCreatedBy(java.time.Instant.MAX, Duration.ofSeconds(30));
                assertThat(manager.awaitAllQueries(3000)).isTrue();
                manager.stopPeriodicCleanup();
            } finally {
                transport.close();
            }
        }
    }

    // -- Property 22a: Timeout enforcement completeness --

    /**
     * For any set of N queries (1-20) in non-terminal states, when their deadline expires,
     * ALL should be in FAILED state with TIMEOUT error.
     *
     * <p><b>Validates: Requirements 44.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 22: Query timeout enforcement")
    void allExpiredNonTerminalQueriesTransitionToFailed(
            @ForAll("queryCounts") int count,
            @ForAll("nonTerminalStates") QueryState state) {

        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        List<QueryStateMachine> machines = IntStream.range(0, count)
                    .mapToObj(i -> createInState(state))
                    .toList();
        for (int i = 0; i < count; i++) {
            queries.put("q-" + i, machines.get(i));
        }

        try (var owner = buildQueryManager()) {
            queries.forEach(owner.manager()::registerStateMachine);
            owner.manager().timeoutQueriesCreatedBy(java.time.Instant.now(), Duration.ofMillis(30));

            for (int i = 0; i < count; i++) {
                QueryStateMachine sm = machines.get(i);
                assertThat(sm.getState())
                    .as("Query q-%d in initial state %s must be FAILED after timeout", i, state)
                    .isEqualTo(QueryState.FAILED);

                Optional<QueryFailure> error = sm.getError();
                assertThat(error).as("Query q-%d must have error detail", i).isPresent();
                assertThat(error.get().errorType()).isEqualTo("TIMEOUT");
            }
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
            @ForAll("terminalStates") QueryState terminalState) {

        ConcurrentHashMap<String, QueryStateMachine> queries = new ConcurrentHashMap<>();
        List<QueryStateMachine> machines = IntStream.range(0, count)
                    .mapToObj(i -> createInState(terminalState))
                    .toList();
        for (int i = 0; i < count; i++) {
            queries.put("q-" + i, machines.get(i));
        }

        try (var owner = buildQueryManager()) {
            queries.forEach(owner.manager()::registerStateMachine);
            owner.manager().timeoutQueriesCreatedBy(java.time.Instant.now(), Duration.ofMillis(30));

            for (int i = 0; i < count; i++) {
                assertThat(machines.get(i).getState())
                    .as("Terminal query q-%d must remain in %s", i, terminalState)
                    .isEqualTo(terminalState);
            }
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
            @ForAll("nonTerminalStates") QueryState activeState,
            @ForAll("terminalStates") QueryState terminalState) {

        try (var owner = buildQueryManager()) {
            QueryManager qm = owner.manager();

            for (int i = 0; i < activeCount; i++) {
                qm.registerStateMachine("active-" + i, createInState(activeState));
            }
            for (int i = 0; i < terminalCount; i++) {
                qm.registerStateMachine("terminal-" + i, createInState(terminalState));
            }

            // Ensure all createdAt timestamps are strictly before Instant.now()
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(() ->
                    qm.getStateMachines().values().stream().allMatch(state ->
                            state.getCreatedAt().isBefore(java.time.Instant.now())));

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
}
