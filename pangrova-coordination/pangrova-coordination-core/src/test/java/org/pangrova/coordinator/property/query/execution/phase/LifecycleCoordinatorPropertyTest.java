package org.pangrova.coordinator.property.query.execution.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.phase.PhaseResult;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionContext;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;
import org.pangrova.coordinator.query.execution.phase.QueryLifecycleCoordinator;
import org.pangrova.client.protocol.ResultEndpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for lifecycle coordinator pairing.
 *
 * <p><b>Validates: Requirement 5.6</b></p>
 */
class LifecycleCoordinatorPropertyTest {

    private static final DispatchResult TERMINAL_RESULT = new DispatchResult(
            "localhost", "task-1",
            new ResultEndpoint("localhost", 8080, "ticket-1", "http"));

    /**
     * Property 13: onQueryStart/onQueryEnd always paired on success.
     */
    @Property(tries = 30)
    void startAndEndPairedOnSuccess(
            @ForAll @IntRange(min = 0, max = 5) int phaseCount) {
        AtomicInteger startCount = new AtomicInteger(0);
        AtomicInteger endCount = new AtomicInteger(0);

        List<QueryExecutionPhase> phases = buildContinuePhases(phaseCount);

        QueryLifecycleCoordinator coordinator = new QueryLifecycleCoordinator(
                phases, startCount::incrementAndGet, endCount::incrementAndGet);

        QueryExecutionContext ctx = createContext();
        coordinator.execute(ctx);

        assertThat(startCount.get()).isEqualTo(1);
        assertThat(endCount.get()).isEqualTo(1);
    }

    /**
     * Property 13 (continued): onQueryStart/onQueryEnd always paired on exception.
     */
    @Property(tries = 30)
    void startAndEndPairedOnException(
            @ForAll @IntRange(min = 1, max = 5) int phaseCount,
            @ForAll @IntRange(min = 0, max = 4) int throwingIndex) {
        int safeThrowIdx = throwingIndex % phaseCount;
        AtomicInteger startCount = new AtomicInteger(0);
        AtomicInteger endCount = new AtomicInteger(0);

        List<QueryExecutionPhase> phases = new ArrayList<>();
        for (int i = 0; i < phaseCount; i++) {
            if (i == safeThrowIdx) {
                phases.add(new ThrowingPhase("throwing-" + i));
            } else {
                phases.add(new ContinuePhase("phase-" + i));
            }
        }

        QueryLifecycleCoordinator coordinator = new QueryLifecycleCoordinator(
                phases, startCount::incrementAndGet, endCount::incrementAndGet);

        QueryExecutionContext ctx = createContext();
        assertThatThrownBy(() -> coordinator.execute(ctx))
                .isInstanceOf(RuntimeException.class);

        assertThat(startCount.get())
                .as("onQueryStart must be called exactly once even on exception")
                .isEqualTo(1);
        assertThat(endCount.get())
                .as("onQueryEnd must be called exactly once even on exception")
                .isEqualTo(1);
    }

    /**
     * Property 13 (continued): onQueryStart/onQueryEnd paired with Terminal result.
     */
    @Property(tries = 20)
    void startAndEndPairedOnTerminal(
            @ForAll @IntRange(min = 1, max = 5) int phaseCount) {
        AtomicInteger startCount = new AtomicInteger(0);
        AtomicInteger endCount = new AtomicInteger(0);

        List<QueryExecutionPhase> phases = new ArrayList<>();
        phases.add(new TerminalPhase("terminal-0"));
        for (int i = 1; i < phaseCount; i++) {
            phases.add(new ContinuePhase("phase-" + i));
        }

        QueryLifecycleCoordinator coordinator = new QueryLifecycleCoordinator(
                phases, startCount::incrementAndGet, endCount::incrementAndGet);

        QueryExecutionContext ctx = createContext();
        DispatchResult result = coordinator.execute(ctx);

        assertThat(result).isNotNull();
        assertThat(startCount.get()).isEqualTo(1);
        assertThat(endCount.get()).isEqualTo(1);
    }

    private static List<QueryExecutionPhase> buildContinuePhases(int count) {
        List<QueryExecutionPhase> phases = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            phases.add(new ContinuePhase("phase-" + i));
        }
        return phases;
    }

    private static QueryExecutionContext createContext() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "q-test", Map.of(), null, "tenant-1");
        return QueryExecutionContext.initial(sm, "SELECT 1", session, null);
    }

    /** Phase that always returns Continue. */
    private record ContinuePhase(String phaseName) implements QueryExecutionPhase {
        @Override
        public PhaseResult execute(QueryExecutionContext context) {
            return new PhaseResult.Continue(context);
        }

        @Override
        public String name() {
            return phaseName;
        }
    }

    /** Phase that always returns Terminal. */
    private record TerminalPhase(String phaseName) implements QueryExecutionPhase {
        @Override
        public PhaseResult execute(QueryExecutionContext context) {
            return new PhaseResult.Terminal(TERMINAL_RESULT);
        }

        @Override
        public String name() {
            return phaseName;
        }
    }

    /** Phase that always throws. */
    private record ThrowingPhase(String phaseName) implements QueryExecutionPhase {
        @Override
        public PhaseResult execute(QueryExecutionContext context) {
            throw new RuntimeException("deliberate test exception from " + phaseName);
        }

        @Override
        public String name() {
            return phaseName;
        }
    }
}
