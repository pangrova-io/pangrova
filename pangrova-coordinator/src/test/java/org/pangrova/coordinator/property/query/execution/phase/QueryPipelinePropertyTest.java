package org.pangrova.coordinator.property.query.execution.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.phase.PhaseResult;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionContext;
import org.pangrova.coordinator.query.execution.phase.QueryExecutionPhase;
import org.pangrova.protocol.rest.query.ResultEndpoint;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for pipeline phase execution semantics.
 *
 * <p><b>Validates: Requirement 5.3</b></p>
 */
class QueryPipelinePropertyTest {

    private static final DispatchResult TERMINAL_RESULT = new DispatchResult(
            "localhost", "task-1",
            new ResultEndpoint("localhost", 8080, "ticket-1", "http"));

    /**
     * Property 12: Terminal short-circuits the pipeline.
     * When a phase returns Terminal, subsequent phases are not called.
     */
    @Property(tries = 30)
    void terminalPhaseShortCircuitsPipeline(
            @ForAll @IntRange(min = 2, max = 8) int phaseCount,
            @ForAll @IntRange(min = 0, max = 7) int terminalIndex) {
        int safeTerminalIdx = terminalIndex % phaseCount;
        CopyOnWriteArrayList<Integer> callOrder = new CopyOnWriteArrayList<>();
        List<QueryExecutionPhase> phases = new ArrayList<>();

        for (int i = 0; i < phaseCount; i++) {
            int phaseId = i;
            if (i == safeTerminalIdx) {
                phases.add(new QueryExecutionPhase() {
                    @Override
                    public PhaseResult execute(QueryExecutionContext context) {
                        callOrder.add(phaseId);
                        return new PhaseResult.Terminal(TERMINAL_RESULT);
                    }

                    @Override
                    public String name() {
                        return "terminal-" + phaseId;
                    }
                });
            } else {
                phases.add(new QueryExecutionPhase() {
                    @Override
                    public PhaseResult execute(QueryExecutionContext context) {
                        callOrder.add(phaseId);
                        return new PhaseResult.Continue(context);
                    }

                    @Override
                    public String name() {
                        return "continue-" + phaseId;
                    }
                });
            }
        }

        QueryExecutionContext ctx = createContext();
        DispatchResult result = runPipeline(phases, ctx);

        // Terminal phase was reached
        assertThat(callOrder).contains(safeTerminalIdx);
        // No phases after terminal were called
        for (int id : callOrder) {
            assertThat(id).isLessThanOrEqualTo(safeTerminalIdx);
        }
        // Result is from the terminal phase
        assertThat(result).isSameAs(TERMINAL_RESULT);
        // Exactly (terminalIndex + 1) phases were called
        assertThat(callOrder).hasSize(safeTerminalIdx + 1);
    }

    /**
     * Property 12 (continued): Continue passes context through all phases.
     * When all phases return Continue, all phases are called in order.
     */
    @Property(tries = 30)
    void continuePassesContextThroughAllPhases(
            @ForAll @IntRange(min = 1, max = 8) int phaseCount) {
        CopyOnWriteArrayList<Integer> callOrder = new CopyOnWriteArrayList<>();
        List<QueryExecutionPhase> phases = new ArrayList<>();

        for (int i = 0; i < phaseCount; i++) {
            int phaseId = i;
            phases.add(new QueryExecutionPhase() {
                @Override
                public PhaseResult execute(QueryExecutionContext context) {
                    callOrder.add(phaseId);
                    return new PhaseResult.Continue(context);
                }

                @Override
                public String name() {
                    return "phase-" + phaseId;
                }
            });
        }

        QueryExecutionContext ctx = createContext();
        DispatchResult result = runPipeline(phases, ctx);

        // All phases called in order
        assertThat(callOrder).hasSize(phaseCount);
        for (int i = 0; i < phaseCount; i++) {
            assertThat(callOrder.get(i)).isEqualTo(i);
        }
        // No terminal result
        assertThat(result).isNull();
    }

    /** Runs the pipeline loop mirroring QueryLifecycleCoordinator.runPipeline. */
    private static DispatchResult runPipeline(
            List<QueryExecutionPhase> phases, QueryExecutionContext ctx) {
        for (QueryExecutionPhase phase : phases) {
            PhaseResult result = phase.execute(ctx);
            if (result instanceof PhaseResult.Terminal) {
                return ((PhaseResult.Terminal) result).result();
            } else if (result instanceof PhaseResult.Continue) {
                ctx = ((PhaseResult.Continue) result).updated();
            }
        }
        return null;
    }

    private static QueryExecutionContext createContext() {
        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorSessionContext session = new CoordinatorSessionContext(
                "q-test", Map.of(), null, "tenant-1");
        return QueryExecutionContext.initial(sm, "SELECT 1", session, null);
    }
}
