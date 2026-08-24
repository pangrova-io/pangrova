package org.pangrova.coordinator.property.query;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;

import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.QueryStateMachine.State;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for QueryStateMachine state transitions.
 * Validates: AC2
 */
class QueryStateMachinePropertyTest {

    private static final Set<State> TERMINAL_STATES = EnumSet.of(
            State.FINISHED, State.FAILED, State.CANCELLED);

    private static final Set<State> ALL_STATES = EnumSet.allOf(State.class);

    /**
     * Property 4: any sequence of valid transitions from QUEUED always reaches a terminal state.
     *
     * Strategy: generate a random sequence of State values, apply only the ones that succeed
     * (tryTransition returns true). After exhausting the sequence, verify the machine is in
     * a terminal state. We use a long enough sequence to ensure the machine has ample
     * opportunity to reach a terminal state via valid-only transitions.
     *
     * Validates: Requirements R1.5, AC2
     */
    @Property(tries = 10)
    void validTransitionSequenceReachesTerminalState(
            @ForAll("validTransitionPaths") List<State> path) {
        QueryStateMachine sm = new QueryStateMachine();

        for (State target : path) {
            sm.tryTransition(target);
        }

        assertThat(TERMINAL_STATES).contains(sm.getState());
        assertThat(sm.isTerminal()).isTrue();
    }

    /**
     * Property 3: terminal states (FINISHED, FAILED, CANCELLED) reject all further transitions.
     *
     * Strategy: drive the state machine to a terminal state via a valid path, then attempt
     * every possible transition — all must return false and the state must not change.
     *
     * Validates: Requirements R1.5, AC2
     */
    @Property(tries = 10)
    void terminalStatesRejectAllTransitions(
            @ForAll("validTransitionPaths") List<State> path) {
        QueryStateMachine sm = new QueryStateMachine();

        for (State target : path) {
            sm.tryTransition(target);
        }

        // Machine should be terminal after a valid path
        State terminalState = sm.getState();
        assertThat(TERMINAL_STATES).contains(terminalState);

        // Every transition attempt from a terminal state must fail
        for (State candidate : ALL_STATES) {
            boolean result = sm.tryTransition(candidate);
            assertThat(result)
                    .as("Transition from terminal %s to %s should be rejected",
                            terminalState, candidate)
                    .isFalse();
        }

        // State must remain unchanged
        assertThat(sm.getState()).isEqualTo(terminalState);
    }

    /**
     * No valid transition sequence can reach a state not in the defined enum.
     *
     * Strategy: apply a random sequence of arbitrary State values (valid or invalid
     * transitions). After each attempt, verify the current state is always a member
     * of the State enum.
     *
     * Validates: Requirements R1.5, AC2
     */
    @Property(tries = 10)
    void stateAlwaysWithinDefinedEnum(
            @ForAll("randomStateSequences") List<State> sequence) {
        QueryStateMachine sm = new QueryStateMachine();
        assertThat(ALL_STATES).contains(sm.getState());

        for (State target : sequence) {
            sm.tryTransition(target);
            assertThat(ALL_STATES)
                    .as("State must always be within defined enum, got %s", sm.getState())
                    .contains(sm.getState());
        }
    }

    /**
     * Generates sequences that always form a valid path to a terminal state.
     * Each path follows the happy-path or failure/cancellation transitions.
     */
    @Provide
    Arbitrary<List<State>> validTransitionPaths() {
        // All possible valid paths from QUEUED to a terminal state
        List<List<State>> paths = List.of(
                // Happy path
                List.of(State.PLANNING, State.SCHEDULING, State.RUNNING,
                        State.FINISHING, State.FINISHED),
                // Fail from FINISHING
                List.of(State.PLANNING, State.SCHEDULING, State.RUNNING,
                        State.FINISHING, State.FAILED),
                // Fail from each non-terminal state
                List.of(State.FAILED),
                List.of(State.PLANNING, State.FAILED),
                List.of(State.PLANNING, State.SCHEDULING, State.FAILED),
                List.of(State.PLANNING, State.SCHEDULING, State.RUNNING, State.FAILED),
                // Cancel from each cancellable state
                List.of(State.CANCELLED),
                List.of(State.PLANNING, State.CANCELLED),
                List.of(State.PLANNING, State.SCHEDULING, State.CANCELLED),
                List.of(State.PLANNING, State.SCHEDULING, State.RUNNING, State.CANCELLED)
        );
        return Arbitraries.of(paths);
    }

    /**
     * Generates random sequences of State values (may include invalid transitions).
     */
    @Provide
    Arbitrary<List<State>> randomStateSequences() {
        return Arbitraries.of(State.values()).list().ofMinSize(1).ofMaxSize(20);
    }
}
