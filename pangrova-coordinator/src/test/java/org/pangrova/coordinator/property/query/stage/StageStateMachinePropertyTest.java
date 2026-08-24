package org.pangrova.coordinator.property.query.stage;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import org.pangrova.coordinator.query.stage.StageRetryPolicy;
import org.pangrova.coordinator.query.stage.StageStateMachine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link StageStateMachine}.
 *
 * <p>**Validates: Requirements 1.1**
 */
class StageStateMachinePropertyTest {

    private static final Map<StageStateMachine.State, Set<StageStateMachine.State>> VALID_TRANSITIONS = Map.of(
            StageStateMachine.State.PLANNED, Set.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.CANCELLED),
            StageStateMachine.State.SCHEDULING, Set.of(StageStateMachine.State.RUNNING, StageStateMachine.State.FAILED, StageStateMachine.State.CANCELLED),
            StageStateMachine.State.RUNNING, Set.of(StageStateMachine.State.FLUSHING, StageStateMachine.State.FAILED, StageStateMachine.State.CANCELLED),
            StageStateMachine.State.FLUSHING, Set.of(StageStateMachine.State.FINISHED, StageStateMachine.State.FAILED),
            StageStateMachine.State.FINISHED, Set.of(),
            StageStateMachine.State.FAILED, Set.of(),
            StageStateMachine.State.CANCELLED, Set.of()
    );

    @Provide
    Arbitrary<StageStateMachine.State> anyState() {
        return Arbitraries.of(StageStateMachine.State.values());
    }

    @Provide
    Arbitrary<List<StageStateMachine.State>> validTransitionSequence() {
        return Arbitraries.lazyOf(
                () -> Arbitraries.just(List.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.RUNNING, StageStateMachine.State.FLUSHING, StageStateMachine.State.FINISHED)),
                () -> Arbitraries.just(List.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.RUNNING, StageStateMachine.State.FAILED)),
                () -> Arbitraries.just(List.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.FAILED)),
                () -> Arbitraries.just(List.of(StageStateMachine.State.CANCELLED)),
                () -> Arbitraries.just(List.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.RUNNING, StageStateMachine.State.CANCELLED)),
                () -> Arbitraries.just(List.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.CANCELLED)),
                () -> Arbitraries.just(List.of(StageStateMachine.State.SCHEDULING, StageStateMachine.State.RUNNING, StageStateMachine.State.FLUSHING, StageStateMachine.State.FAILED))
        );
    }

    /**
     * Property 1: Only legal transitions succeed.
     * For any random target state from PLANNED, only SCHEDULING and CANCELLED should succeed.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property1_onlyLegalTransitionsSucceed(@ForAll("anyState") StageStateMachine.State target) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        boolean expected = VALID_TRANSITIONS.get(StageStateMachine.State.PLANNED).contains(target);
        boolean actual = sm.tryTransitionTo(target);
        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Property 2: Valid transition sequences always reach a terminal state.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property2_validSequenceReachesTerminal(
            @ForAll("validTransitionSequence") List<StageStateMachine.State> sequence) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        for (StageStateMachine.State target : sequence) {
            sm.transitionTo(target);
        }
        assertThat(sm.isTerminal()).isTrue();
    }

    /**
     * Property 3: Listeners are always called on successful transitions.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property3_listenersCalledOnEveryTransition(
            @ForAll("validTransitionSequence") List<StageStateMachine.State> sequence) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        AtomicInteger callCount = new AtomicInteger(0);
        sm.addStateChangeListener((stageId, oldState, newState) -> callCount.incrementAndGet());

        for (StageStateMachine.State target : sequence) {
            sm.transitionTo(target);
        }
        assertThat(callCount.get()).isEqualTo(sequence.size());
    }

    /**
     * Property 4: Terminal states reject all transitions.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property4_terminalStatesRejectAllTransitions(
            @ForAll("anyState") StageStateMachine.State target) {
        // FINISHED is terminal
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.RUNNING);
        sm.transitionTo(StageStateMachine.State.FLUSHING);
        sm.transitionTo(StageStateMachine.State.FINISHED);

        assertThat(sm.tryTransitionTo(target)).isFalse();
    }

    /**
     * Property 5: Illegal transitions do not change state.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property5_illegalTransitionPreservesState(@ForAll("anyState") StageStateMachine.State target) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        StageStateMachine.State before = sm.getState();
        boolean allowed = VALID_TRANSITIONS.get(before).contains(target);
        if (!allowed) {
            sm.tryTransitionTo(target);
            assertThat(sm.getState()).isEqualTo(before);
        }
    }

    /**
     * Property 6: Stage retry policy correctness.
     * canRetry returns true iff currentAttempt < maxRetries.
     *
     * **Validates: Requirements 1.4**
     */
    @Property(tries = 10)
    void property6_retryPolicyCorrectness(
            @ForAll("maxRetries") int maxRetries,
            @ForAll("attempt") int attempt) {
        StageRetryPolicy policy = new StageRetryPolicy(maxRetries);
        assertThat(policy.canRetry(attempt)).isEqualTo(attempt < maxRetries);
    }

    @Provide
    Arbitrary<Integer> maxRetries() {
        return Arbitraries.integers().between(0, 10);
    }

    @Provide
    Arbitrary<Integer> attempt() {
        return Arbitraries.integers().between(0, 15);
    }
}
