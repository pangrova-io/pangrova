package org.pangrova.coordinator.property.query.stage;

import org.pangrova.query.api.stage.StageState;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.core.stage.StageStateMachine;

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

    private static final Map<StageState, Set<StageState>> VALID_TRANSITIONS = Map.of(
            StageState.PLANNED, Set.of(StageState.SCHEDULING, StageState.CANCELLED),
            StageState.SCHEDULING, Set.of(StageState.RUNNING, StageState.FAILED, StageState.CANCELLED),
            StageState.RUNNING, Set.of(StageState.FLUSHING, StageState.FAILED, StageState.CANCELLED),
            StageState.FLUSHING, Set.of(StageState.FINISHED, StageState.FAILED),
            StageState.FINISHED, Set.of(),
            StageState.FAILED, Set.of(),
            StageState.CANCELLED, Set.of()
    );

    @Provide
    Arbitrary<StageState> anyState() {
        return Arbitraries.of(StageState.values());
    }

    @Provide
    Arbitrary<List<StageState>> validTransitionSequence() {
        return Arbitraries.lazyOf(
                () -> Arbitraries.just(List.of(StageState.SCHEDULING, StageState.RUNNING, StageState.FLUSHING, StageState.FINISHED)),
                () -> Arbitraries.just(List.of(StageState.SCHEDULING, StageState.RUNNING, StageState.FAILED)),
                () -> Arbitraries.just(List.of(StageState.SCHEDULING, StageState.FAILED)),
                () -> Arbitraries.just(List.of(StageState.CANCELLED)),
                () -> Arbitraries.just(List.of(StageState.SCHEDULING, StageState.RUNNING, StageState.CANCELLED)),
                () -> Arbitraries.just(List.of(StageState.SCHEDULING, StageState.CANCELLED)),
                () -> Arbitraries.just(List.of(StageState.SCHEDULING, StageState.RUNNING, StageState.FLUSHING, StageState.FAILED))
        );
    }

    /**
     * Property 1: Only legal transitions succeed.
     * For any random target state from PLANNED, only SCHEDULING and CANCELLED should succeed.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property1_onlyLegalTransitionsSucceed(@ForAll("anyState") StageState target) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        boolean expected = VALID_TRANSITIONS.get(StageState.PLANNED).contains(target);
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
            @ForAll("validTransitionSequence") List<StageState> sequence) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        for (StageState target : sequence) {
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
            @ForAll("validTransitionSequence") List<StageState> sequence) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        AtomicInteger callCount = new AtomicInteger(0);
        sm.addStateChangeListener((stageId, oldState, newState) -> callCount.incrementAndGet());

        for (StageState target : sequence) {
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
            @ForAll("anyState") StageState target) {
        // FINISHED is terminal
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.RUNNING);
        sm.transitionTo(StageState.FLUSHING);
        sm.transitionTo(StageState.FINISHED);

        assertThat(sm.tryTransitionTo(target)).isFalse();
    }

    /**
     * Property 5: Illegal transitions do not change state.
     *
     * **Validates: Requirements 1.1**
     */
    @Property(tries = 10)
    void property5_illegalTransitionPreservesState(@ForAll("anyState") StageState target) {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        StageState before = sm.getState();
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
