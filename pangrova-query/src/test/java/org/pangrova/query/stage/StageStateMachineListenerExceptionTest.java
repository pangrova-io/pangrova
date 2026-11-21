package org.pangrova.query.stage;

import org.pangrova.query.api.enums.StageState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BUG-C03: StageStateMachine.notifyListeners() must catch listener exceptions.
 *
 * Verifies:
 * 1. Listener exception is caught and does not propagate to the caller
 * 2. Other listeners still execute after one throws
 * 3. State transition succeeds even when a listener throws
 */
class StageStateMachineListenerExceptionTest {

    @Test
    void listenerExceptionDoesNotPropagate() {
        StageStateMachine sm = new StageStateMachine("s1", "q1");
        sm.addStateChangeListener((stageId, oldState, newState) -> {
            throw new RuntimeException("listener failure");
        });

        // Should NOT throw — exception is caught and logged
        sm.transitionTo(StageState.SCHEDULING);
        assertThat(sm.getState()).isEqualTo(StageState.SCHEDULING);
    }

    @Test
    void otherListenersExecuteAfterOneThrows() {
        StageStateMachine sm = new StageStateMachine("s1", "q1");
        AtomicBoolean firstCalled = new AtomicBoolean(false);
        List<String> thirdEvents = new ArrayList<>();

        // Listener 1: records call then throws
        sm.addStateChangeListener((stageId, oldState, newState) -> {
            firstCalled.set(true);
            throw new RuntimeException("first listener fails");
        });

        // Listener 2: also throws
        sm.addStateChangeListener((stageId, oldState, newState) -> {
            throw new IllegalStateException("second listener fails");
        });

        // Listener 3: should still execute
        sm.addStateChangeListener((stageId, oldState, newState) ->
                thirdEvents.add(stageId + ":" + oldState + "→" + newState));

        sm.transitionTo(StageState.SCHEDULING);

        assertThat(firstCalled).isTrue();
        assertThat(thirdEvents).containsExactly("s1:PLANNED→SCHEDULING");
        assertThat(sm.getState()).isEqualTo(StageState.SCHEDULING);
    }

    @Test
    void tryTransitionToAlsoCatchesListenerException() {
        StageStateMachine sm = new StageStateMachine("s1", "q1");
        sm.addStateChangeListener((stageId, oldState, newState) -> {
            throw new RuntimeException("listener boom");
        });

        boolean result = sm.tryTransitionTo(StageState.SCHEDULING);
        assertThat(result).isTrue();
        assertThat(sm.getState()).isEqualTo(StageState.SCHEDULING);
    }

    @Test
    void stateTransitionSucceedsWithThrowingAndNormalListeners() {
        StageStateMachine sm = new StageStateMachine("s1", "q1");
        List<String> events = new ArrayList<>();

        sm.addStateChangeListener((stageId, oldState, newState) ->
                events.add("before:" + newState));

        sm.addStateChangeListener((stageId, oldState, newState) -> {
            throw new RuntimeException("middle listener fails");
        });

        sm.addStateChangeListener((stageId, oldState, newState) ->
                events.add("after:" + newState));

        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.RUNNING);

        assertThat(events).containsExactly(
                "before:SCHEDULING", "after:SCHEDULING",
                "before:RUNNING", "after:RUNNING");
    }
}
