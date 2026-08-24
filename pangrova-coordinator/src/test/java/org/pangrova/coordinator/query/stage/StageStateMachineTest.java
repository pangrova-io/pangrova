package org.pangrova.coordinator.query.stage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link StageStateMachine}.
 * Validates: Requirements 1.1, 1.6
 */
class StageStateMachineTest {

    @Test
    void initialStateIsPlanned() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        assertThat(sm.getState()).isEqualTo(StageStateMachine.State.PLANNED);
        assertThat(sm.getStageId()).isEqualTo("s0");
        assertThat(sm.getQueryId()).isEqualTo("q1");
    }

    @Test
    void happyPathTransitions() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.RUNNING);
        sm.transitionTo(StageStateMachine.State.FLUSHING);
        sm.transitionTo(StageStateMachine.State.FINISHED);
        assertThat(sm.getState()).isEqualTo(StageStateMachine.State.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void illegalTransitionThrows() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        assertThatThrownBy(() -> sm.transitionTo(StageStateMachine.State.RUNNING))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PLANNED")
                .hasMessageContaining("RUNNING");
    }

    @Test
    void tryTransitionToReturnsFalseOnIllegalTransition() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        assertThat(sm.tryTransitionTo(StageStateMachine.State.FINISHED)).isFalse();
        assertThat(sm.getState()).isEqualTo(StageStateMachine.State.PLANNED);
    }

    @Test
    void listenerNotifiedOnTransition() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        List<String> events = new ArrayList<>();
        sm.addStateChangeListener((stageId, oldState, newState) ->
                events.add(stageId + ":" + oldState + "→" + newState));

        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.RUNNING);

        assertThat(events).containsExactly(
                "s0:PLANNED→SCHEDULING",
                "s0:SCHEDULING→RUNNING");
    }

    @Test
    void listenerNotCalledOnFailedTryTransition() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        List<String> events = new ArrayList<>();
        sm.addStateChangeListener((stageId, oldState, newState) ->
                events.add(newState.name()));

        sm.tryTransitionTo(StageStateMachine.State.FINISHED); // illegal
        assertThat(events).isEmpty();
    }

    @Test
    void cancelFromPlanned() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageStateMachine.State.CANCELLED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void failFromScheduling() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.FAILED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void failFromRunning() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.RUNNING);
        sm.transitionTo(StageStateMachine.State.FAILED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void noTransitionFromTerminalStates() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageStateMachine.State.SCHEDULING);
        sm.transitionTo(StageStateMachine.State.FAILED);

        assertThatThrownBy(() -> sm.transitionTo(StageStateMachine.State.RUNNING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsNullStageId() {
        assertThatThrownBy(() -> new StageStateMachine(null, "q1"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullQueryId() {
        assertThatThrownBy(() -> new StageStateMachine("s0", null))
                .isInstanceOf(NullPointerException.class);
    }
}
