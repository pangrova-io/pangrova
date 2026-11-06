package org.pangrova.query.core.stage;

import org.pangrova.query.api.stage.StageState;

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
        assertThat(sm.getState()).isEqualTo(StageState.PLANNED);
        assertThat(sm.getStageId()).isEqualTo("s0");
        assertThat(sm.getQueryId()).isEqualTo("q1");
    }

    @Test
    void happyPathTransitions() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.RUNNING);
        sm.transitionTo(StageState.FLUSHING);
        sm.transitionTo(StageState.FINISHED);
        assertThat(sm.getState()).isEqualTo(StageState.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void illegalTransitionThrows() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        assertThatThrownBy(() -> sm.transitionTo(StageState.RUNNING))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PLANNED")
                .hasMessageContaining("RUNNING");
    }

    @Test
    void tryTransitionToReturnsFalseOnIllegalTransition() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        assertThat(sm.tryTransitionTo(StageState.FINISHED)).isFalse();
        assertThat(sm.getState()).isEqualTo(StageState.PLANNED);
    }

    @Test
    void listenerNotifiedOnTransition() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        List<String> events = new ArrayList<>();
        sm.addStateChangeListener((stageId, oldState, newState) ->
                events.add(stageId + ":" + oldState + "→" + newState));

        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.RUNNING);

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

        sm.tryTransitionTo(StageState.FINISHED); // illegal
        assertThat(events).isEmpty();
    }

    @Test
    void cancelFromPlanned() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageState.CANCELLED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void failFromScheduling() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.FAILED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void failFromRunning() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.RUNNING);
        sm.transitionTo(StageState.FAILED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void noTransitionFromTerminalStates() {
        StageStateMachine sm = new StageStateMachine("s0", "q1");
        sm.transitionTo(StageState.SCHEDULING);
        sm.transitionTo(StageState.FAILED);

        assertThatThrownBy(() -> sm.transitionTo(StageState.RUNNING))
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
    @Test
    void successfulOutputCannotBeReopenedByRetry() {
        var sm = new StageStateMachine("s0", "q1");
        for (var state : new StageState[]{StageState.SCHEDULING, StageState.RUNNING,
                StageState.FLUSHING, StageState.FINISHED}) {
            sm.transitionTo(state);
        }
        assertThat(sm.reset()).isFalse();
        assertThat(sm.getState()).isEqualTo(StageState.FINISHED);
    }

    @Test
    void failedAndCancelledStagesCanResetBeforeRetry() {
        var failed = new StageStateMachine("s0", "q1");
        failed.transitionTo(StageState.SCHEDULING);
        failed.transitionTo(StageState.FAILED);
        assertThat(failed.reset()).isTrue();
        assertThat(failed.getState()).isEqualTo(StageState.PLANNED);
        var cancelled = new StageStateMachine("s1", "q1");
        cancelled.transitionTo(StageState.CANCELLED);
        assertThat(cancelled.reset()).isTrue();
        assertThat(cancelled.getState()).isEqualTo(StageState.PLANNED);
    }
}
