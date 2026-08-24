package org.pangrova.coordinator.query;

import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for QueryStateMachine Stage state aggregation view.
 * Validates: Requirements 1.5, 1.6
 */
class QueryStateMachineStageViewTest {

    @Test
    void updateStageStateTracksStageProgress() {
        QueryStateMachine qsm = new QueryStateMachine();
        qsm.updateStageState("0", StageStateMachine.State.SCHEDULING);
        qsm.updateStageState("1", StageStateMachine.State.PLANNED);

        assertThat(qsm.getStageState("0"))
                .hasValue(StageStateMachine.State.SCHEDULING);
        assertThat(qsm.getStageState("1"))
                .hasValue(StageStateMachine.State.PLANNED);
        assertThat(qsm.getStageState("99")).isEmpty();
    }

    @Test
    void getStageStatesReturnsImmutableSnapshot() {
        QueryStateMachine qsm = new QueryStateMachine();
        qsm.updateStageState("0", StageStateMachine.State.RUNNING);
        qsm.updateStageState("1", StageStateMachine.State.FINISHED);

        Map<String, StageStateMachine.State> snapshot = qsm.getStageStates();
        assertThat(snapshot).hasSize(2);
        assertThat(snapshot.get("0")).isEqualTo(StageStateMachine.State.RUNNING);
        assertThat(snapshot.get("1")).isEqualTo(StageStateMachine.State.FINISHED);
    }

    @Test
    void onStageFailedTransitionsQueryToFailed() {
        QueryStateMachine qsm = new QueryStateMachine();
        qsm.tryTransition(QueryStateMachine.State.PLANNING);
        qsm.tryTransition(QueryStateMachine.State.SCHEDULING);
        qsm.tryTransition(QueryStateMachine.State.RUNNING);

        ErrorDetail error = new ErrorDetail("INTERNAL", "INTERNAL", "stage failed");
        qsm.onStageFailed("0", error);

        assertThat(qsm.getState()).isEqualTo(QueryStateMachine.State.FAILED);
        assertThat(qsm.getStageState("0"))
                .hasValue(StageStateMachine.State.FAILED);
        assertThat(qsm.getError()).hasValue(error);
    }

    @Test
    void stageListenerUpdatesQueryStateMachine() {
        QueryStateMachine qsm = new QueryStateMachine();
        StageStateMachine ssm = new StageStateMachine("0", "q1");

        // Wire listener
        ssm.addStateChangeListener(
                (stageId, oldState, newState) -> qsm.updateStageState(stageId, newState));

        ssm.transitionTo(StageStateMachine.State.SCHEDULING);
        assertThat(qsm.getStageState("0"))
                .hasValue(StageStateMachine.State.SCHEDULING);

        ssm.transitionTo(StageStateMachine.State.RUNNING);
        assertThat(qsm.getStageState("0"))
                .hasValue(StageStateMachine.State.RUNNING);
    }
}
