package org.pangrova.query.core.lifecycle;

import org.pangrova.query.api.lifecycle.QueryState;
import org.pangrova.query.api.stage.StageState;

import org.pangrova.query.core.stage.StageStateMachine;
import org.pangrova.query.api.error.QueryFailure;
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
        qsm.updateStageState("0", StageState.SCHEDULING);
        qsm.updateStageState("1", StageState.PLANNED);

        assertThat(qsm.getStageState("0"))
                .hasValue(StageState.SCHEDULING);
        assertThat(qsm.getStageState("1"))
                .hasValue(StageState.PLANNED);
        assertThat(qsm.getStageState("99")).isEmpty();
    }

    @Test
    void getStageStatesReturnsImmutableSnapshot() {
        QueryStateMachine qsm = new QueryStateMachine();
        qsm.updateStageState("0", StageState.RUNNING);
        qsm.updateStageState("1", StageState.FINISHED);

        Map<String, StageState> snapshot = qsm.getStageStates();
        assertThat(snapshot).hasSize(2);
        assertThat(snapshot.get("0")).isEqualTo(StageState.RUNNING);
        assertThat(snapshot.get("1")).isEqualTo(StageState.FINISHED);
    }

    @Test
    void onStageFailedTransitionsQueryToFailed() {
        QueryStateMachine qsm = new QueryStateMachine();
        qsm.tryTransition(QueryState.PLANNING);
        qsm.tryTransition(QueryState.SCHEDULING);
        qsm.tryTransition(QueryState.RUNNING);

        QueryFailure error = new QueryFailure("INTERNAL", "INTERNAL", "stage failed");
        qsm.onStageFailed("0", error);

        assertThat(qsm.getState()).isEqualTo(QueryState.FAILED);
        assertThat(qsm.getStageState("0"))
                .hasValue(StageState.FAILED);
        assertThat(qsm.getError()).hasValue(error);
    }

    @Test
    void stageListenerUpdatesQueryStateMachine() {
        QueryStateMachine qsm = new QueryStateMachine();
        StageStateMachine ssm = new StageStateMachine("0", "q1");

        // Wire listener
        ssm.addStateChangeListener(
                (stageId, oldState, newState) -> qsm.updateStageState(stageId, newState));

        ssm.transitionTo(StageState.SCHEDULING);
        assertThat(qsm.getStageState("0"))
                .hasValue(StageState.SCHEDULING);

        ssm.transitionTo(StageState.RUNNING);
        assertThat(qsm.getStageState("0"))
                .hasValue(StageState.RUNNING);
    }
}
