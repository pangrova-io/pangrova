package org.pangrova.coordinator.query.stage;

import org.pangrova.query.api.stage.StageState;

import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.core.stage.StageStateMachine;

import org.pangrova.query.api.error.QueryFailure;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Stage}.
 * Validates: Requirements 1.2, 1.4, 1.6
 */
class StageTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RelOptCluster CLUSTER = RelOptCluster.create(
            new VolcanoPlanner(), new RexBuilder(TYPE_FACTORY));

    private StageNode dummyNode(int id) {
        RelNode plan = LogicalValues.createOneRow(CLUSTER);
        return new StageNode(id, new org.pangrova.planner.core.analysis.PlanFragment(plan, -1L, -1L), 2, "COMPUTE", List.of());
    }

    @Test
    void createFactoryProducesValidStage() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        assertThat(stage.stageId()).isEqualTo(0);
        assertThat(stage.plan().stageId()).isEqualTo(0);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.PLANNED);
        assertThat(stage.retryPolicy()).isEqualTo(StageRetryPolicy.DEFAULT);
        assertThat(stage.taskIds()).isEmpty();
    }

    @Test
    void onTaskFinishedTransitionsToFinishedWhenAllDone() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);

        stage.addTaskId("t1");
        stage.addTaskId("t2");

        stage.onTaskFinished("t1");
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.RUNNING);

        stage.onTaskFinished("t2");
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
    }

    @Test
    void onTaskFailedTransitionsToFailedWhenRetryExhausted() {
        Stage stage = new Stage(dummyNode(0),
                new StageStateMachine("0", "q1"), StageRetryPolicy.NO_RETRY);
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);

        stage.addTaskId("t1");
        QueryFailure error = new QueryFailure("INTERNAL", "INTERNAL", "boom");
        stage.onTaskFailed("t1", error);

        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FAILED);
        assertThat(stage.lastError()).isEqualTo(error);
    }

    @Test
    void shouldRetryReturnsTrueWhenRetriesRemain() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        assertThat(stage.shouldRetry()).isTrue();
        assertThat(stage.retryCount()).isEqualTo(0);
    }

    @Test
    void tryPrepareRetryClearsTaskTracking() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.addTaskId("t1");
        boolean prepared = stage.tryPrepareRetry();

        assertThat(prepared).isTrue();
        assertThat(stage.taskIds()).isEmpty();
        assertThat(stage.retryCount()).isEqualTo(1);
    }

    @Test
    void retryExhaustedAfterMaxRetries() {
        Stage stage = new Stage(dummyNode(0),
                new StageStateMachine("0", "q1"), new StageRetryPolicy(1));
        assertThat(stage.shouldRetry()).isTrue();

        assertThat(stage.tryPrepareRetry()).isTrue();
        assertThat(stage.shouldRetry()).isFalse();
        assertThat(stage.tryPrepareRetry()).isFalse();
    }

    @Test
    void taskFailedTransitionsToFailedWithRetryHandledExternally() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);

        stage.addTaskId("t1");
        stage.onTaskFailed("t1", new QueryFailure("INTERNAL", "INTERNAL", "boom"));

        // Stage always transitions to FAILED; RetryStageHandler decides retry externally
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FAILED);
    }

    @Test
    void stageRetryPolicyRejectsNegativeMaxRetries() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StageRetryPolicy(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
