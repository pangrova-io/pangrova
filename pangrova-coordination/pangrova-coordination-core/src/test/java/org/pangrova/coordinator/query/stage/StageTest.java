package org.pangrova.coordinator.query.stage;

import org.pangrova.client.error.ErrorDetail;
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
        return new StageNode(id, new org.pangrova.planner.PlanFragment(plan, -1L, -1L), 2, "COMPUTE", List.of());
    }

    @Test
    void createFactoryProducesValidStage() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        assertThat(stage.stageId()).isEqualTo(0);
        assertThat(stage.plan().stageId()).isEqualTo(0);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageStateMachine.State.PLANNED);
        assertThat(stage.retryPolicy()).isEqualTo(StageRetryPolicy.DEFAULT);
        assertThat(stage.taskIds()).isEmpty();
    }

    @Test
    void onTaskFinishedTransitionsToFinishedWhenAllDone() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().transitionTo(StageStateMachine.State.RUNNING);

        stage.addTaskId("t1");
        stage.addTaskId("t2");

        stage.onTaskFinished("t1");
        assertThat(stage.stateMachine().getState()).isEqualTo(StageStateMachine.State.RUNNING);

        stage.onTaskFinished("t2");
        assertThat(stage.stateMachine().getState()).isEqualTo(StageStateMachine.State.FINISHED);
    }

    @Test
    void onTaskFailedTransitionsToFailedWhenRetryExhausted() {
        Stage stage = new Stage(dummyNode(0),
                new StageStateMachine("0", "q1"), StageRetryPolicy.NO_RETRY);
        stage.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().transitionTo(StageStateMachine.State.RUNNING);

        stage.addTaskId("t1");
        ErrorDetail error = new ErrorDetail("INTERNAL", "INTERNAL", "boom");
        stage.onTaskFailed("t1", error);

        assertThat(stage.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
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
        stage.stateMachine().transitionTo(StageStateMachine.State.SCHEDULING);
        stage.stateMachine().transitionTo(StageStateMachine.State.RUNNING);

        stage.addTaskId("t1");
        stage.onTaskFailed("t1", new ErrorDetail("INTERNAL", "INTERNAL", "boom"));

        // Stage always transitions to FAILED; RetryStageHandler decides retry externally
        assertThat(stage.stateMachine().getState()).isEqualTo(StageStateMachine.State.FAILED);
    }

    @Test
    void stageRetryPolicyRejectsNegativeMaxRetries() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new StageRetryPolicy(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
