package org.pangrova.coordinator.query.stage;

import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;

import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.stage.StageStateMachine;

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
import java.util.Map;

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
        return new StageNode(id, new org.pangrova.planner.analysis.PlanFragment(plan, -1L, -1L), 2, "COMPUTE", List.of());
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
    void buildRangeUsesExactlyOneSuccessfulReportForEveryCurrentSlot() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
        stage.addTaskId("first");
        stage.addTaskId("second");
        int generation = stage.currentGeneration();

        stage.onTaskFinished("first", generation, Map.of(0, List.of(new BuildKeyRange(-8, -2))));
        stage.onTaskFinished("first", generation, Map.of(0, List.of(new BuildKeyRange(Long.MIN_VALUE, Long.MAX_VALUE))));
        stage.onTaskFinished("second", generation + 1, Map.of(0, List.of(new BuildKeyRange(-40, 40))));
        assertThat(stage.completeBuildKeyRanges()).isNull();
        stage.onTaskFinished("second", generation, Map.of(0, List.of()));

        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        assertThat(stage.completeBuildKeyRanges().get(0)).containsExactly(new BuildKeyRange(-8, -2));
    }

    @Test
    void missingBuildRangePreservesFullProbeEvenWhenEveryTaskSucceeded() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
        stage.addTaskId("first");
        stage.addTaskId("second");
        stage.onTaskFinished("first", stage.currentGeneration(), Map.of(0, List.of()));
        stage.onTaskFinished("second");

        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        assertThat(stage.completeBuildKeyRanges()).isNull();
    }

    @Test
    void completeBuildPreservesGapsAndCoalescesOverlappingOrAdjacentSignedRanges() {
        var ranges = List.of(new BuildKeyRange(Long.MAX_VALUE, Long.MAX_VALUE),
                new BuildKeyRange(12, 15), new BuildKeyRange(0, 1),
                new BuildKeyRange(Long.MIN_VALUE, Long.MIN_VALUE),
                new BuildKeyRange(2, 3), new BuildKeyRange(11, 12),
                new BuildKeyRange(Long.MAX_VALUE - 2, Long.MAX_VALUE - 1));
        assertThat(completedBuild(ranges).completeBuildKeyRanges().get(0)).containsExactly(
                new BuildKeyRange(Long.MIN_VALUE, Long.MIN_VALUE), new BuildKeyRange(0, 3),
                new BuildKeyRange(11, 15), new BuildKeyRange(Long.MAX_VALUE - 2, Long.MAX_VALUE));
        assertThat(completedBuildSummaries(List.of(List.of())).completeBuildKeyRanges().get(0)).isEmpty();
    }

    @Test
    void excessiveDisjointBuildRangesUseConservativeBoundedCoverage() {
        var ranges = java.util.stream.LongStream.range(0, 17)
                .mapToObj(i -> new BuildKeyRange(2 * i, 2 * i)).toList();
        assertThat(completedBuild(ranges.subList(0, 16)).completeBuildKeyRanges().get(0)).hasSize(16);
        assertThat(completedBuild(ranges).completeBuildKeyRanges().get(0)).containsExactly(new BuildKeyRange(0, 32));
    }

    @Test
    void laterWinningReportsExpandTheBoundedCoverageAfterDisjointOverflow() {
        var ranges = new java.util.ArrayList<BuildKeyRange>();
        for (long key = 0; key <= 32; key += 2) ranges.add(new BuildKeyRange(key, key));
        ranges.add(new BuildKeyRange(4, 16));
        ranges.add(new BuildKeyRange(Long.MIN_VALUE, -10));
        ranges.add(new BuildKeyRange(40, Long.MAX_VALUE));
        var stage = completedBuild(ranges);
        var result = stage.completeBuildKeyRanges();

        assertThat(result.get(0)).containsExactly(new BuildKeyRange(Long.MIN_VALUE, Long.MAX_VALUE));
        assertThat(stage.completeBuildKeyRanges()).isSameAs(result);
    }

    private Stage completedBuild(List<BuildKeyRange> ranges) {
        return completedBuildSummaries(ranges.stream().map(List::of).toList());
    }

    private Stage completedBuildSummaries(List<List<BuildKeyRange>> summaries) {
        var stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
        for (int index = 0; index < summaries.size(); index++) stage.addTaskId("task-" + index);
        for (int index = 0; index < summaries.size(); index++) {
            stage.onTaskFinished("task-" + index, stage.currentGeneration(), Map.of(0, summaries.get(index)));
        }
        return stage;
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
    void onlyAFailedGenerationCanSpendItsRetryBudget() {
        var stage = new Stage(dummyNode(0), new StageStateMachine("0", "q1"), new StageRetryPolicy(1));
        for (int attempt = 0; attempt < 2; attempt++) {
            stage.stateMachine().transitionTo(StageState.SCHEDULING);
            stage.stateMachine().transitionTo(StageState.RUNNING);
            stage.addTaskId("t" + attempt);
            assertThat(stage.tryRestartFailedGeneration(attempt, 3, "WORKER_LOST")).isFalse();
            stage.onTaskFailed("t" + attempt, new QueryFailure("WORKER_LOST", "worker", "unavailable"));
            assertThat(stage.tryRestartFailedGeneration(attempt, 3, "WORKER_LOST")).isEqualTo(attempt == 0);
            if (attempt == 0) assertThat(stage.taskIds()).isEmpty();
        }
        assertThat(stage.retryCount()).isOne();
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

    @Test
    void taskRetryReplacesTheSlotAndKeepsTheStageRunning() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
        stage.addTaskId("t1");
        stage.addTaskId("t2");
        stage.onTaskFinished("t2");

        assertThat(stage.tryPrepareTaskRetry("t1", "r1", 3)).isTrue();
        assertThat(stage.taskIds()).containsExactlyInAnyOrder("r1", "t2");

        // A stale report for the replaced attempt must not move the stage.
        stage.onTaskFailed("t1", new QueryFailure("WORKER-0002", "WORKER_LOST", "dead"));
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.RUNNING);

        stage.onTaskFinished("r1");
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
    }

    @Test
    void taskRetryBudgetFollowsTheReplacementLineage() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.addTaskId("t1");

        assertThat(stage.tryPrepareTaskRetry("t1", "r1", 1)).isTrue();
        // The first retry itself fails and asks for another replacement.
        assertThat(stage.tryPrepareTaskRetry("r1", "r2", 1)).isFalse();
        assertThat(stage.taskIds()).containsExactly("r1");
        // A denied reservation never consumes an attempt, even if the caller later allows a larger budget.
        assertThat(stage.tryPrepareTaskRetry("r1", "r3", 2)).isTrue();
    }

    @Test
    void rollbackRestoresTheFailedSlotAndRefundsTheBudget() {
        Stage stage = Stage.create(dummyNode(0), "q1");
        stage.addTaskId("t1");

        assertThat(stage.tryPrepareTaskRetry("t1", "r1", 1)).isTrue();
        stage.rollbackTaskRetry("t1", "r1");

        assertThat(stage.taskIds()).containsExactly("t1");
        // The refunded budget allows the next replacement to be admitted.
        assertThat(stage.tryPrepareTaskRetry("t1", "r2", 1)).isTrue();
    }
}
