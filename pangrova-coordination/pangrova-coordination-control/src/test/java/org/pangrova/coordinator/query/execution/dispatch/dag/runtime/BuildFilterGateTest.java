package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
import org.pangrova.query.profiling.enums.RuntimeFilterOutcome;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BuildFilterGateTest {
    private static final int BUILD = 0;
    private static final int PROBE = 1;
    private static final int OUTPUT = 2;

    @Test
    void completedBuildReleasesProbeAndCancelsItsDeadline() {
        var dag = dag();
        var gate = BuildFilterGate.create(dag);
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer).toCompletableFuture();
            assertThat(release).isNotDone();
            assertThat(gate.blocks(PROBE)).isTrue();

            var ranges = Map.of(0, List.of(new BuildKeyRange(2, 3), new BuildKeyRange(35, 36)),
                    2, List.of(new BuildKeyRange(11, 11)));
            gate.onFinished(completeBuild(dag, ranges));

            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.APPLIED);
            assertThat(gate.probeRanges(PROBE)).isEqualTo(ranges);
            assertThat(release.join().rangeCount()).isEqualTo(3);
            assertThat(gate.buildOrdinals(BUILD)).containsExactly(0, 2);
            assertThat(gate.blocks(PROBE)).isFalse();
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void deadlineReleasesWithoutRangesAndIgnoresLateBuild() {
        var dag = dag();
        var gate = BuildFilterGate.create(dag);
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer).toCompletableFuture();
            assertThat(release).isNotDone();

            timer.expire();
            gate.onFinished(completeBuild(dag, Map.of(0, List.of(new BuildKeyRange(2, 3)), 2, List.of(new BuildKeyRange(11, 11)))));

            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.TIMED_OUT);
            assertThat(gate.probeRanges(PROBE)).isNull();
            assertThat(gate.blocks(PROBE)).isFalse();
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void queryTerminationRemovesDeadlineAndRetainsFirstDecision() {
        var gate = BuildFilterGate.create(dag());
        var query = new CompletableFuture<>();
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(query, timer).toCompletableFuture();
            query.cancel(false);
            timer.expire();

            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.QUERY_TERMINATED);
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void emptyBuildAlreadyAvailableNeedsNoDeadline() {
        var dag = dag();
        var gate = BuildFilterGate.create(dag);
        gate.onFinished(completeBuild(dag, Map.of(0, List.of(new BuildKeyRange(2, 3)), 2, List.of())));
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer).toCompletableFuture();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.EMPTY_BUILD);
            assertThat(gate.probeRanges(PROBE)).isEmpty();
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void incompleteOrdinalsReleaseTheFullProbe() {
        var dag = dag();
        var gate = BuildFilterGate.create(dag);
        gate.onFinished(completeBuild(dag, Map.of(0, List.of())));
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer).toCompletableFuture();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.MISSING_SUMMARY);
            assertThat(gate.probeRanges(PROBE)).isNull();
        }
    }

    private static Stage completeBuild(StageDAG dag, Map<Integer, List<BuildKeyRange>> ranges) {
        var build = dag.findStage(BUILD);
        build.addTaskId("build-task");
        assertThat(build.stateMachine().tryTransitionTo(StageState.SCHEDULING)).isTrue();
        assertThat(build.stateMachine().tryTransitionTo(StageState.RUNNING)).isTrue();
        assertThat(build.onTaskFinished("build-task", build.currentGeneration(), ranges)).isTrue();
        assertThat(build.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        return build;
    }

    private static StageDAG dag() {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        var fragment = new PlanFragment(LogicalValues.createOneRow(cluster), 256, 4);
        var build = new StageNode(BUILD, fragment, 1, "COMPUTE", List.of(), 256, 4);
        var probe = new StageNode(PROBE, fragment, 1, "COMPUTE", List.of());
        var output = new StageNode(OUTPUT, fragment, 1, "OUTPUT", List.of());
        var buildEdge = new StageEdge(BUILD, OUTPUT, ShuffleType.HASH, List.of(0));
        var probeEdge = new StageEdge(PROBE, OUTPUT, ShuffleType.HASH, List.of(0));
        var declaration = new ConnectorTableMetadata(new SchemaTableName("default", "items"),
                List.of("id"), Map.of());
        var selected = new ConnectorReadBinding(new ConnectorTableHandle() { },
                new ConnectorTableReadMetadata(declaration, TableDeclarationEvidence.EXACT),
                new TableStatistics(2_048, 32_768, 40, Map.of()),
                ReadSnapshotState.SNAPSHOT, OptionalLong.of(7), "selected-items", Optional.empty(), Optional.empty());
        var filter = new StageRuntimeFilter(BUILD, buildEdge.exchangeId(), PROBE, selected,
                List.of(new StageRuntimeFilter.ProbeKey(0, 1, "id"),
                        new StageRuntimeFilter.ProbeKey(2, 2, "tag")));
        return new StageDAG(List.of(), List.of(build, probe, output), Map.of(OUTPUT, List.of(BUILD, PROBE)),
                List.of(buildEdge, probeEdge), Map.of(), Map.of(), Map.of(), Optional.of(filter)).withStages("query");
    }

    private static final class Deadline extends ScheduledThreadPoolExecutor implements AutoCloseable {
        private Runnable timeout;

        private Deadline() {
            super(1);
            setRemoveOnCancelPolicy(true);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
            assertThat(unit.toMillis(delay)).isEqualTo(200L);
            timeout = task;
            return super.schedule(task, 1, TimeUnit.DAYS);
        }

        private void expire() {
            timeout.run();
        }

        @Override
        public void close() {
            shutdownNow();
        }
    }
}
