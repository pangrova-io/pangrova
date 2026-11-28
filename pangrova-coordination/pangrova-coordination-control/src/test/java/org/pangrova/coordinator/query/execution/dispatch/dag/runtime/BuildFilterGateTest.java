package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.FieldIdStability;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.connector.api.metadata.Domain;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorReadSchemaIdentity;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitFetcher;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.metadata.catalog.MetadataResolvedTable;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
import org.pangrova.query.profiling.enums.RuntimeFilterOutcome;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class BuildFilterGateTest {
    private static final int BUILD = 0;
    private static final int PROBE = 1;

    @Test
    void completedBuildReleasesProbeAndCancelsItsDeadline() {
        var dag = dag();
        var gate = onlyGate(dag);
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer, deadline()).toCompletableFuture();
            assertThat(release).isNotDone();
            assertThat(gate.blocksProbe()).isTrue();

            var ranges = Map.of(0, List.of(new BuildKeyRange(2, 3), new BuildKeyRange(35, 36)),
                    2, List.of(new BuildKeyRange(11, 11)));
            gate.onFinished(completeBuild(dag, ranges));

            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.APPLIED);
            assertThat(gate.probeRanges(PROBE)).isEqualTo(ranges);
            assertThat(release.join().rangeCount()).isEqualTo(3);
            assertThat(gate.buildOrdinals()).containsExactly(0, 2);
            assertThat(gate.blocksProbe()).isFalse();
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void deadlineReleasesTheProbeAndRetainsAFinishedBuildForEnumeration() {
        var dag = dag();
        var gate = onlyGate(dag);
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer, deadline()).toCompletableFuture();
            assertThat(release).isNotDone();

            timer.expire();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.TIMED_OUT);
            assertThat(gate.probeRanges(PROBE)).isNull();
            assertThat(gate.blocksProbe()).isFalse();
            assertThat(timer.getQueue()).isEmpty();

            var ranges = Map.of(0, List.of(new BuildKeyRange(2, 3)), 2, List.of(new BuildKeyRange(11, 11)));
            gate.onFinished(completeBuild(dag, ranges));

            assertThat(gate.probeRanges(PROBE)).isEqualTo(ranges);
            assertThat(gate.probeRanges(BUILD)).isNull();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.TIMED_OUT);
        }
    }

    @Test
    void finishedBuildAfterTimeoutPrunesOneExactEnumerationAndDoesNotCrossSnapshots() {
        var dag = dag();
        var gate = onlyGate(dag);
        var session = new QuerySession("probe-query", Map.of(),
                new UserIdentity("reader", Set.of(), Map.of(), Instant.EPOCH), "tenant",
                Optional.empty(), new OtelExecutionTraceContextFactory().capture());
        var loads = new AtomicInteger();
        var fetcher = new StageSplitFetcher(null, (fragment, sources, overrides, actor, granularity) -> {
            assertThat(actor).isSameAs(session);
            assertThat(sources).singleElement().satisfies(source -> {
                var domains = source.constraint().getDomains();
                if (loads.getAndIncrement() == 0) {
                    assertThat(source.readBinding()).containsSame(gate.plan().probeRead());
                    assertThat(domains.values()).containsExactlyInAnyOrder(
                            new Domain.Range(2L, 3L, true, true), new Domain.Range(11L, 11L, true, true));
                } else {
                    assertThat(source.readBinding().orElseThrow().snapshotId()).hasValue(8);
                    assertThat(domains).isEmpty();
                }
            });
            return List.of();
        });
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer, deadline()).toCompletableFuture();
            timer.expire();
            gate.onFinished(completeBuild(dag, Map.of(
                    0, List.of(new BuildKeyRange(2, 3)), 2, List.of(new BuildKeyRange(11, 11)))));
            fetcher.fetch(dag.findStage(PROBE).plan(), session, Map.of(), null, gate, FileGranularity.WHOLE_OBJECT);

            var drift = probeFragment(cluster(), selected(8));
            fetcher.fetch(new StageNode(PROBE, drift, 1, "COMPUTE", List.of()), session,
                    Map.of(), null, gate, FileGranularity.WHOLE_OBJECT);
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.TIMED_OUT);
            assertThat(loads).hasValue(2);
        }
    }

    @Test
    void terminalQueryRetiresTheBuildEvenWhenTheProbeHadAlreadyTimedOut() {
        var dag = dag();
        var gate = onlyGate(dag);
        var query = new CompletableFuture<>();
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(query, timer, deadline()).toCompletableFuture();
            timer.expire();
            query.complete(null);
            var ranges = Map.of(0, List.of(new BuildKeyRange(2, 3)), 2, List.of(new BuildKeyRange(11, 11)));
            gate.onFinished(completeBuild(dag, ranges));

            assertThat(gate.probeRanges(PROBE)).isNull();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.TIMED_OUT);
        }
    }

    @Test
    void queryTerminationRemovesDeadlineAndRetainsFirstDecision() {
        var gate = onlyGate(dag());
        var query = new CompletableFuture<>();
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(query, timer, deadline()).toCompletableFuture();
            query.cancel(false);
            timer.expire();

            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.QUERY_TERMINATED);
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void emptyBuildAlreadyAvailableNeedsNoDeadline() {
        var dag = dag();
        var gate = onlyGate(dag);
        gate.onFinished(completeBuild(dag, Map.of(0, List.of(new BuildKeyRange(2, 3)), 2, List.of())));
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer, deadline()).toCompletableFuture();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.EMPTY_BUILD);
            assertThat(gate.probeRanges(PROBE)).isEmpty();
            assertThat(timer.getQueue()).isEmpty();
        }
    }

    @Test
    void independentProbesReleaseSeparatelyInsideOneSharedDeadline() {
        var dag = dag(2);
        var gates = BuildFilterGate.forLiveQuery(dag);
        assertThat(gates).hasSize(2);
        var first = gates.get(0);
        var second = gates.get(1);
        var query = new CompletableFuture<>();
        long deadline = deadline();
        try (var firstTimer = new Deadline(); var secondTimer = new Deadline()) {
            var firstRelease = first.releaseProbe(query, firstTimer, deadline).toCompletableFuture();
            var secondRelease = second.releaseProbe(query, secondTimer, deadline).toCompletableFuture();
            var ranges = Map.of(0, List.of(new BuildKeyRange(2, 3)), 2, List.of(new BuildKeyRange(11, 11)));
            var firstBuild = completeBuild(dag, ranges);
            first.onFinished(firstBuild);
            second.onFinished(firstBuild);

            assertThat(firstRelease.join().outcome()).isEqualTo(RuntimeFilterOutcome.APPLIED);
            assertThat(first.probeRanges(PROBE)).isEqualTo(ranges);
            assertThat(secondRelease).isNotDone();
            assertThat(second.blocksProbe()).isTrue();

            secondTimer.expire();
            assertThat(secondRelease.join().outcome()).isEqualTo(RuntimeFilterOutcome.TIMED_OUT);
            assertThat(second.probeRanges(PROBE + 2)).isNull();
            assertThat(firstTimer.getQueue()).isEmpty();
            assertThat(secondTimer.getQueue()).isEmpty();
        }
    }

    @Test
    void boundsTheNumberOfActivePairsAndBreaksTiesByProbeStage() {
        var gates = BuildFilterGate.forLiveQuery(dag(6));
        assertThat(gates).hasSize(BuildFilterGate.MAX_FILTERS_PER_QUERY)
                .extracting(gate -> gate.plan().probeStageId()).containsExactly(1, 3, 5, 7);
    }

    @Test
    void incompleteOrdinalsReleaseTheFullProbe() {
        var dag = dag();
        var gate = onlyGate(dag);
        gate.onFinished(completeBuild(dag, Map.of(0, List.of())));
        try (var timer = new Deadline()) {
            var release = gate.releaseProbe(new CompletableFuture<>(), timer, deadline()).toCompletableFuture();
            assertThat(release.join().outcome()).isEqualTo(RuntimeFilterOutcome.MISSING_SUMMARY);
            assertThat(gate.probeRanges(PROBE)).isNull();
        }
    }

    private static Stage completeBuild(StageDAG dag, Map<Integer, List<BuildKeyRange>> ranges) {
        return completeBuild(dag, BUILD, ranges);
    }

    private static Stage completeBuild(StageDAG dag, int buildId, Map<Integer, List<BuildKeyRange>> ranges) {
        var build = dag.findStage(buildId);
        build.addTaskId("build-task");
        assertThat(build.stateMachine().tryTransitionTo(StageState.SCHEDULING)).isTrue();
        assertThat(build.stateMachine().tryTransitionTo(StageState.RUNNING)).isTrue();
        assertThat(build.onTaskFinished("build-task", build.currentGeneration(), ranges)).isTrue();
        assertThat(build.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        return build;
    }

    private static BuildFilterGate onlyGate(StageDAG dag) {
        var gates = BuildFilterGate.forLiveQuery(dag);
        assertThat(gates).hasSize(1);
        return gates.get(0);
    }

    private static long deadline() {
        return System.nanoTime() + BuildFilterGate.MAX_WAIT_NANOS;
    }

    private static StageDAG dag() {
        return dag(1);
    }

    private static StageDAG dag(int pairCount) {
        var cluster = cluster();
        var fragment = new PlanFragment(LogicalValues.createOneRow(cluster), 256, 4);
        var selected = selected(7);
        var probe = probeFragment(cluster, selected);
        int outputId = 2 * pairCount;
        var stages = new ArrayList<StageNode>();
        var exchanges = new ArrayList<StageEdge>();
        var dependencies = new ArrayList<Integer>();
        var filters = new ArrayList<StageRuntimeFilter>();
        for (int buildId = 0; buildId < outputId; buildId += 2) {
            int probeId = buildId + 1;
            stages.add(new StageNode(buildId, fragment, 1, "COMPUTE", List.of(), 256, 4));
            stages.add(new StageNode(probeId, probe, 1, "COMPUTE", List.of()));
            var buildEdge = new StageEdge(buildId, outputId, ShuffleType.HASH, List.of(0));
            exchanges.add(buildEdge);
            exchanges.add(new StageEdge(probeId, outputId, ShuffleType.HASH, List.of(0)));
            dependencies.addAll(List.of(buildId, probeId));
            filters.add(new StageRuntimeFilter(buildId, buildEdge.exchangeId(), probeId, selected,
                    List.of(new StageRuntimeFilter.ProbeKey(0, 1, "id"),
                            new StageRuntimeFilter.ProbeKey(2, 2, "tag"))));
        }
        stages.add(new StageNode(outputId, fragment, 1, "OUTPUT", List.of()));
        return new StageDAG(List.of(), stages, Map.of(outputId, dependencies),
                exchanges, Map.of(), Map.of(), Map.of(), filters).withStages("query");
    }

    private static RelOptCluster cluster() {
        return RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
    }

    private static ConnectorReadBinding selected(long snapshot) {
        var columns = List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT, OptionalInt.of(1), null),
                new ConnectorTableMetadata.Column("tag", PangrovaType.BIGINT, OptionalInt.of(2), null));
        var declaration = new ConnectorTableMetadata(
                new SchemaTableName("default", "items"), List.of("id", "tag"), columns, Map.of());
        var schema = new ConnectorReadSchemaIdentity(
                new TableIdentity("ICEBERG", "items-uuid", "s3://test/items", "schema-1"),
                "test-items", columns, FieldIdStability.SELECTED_SOURCE);
        return new ConnectorReadBinding(new ConnectorTableHandle() { },
                new ConnectorTableReadMetadata(declaration, TableDeclarationEvidence.EXACT),
                new TableStatistics(2_048, 32_768, 40, Map.of()),
                ReadSnapshotState.SNAPSHOT, OptionalLong.of(snapshot), "selected-items",
                Optional.of(schema), Optional.empty());
    }

    private static PlanFragment probeFragment(RelOptCluster cluster, ConnectorReadBinding binding) {
        var table = MetadataResolvedTable.fromReadBinding(() -> binding);
        var qualified = RelOptTableImpl.create(null, table.getRowType(cluster.getTypeFactory()),
                List.of("lake", "default", "items"), table, ignored -> null);
        return new PlanFragment(LogicalTableScan.create(cluster, qualified, List.of()), 32_768, 2_048);
    }

    private static final class Deadline extends ScheduledThreadPoolExecutor implements AutoCloseable {
        private Runnable timeout;

        private Deadline() {
            super(1);
            setRemoveOnCancelPolicy(true);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
            assertThat(unit.toNanos(delay)).isBetween(0L, BuildFilterGate.MAX_WAIT_NANOS);
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
