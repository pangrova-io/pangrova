package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.logical.LogicalTableScan;
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
import org.pangrova.planner.metadata.catalog.MetadataResolvedTable;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;

import java.time.Instant;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the orchestration seam; actual connector authorization is covered by the integration suite. */
class StageSplitFetcherTest {
    @Test
    void explicitSourceReceivesExactQueryAuthorityAndPinnedSelectors() {
        var session = session();
        var fragment = fragment();
        var overrides = Map.of("lake.public.items", 17L);
        var expected = List.of(new QuerySplit(split("encoded-query-split"), List.of("s3://bucket/object")));
        var calls = new AtomicInteger();
        var fetcher = new StageSplitFetcher(null, (supplied, sources, snapshots, actor, granularity) -> {
            calls.incrementAndGet();
            assertThat(supplied).isSameAs(fragment);
            assertThat(snapshots).isEqualTo(overrides);
            assertThat(granularity).isEqualTo(FileGranularity.BYTE_RANGES);
            assertThat(actor).isSameAs(session);
            return expected;
        });
        assertThat(fetcher.fetch(stage(fragment), session, overrides, null, null, FileGranularity.BYTE_RANGES).splits())
                .containsExactlyElementsOf(expected);
        assertThat(calls).hasValue(1);
    }

    @Test
    void enumeratedSourceRetainsItsSnapshotAndStillFencesOwnerChanges() {
        var selection = new AtomicReference<>(snapshotBinding(17));
        var table = MetadataResolvedTable.fromReadBinding(selection::get);
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        var qualified = RelOptTableImpl.create(null, table.getRowType(cluster.getTypeFactory()),
                List.of("lake", "public", "items"), table, ignored -> null);
        var scan = LogicalTableScan.create(cluster, qualified, List.of());
        var fragment = new PlanFragment(scan, -1, -1);
        var original = selection.get();
        var fetcher = new StageSplitFetcher(null, (ignored, sources, snapshots, actor, granularity) -> {
            assertThat(sources).singleElement().satisfies(source ->
                    assertThat(source.readBinding()).containsSame(original));
            selection.set(snapshotBinding(18));
            return List.of(new QuerySplit(split("old"), List.of("s3://bucket/old")));
        });

        var result = fetcher.fetch(stage(fragment), session(), Map.of(), null);
        assertThat(result.sources().descriptors()).singleElement().satisfies(source ->
                assertThat(source.readBinding()).containsSame(original));
        assertThatThrownBy(result.sources()::requireCurrent).hasMessageContaining("split source binding changed");
    }

    private static ConnectorReadBinding snapshotBinding(long snapshot) {
        var declaration = new ConnectorTableMetadata(new SchemaTableName("public", "items"),
                List.of("id"), List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT)), Map.of());
        return new ConnectorReadBinding(new ConnectorTableHandle() {},
                new ConnectorTableReadMetadata(declaration, TableDeclarationEvidence.EXACT),
                TableStatistics.UNKNOWN, ReadSnapshotState.SNAPSHOT, OptionalLong.of(snapshot), "selected-items",
                Optional.empty(), Optional.empty());
    }

    @Test
    void failedOrCancelledProviderNeverFallsBackToAnEmptyCatalogResult() {
        var session = session();
        for (RuntimeException failure : List.of(new AccessDeniedException("revoked"),
                new CancellationException("query cancelled"), new IllegalStateException("provider unavailable"))) {
            var fetcher = new StageSplitFetcher(null, (fragment, sources, snapshots, actor, granularity) -> {
                throw failure;
            });
            assertThatThrownBy(() -> fetcher.fetch(stage(fragment()), session, Map.of(), null))
                    .isSameAs(failure);
        }
    }

    @Test
    void missingCatalogRegistryCannotProduceASuccessfulEmptyTableScan() {
        var source = new org.pangrova.planner.analysis.SplitSourceDescriptor(
                "lake", "public", "items", org.pangrova.connector.api.metadata.Constraint.EMPTY,
                List.of("id"), Optional.empty());
        assertThatThrownBy(() -> org.pangrova.coordinator.query.execution.split.SplitFetcher
                .fetchRawConnectorSplits(List.of(source), null, Map.of(), List.of("id"), session(), FileGranularity.WHOLE_OBJECT))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("configured catalog registry");
    }

    @Test
    void wholeTableReplacementDoesNotInvokeAnIndexPruningSource() {
        var session = session();
        var calls = new AtomicInteger();
        var fetcher = new StageSplitFetcher(null, (fragment, sources, snapshots, actor, granularity) -> {
            calls.incrementAndGet();
            throw new AssertionError("A whole-table replacement must enumerate its unpruned read");
        });
        for (String mode : List.of("OVERWRITE", "UPDATE")) {
            // A real source-free Calcite plan has no catalog splits; this checks routing, not table I/O.
            assertThat(fetcher.fetch(stage(fragment()), session, Map.of(), mode).splits())
                    .isEmpty();
        }
        assertThat(calls).hasValue(0);
        assertThatThrownBy(() -> fetcher.fetch(stage(fragment()), null, Map.of(), null))
                .hasMessageContaining("authenticated split lookup");
    }

    @Test
    void partitionSourceCannotMutatePinnedSnapshotSelectors() {
        var selectors = new java.util.HashMap<>(Map.of("lake.public.items", 17L));
        var fetcher = new StageSplitFetcher(null, (fragment, sources, snapshots, actor, granularity) -> {
            assertThatThrownBy(() -> snapshots.put("lake.public.items", 18L))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(granularity).isEqualTo(FileGranularity.WHOLE_OBJECT);
            return List.of();
        });
        fetcher.fetch(stage(fragment()), session(), selectors, null);
        assertThat(selectors).containsEntry("lake.public.items", 17L);
    }

    private static QuerySession session() {
        return new QuerySession("query-source", Map.of(),
                new UserIdentity("reader", Set.of(), Map.of(), Instant.EPOCH), "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static PlanFragment fragment() {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        return new PlanFragment(LogicalValues.createOneRow(cluster), -1, -1);
    }

    private static StageNode stage(PlanFragment fragment) {
        return new StageNode(0, fragment, 1, "OUTPUT", List.of());
    }
    private static DataSplit split(String id) {
        return DataSplit.newBuilder().setSplitId(id).build();
    }

}
