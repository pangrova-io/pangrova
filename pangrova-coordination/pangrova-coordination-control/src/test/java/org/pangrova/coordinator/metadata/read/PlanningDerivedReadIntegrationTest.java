package org.pangrova.coordinator.metadata.read;

import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.planner.metadata.catalog.MetadataResolvedTable;
import org.pangrova.planner.metadata.catalog.PlanningReadBindings;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.metadata.ReadBindingSdkFixture;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorDerivedQuery;
import org.pangrova.connector.api.metadata.table.ConnectorDerivedTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.testing.testkit.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/** Real PG/plugin/SDK/Calcite/split chain; does not claim Native execution or distributed E2E. */
class PlanningDerivedReadIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void aggregateExpansionKeepsTheSelectedSdkSnapshotThroughLaterCommitsAndSplitEncoding() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("rows");
            ReadBindingSdkFixture.write(table, 3);
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            var selected = owner.resolve("bound", "db", "rows").orElseThrow();
            var definition = new ConnectorDerivedQuery(
                    "SELECT COUNT(*) AS record_count, SUM(id) AS id_sum FROM selected_input",
                    List.of(new ConnectorDerivedQuery.Input("selected_input", selected)));
            var columns = List.of(new ConnectorTableMetadata.Column("record_count", PangrovaType.BIGINT),
                    new ConnectorTableMetadata.Column("id_sum", PangrovaType.BIGINT));
            var metadata = new ConnectorTableMetadata(new SchemaTableName("db", "summary"),
                    List.of("record_count", "id_sum"), columns, Map.of());
            var derived = new ConnectorReadBinding(new DerivedHandle(definition),
                    new ConnectorTableReadMetadata(metadata, TableDeclarationEvidence.EXACT),
                    TableStatistics.UNKNOWN, ReadSnapshotState.METADATA,
                    OptionalLong.empty(), selected.sourceIdentity(), java.util.Optional.empty(), java.util.Optional.empty());
            var root = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false, false);
            var catalog = root.add("bound", new org.apache.calcite.schema.impl.AbstractSchema());
            var schema = catalog.add("db", new org.apache.calcite.schema.impl.AbstractSchema());
            var lookups = new java.util.concurrent.atomic.AtomicInteger();
            var resolved = MetadataResolvedTable.fromReadBinding(() -> {
                lookups.incrementAndGet();
                return derived;
            });
            assertThat(resolved.unwrap(org.apache.calcite.rel.metadata.BuiltInMetadata.Predicates.Handler.class))
                    .isNull();
            assertThat(resolved.unwrap(MetadataResolvedTable.class)).isSameAs(resolved);
            assertThat(lookups).hasValue(0);
            assertThat(resolved.unwrap(ConnectorReadBinding.class)).isSameAs(derived);
            assertThat(lookups).hasValue(1);
            schema.add("summary", resolved);
            var plan = new PangrovaPlanner(root, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT record_count, id_sum FROM summary").toPlannedQuery();
            assertThat(plan.tableReferences()).containsExactly(List.of("bound", "db", "rows"));
            assertThat(plan.splitSources()).singleElement().satisfies(source ->
                    assertThat(source.readBinding()).containsSame(selected));
            ReadBindingSdkFixture.write(table, 5);
            assertThat(new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "rows").orElseThrow().statistics().rowCount()).isEqualTo(8);
            assertThat(selected.statistics().rowCount()).isEqualTo(3);
            assertThat(fixture.splits(plan.splitSources().get(0).readBinding().orElseThrow(), Map.of()))
                    .isNotEmpty().allSatisfy(split -> {
                        try {
                            var wire = DataSplit.parseFrom(split.toSplitInfoProto());
                            assertThat(wire.getSourceSnapshot()).isEqualTo(
                                    Long.toString(selected.snapshotId().orElseThrow()));
                        } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                            throw new AssertionError(error);
                        }
                    });
        }
    }

    private record DerivedHandle(ConnectorDerivedQuery derivedQuery) implements ConnectorDerivedTableHandle {}
}
