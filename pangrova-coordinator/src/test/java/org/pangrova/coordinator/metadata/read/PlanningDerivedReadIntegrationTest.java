package org.pangrova.coordinator.metadata.read;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.metadata.ReadBindingSdkFixture;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorDerivedQuery;
import org.pangrova.spi.connector.metadata.table.ConnectorDerivedTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.testing.ContainerTestBase;

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
                    new ConnectorTableReadMetadata(metadata, ConnectorTableReadMetadata.DeclarationEvidence.EXACT),
                    TableStatistics.UNKNOWN, ConnectorReadBinding.SnapshotState.METADATA,
                    OptionalLong.empty(), selected.sourceIdentity());
            var root = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(false, false);
            var catalog = root.add("bound", new org.apache.calcite.schema.impl.AbstractSchema());
            var schema = catalog.add("db", new org.apache.calcite.schema.impl.AbstractSchema());
            schema.add("summary", new MetadataResolvedTable(metadata, () -> derived));
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
