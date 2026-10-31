package org.pangrova.coordinator.query.execution;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.metadata.read.PlanningReadBindings;
import org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors;
import org.pangrova.coordinator.metadata.ReadBindingSdkFixture;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual parser/session/PG/plugin/SDK/schema/plan/split path, not a Native execution proof. */
class TimeTravelReadBindingIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void unqualifiedHistoricalSourceIsSelectedBeforeCurrentSchemaCouldRejectItsOldColumn() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("history");
            ReadBindingSdkFixture.write(table, 2);
            long version = table.snapshotManager().latestSnapshotId();
            fixture.sdk.alterTable(Identifier.create("db", "history"),
                    List.of(SchemaChange.renameColumn("id", "current_id")), false);
            var extracted = TimeTravelExtractor.extract(
                    "SELECT id FROM history FOR SYSTEM_VERSION AS OF " + version, fixture.context, null);
            assertThat(extracted.snapshotOverrides()).containsEntry("bound.db.history", version);
            var session = PlanningSnapshotSelectors.bind(fixture.session, extracted.snapshotOverrides());
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), session);
            var schema = fixture.bootstrap.buildCalciteSchema(session, owner);
            var plan = new PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication(extracted.strippedSql()).toPlannedQuery();
            var binding = owner.resolve("bound", "db", "history").orElseThrow();
            assertThat(binding.declaration().metadata().columns()).containsExactly("id");
            assertThat(plan.splitSources()).singleElement().satisfies(source ->
                    assertThat(source.readBinding()).contains(binding));
            assertThat(fixture.splits(binding, extracted.snapshotOverrides())).isNotEmpty().allSatisfy(split -> {
                try {
                    assertThat(DataSplit.parseFrom(split.toSplitInfoProto()).getSourceSnapshot()).isEqualTo(Long.toString(version));
                } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                    throw new AssertionError(error);
                }
            });
            var current = fixture.bootstrap.buildCalciteSchema(fixture.session,
                    new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session));
            assertThatThrownBy(() -> new PangrovaPlanner(current, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM history")).hasMessageContaining("id");
        }
    }
}
