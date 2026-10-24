package org.pangrova.coordinator.metadata.read;

import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.coordinator.metadata.ReadBindingSdkFixture;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.testing.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PG/plugin/Calcite/SDK/split chain; does not execute a Native task or claim distributed E2E. */
class PlanningReadBindingsIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void calciteStatisticsCostStatisticsAndSplitWireShareOneActualSource() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var sdkTable = fixture.create("rows");
            ReadBindingSdkFixture.write(sdkTable, 3);
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            var schema = fixture.bootstrap.buildCalciteSchema(fixture.session, owner);
            var table = schema.plus().getSubSchema("bound").getSubSchema("db").getTable("rows");
            assertThat(table).isInstanceOf(MetadataResolvedTable.class);
            var binding = ((MetadataResolvedTable) table).unwrap(ConnectorReadBinding.class);
            var provider = new CatalogStatisticsProvider(owner);
            assertThat(table.getStatistic().getRowCount()).isEqualTo(3.0);
            assertThat(provider.getTableStatistics(new QualifiedTableName("bound", "db", "rows"))).isSameAs(binding.statistics());
            var plan = new org.pangrova.planner.PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM rows").toPlannedQuery();
            assertThat(plan.splitSources()).singleElement().satisfies(source ->
                    assertThat(source.readBinding().orElseThrow()).isSameAs(binding));
            ReadBindingSdkFixture.write(sdkTable, 5);
            assertThat(table.getStatistic().getRowCount()).isEqualTo(3.0);
            assertThat(provider.getTableStatistics(new QualifiedTableName("bound", "db", "rows")).rowCount()).isEqualTo(3);
            assertThat(fixture.splits(binding, Map.of())).isNotEmpty().allSatisfy(split -> {
                try {
                    var wire = DataSplit.parseFrom(split.toSplitInfoProto());
                    assertThat(wire.getSourceSnapshot()).isEqualTo(Long.toString(binding.snapshotId().orElseThrow()));
                    assertThat(wire.getCatalog()).isEqualTo(CatalogExecutionBindings.from(
                            fixture.bootstrap.catalogRegistry().getCatalog("bound").getDescriptor()));
                } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                    throw new AssertionError(error);
                }
            });
            assertThat(new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "rows").orElseThrow().statistics().rowCount()).isEqualTo(8);
        }
    }

    @Test
    void concurrentLookupsShareActualBindingAndDifferentTablesDoNotSharePins() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            ReadBindingSdkFixture.write(fixture.create("first"), 3);
            ReadBindingSdkFixture.write(fixture.create("second"), 7);
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            var executor = Executors.newFixedThreadPool(2);
            try {
                var first = executor.submit(() -> owner.resolve("bound", "db", "first").orElseThrow());
                var repeated = executor.submit(() -> owner.resolve("bound", "db", "first").orElseThrow());
                var binding = first.get(10, TimeUnit.SECONDS);
                assertThat(repeated.get(10, TimeUnit.SECONDS)).isSameAs(binding);
                var second = owner.resolve("bound", "db", "second").orElseThrow();
                assertThat(second.sourceIdentity()).isNotEqualTo(binding.sourceIdentity());
                assertThat(binding.statistics().rowCount()).isEqualTo(3);
                assertThat(second.statistics().rowCount()).isEqualTo(7);
            } finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void explicitVersionBindsHistoricalDeclarationBeforeCalciteValidation() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("history");
            ReadBindingSdkFixture.write(table, 2);
            long version = table.snapshotManager().latestSnapshotId();
            fixture.sdk.alterTable(Identifier.create("db", "history"),
                    List.of(SchemaChange.addColumn("added", DataTypes.STRING())), false);
            var selectedSession = PlanningSnapshotSelectors.bind(fixture.session, Map.of("bound.db.history", version));
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), selectedSession);
            var schema = fixture.bootstrap.buildCalciteSchema(selectedSession, owner);
            var declared = schema.plus().getSubSchema("bound").getSubSchema("db").getTable("history");
            assertThat(declared.getRowType(new JavaTypeFactoryImpl()).getFieldNames()).containsExactly("id");
            var currentOwner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            assertThat(currentOwner.resolve("bound", "db", "history").orElseThrow()
                    .declaration().metadata().columns()).containsExactly("id", "added");
            var binding = owner.resolve("bound", "db", "history").orElseThrow();
            assertThat(fixture.splits(binding, Map.of("bound.db.history", version))).isNotEmpty();
            assertThatThrownBy(() -> fixture.splits(binding, Map.of("bound.db.history", version + 1)))
                    .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                    .hasMessageContaining("differs from execution selector");
        }
    }

    @Test
    void emptyBindingNeverTurnsIntoALiveReadAfterTheFirstWrite() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("empty");
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            var binding = owner.resolve("bound", "db", "empty").orElseThrow();
            assertThat(binding.state()).isEqualTo(ConnectorReadBinding.SnapshotState.EMPTY);
            ReadBindingSdkFixture.write(table, 2);
            assertThat(fixture.splits(binding, Map.of())).isEmpty();
            assertThat(new CatalogStatisticsProvider(owner).getTableStatistics(new QualifiedTableName("bound", "db", "empty")).rowCount()).isZero();
        }
    }

    @Test
    void realQuotedDotSourcesAndStatisticsCacheCannotCollide() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            fixture.sdk.createDatabase("a.b", false);
            fixture.sdk.createDatabase("a", false);
            ReadBindingSdkFixture.write(fixture.create("a.b", "rows"), 2);
            ReadBindingSdkFixture.write(fixture.create("a", "b.rows"), 7);
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            var firstName = new QualifiedTableName("bound", "a.b", "rows");
            var secondName = new QualifiedTableName("bound", "a", "b.rows");
            assertThat(firstName.toString()).isEqualTo(secondName.toString());
            var first = owner.resolve(firstName.catalog(), firstName.schema(), firstName.table()).orElseThrow();
            var second = owner.resolve(secondName.catalog(), secondName.schema(), secondName.table()).orElseThrow();
            var statistics = new org.pangrova.planner.stats.StatisticsCollector(new CatalogStatisticsProvider(owner));
            assertThat(statistics.getStatistics(firstName).orElseThrow().columns().get("id").rowCount()).isEqualTo(2);
            assertThat(statistics.getStatistics(secondName).orElseThrow().columns().get("id").rowCount()).isEqualTo(7);
            var schema = fixture.bootstrap.buildCalciteSchema(fixture.session, owner);
            var firstPlan = new org.pangrova.planner.PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM bound.\"a.b\".rows").toPlannedQuery();
            var secondPlan = new org.pangrova.planner.PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM bound.a.\"b.rows\"").toPlannedQuery();
            assertThat(firstPlan.splitSources()).singleElement().satisfies(source -> assertThat(source.readBinding()).contains(first));
            assertThat(secondPlan.splitSources()).singleElement().satisfies(source -> assertThat(source.readBinding()).contains(second));
            statistics.invalidate(firstName);
            assertThat(statistics.getStatistics(secondName).orElseThrow().tableName()).isEqualTo(secondName);
        }
    }

    @Test
    void persistedAnalyzeStatisticsWinOnlyWhileTheyDescribeTheCurrentVersion() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            ReadBindingSdkFixture.write(fixture.create("analyzed"), 3);
            var owner = new PlanningReadBindings(
                    fixture.bootstrap.catalogRegistry(), fixture.session);

            var controlStore = new org.pangrova.metadata.control.InMemoryTableControlStore();
            var control = new org.pangrova.metadata.control.TableControlService(controlStore,
                    new org.pangrova.metadata.control.TableControlSettings(
                            true, true, java.util.Set.of()));
            var statisticsService =
                    new org.pangrova.metadata.control.TableControlStatisticsService(control,
                            new org.pangrova.metadata.control
                                    .InMemoryTableControlStatisticsStore());
            var record = org.pangrova.metadata.control.TableControlRecord.builder()
                    .tableId(java.util.UUID.randomUUID())
                    .name("bound", "db", "analyzed")
                    .format("PAIMON")
                    .formatTableUuid("uuid-analyzed")
                    .ownershipMode(org.pangrova.metadata.control.OwnershipMode.MANAGED)
                    .state(org.pangrova.metadata.control.TableControlState.ACTIVE)
                    .provenance(org.pangrova.metadata.control.TableControlProvenance.CREATED)
                    .metadataRevision(1)
                    .version("PAIMON:uuid-analyzed:1", java.time.Instant.now())
                    .createdAt(java.time.Instant.now())
                    .updatedAt(java.time.Instant.now())
                    .build();
            controlStore.save(record);
            statisticsService.record("bound", "db", "analyzed",
                    new org.pangrova.spi.connector.metadata.table.TableStatistics(999, 123_456),
                    "ANALYZE");

            var name = new QualifiedTableName("bound", "db", "analyzed");
            assertThat(new CatalogStatisticsProvider(owner, statisticsService)
                    .getTableStatistics(name).rowCount()).isEqualTo(999);

            controlStore.save(record.toBuilder()
                    .version("PAIMON:uuid-analyzed:2", java.time.Instant.now())
                    .metadataRevision(2)
                    .build());
            assertThat(new CatalogStatisticsProvider(owner, statisticsService)
                    .getTableStatistics(name).rowCount()).isEqualTo(3);
        }
    }

    @Test
    void currentCaseInsensitivePlannerRetainsActualCaseInHistoricalSourceIdentity() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("CamelRows");
            ReadBindingSdkFixture.write(table, 2);
            long version = table.snapshotManager().latestSnapshotId();
            ReadBindingSdkFixture.write(table, 3);
            var selected = PlanningSnapshotSelectors.bind(fixture.session,
                    Map.of(PlanningSnapshotSelectors.tableKey("bound", "db", "CamelRows"), version));
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), selected);
            var schema = fixture.bootstrap.buildCalciteSchema(selected, owner);
            var binding = owner.resolve("bound", "db", "CamelRows").orElseThrow();
            assertThat(binding.statistics().rowCount()).isEqualTo(2);
            assertThat(binding.snapshotId()).hasValue(version);
            for (String reference : List.of("camelrows", "\"CAMELROWS\"", "\"CamelRows\"")) {
                var plan = new org.pangrova.planner.PangrovaPlanner(schema, List.of("bound", "db"))
                        .planForPolicyApplication("SELECT id FROM " + reference).toPlannedQuery();
                assertThat(plan.splitSources()).singleElement().satisfies(source -> {
                    assertThat(source.tableName()).isEqualTo("CamelRows");
                    assertThat(source.readBinding()).contains(binding);
                });
            }
            assertThat(new CatalogStatisticsProvider(owner).getTableStatistics(
                    new QualifiedTableName("bound", "db", "CamelRows"))).isSameAs(binding.statistics());
        }
    }

    @Test
    void duplicateActualSourcesCannotSilentlyMergeDifferentSnapshots() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("conflict");
            ReadBindingSdkFixture.write(table, 2);
            var first = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "conflict").orElseThrow();
            ReadBindingSdkFixture.write(table, 3);
            var second = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "conflict").orElseThrow();
            assertThat(first.snapshotId()).isNotEqualTo(second.snapshotId());
            var sources = java.util.stream.Stream.of(first, second).map(binding -> new org.pangrova.planner.SplitSourceDescriptor(
                    "bound", "db", "conflict", org.pangrova.spi.connector.metadata.Constraint.EMPTY,
                    binding.declaration().metadata().columns(), java.util.Optional.of(binding))).toList();
            assertThatThrownBy(() -> org.pangrova.coordinator.query.execution.split.SplitFetcher.fetchRawConnectorSplits(
                    sources, fixture.bootstrap.catalogRegistry(), Map.of(), List.of("id"), fixture.context))
                    .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                    .hasMessageContaining("Conflicting read bindings");
        }
    }

    @Test
    void expiredSnapshotFailsThroughTheRealSplitFetcherWithoutRebinding() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("expired");
            ReadBindingSdkFixture.write(table, 2);
            var owner = new PlanningReadBindings(fixture.bootstrap.catalogRegistry(), fixture.session);
            var binding = owner.resolve("bound", "db", "expired").orElseThrow();
            ReadBindingSdkFixture.write(table, 3);
            assertThat(table.fileIO().delete(table.snapshotManager().snapshotPath(binding.snapshotId().orElseThrow()), false)).isTrue();
            assertThatThrownBy(() -> fixture.splits(binding, Map.of()))
                    .isInstanceOf(org.pangrova.spi.connector.base.ConnectorException.class)
                    .hasMessageContaining("unavailable");
            assertThat(owner.resolve("bound", "db", "expired").orElseThrow()).isSameAs(binding);
        }
    }
}
