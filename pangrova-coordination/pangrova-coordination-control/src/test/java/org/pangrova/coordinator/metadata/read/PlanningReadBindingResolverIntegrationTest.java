package org.pangrova.coordinator.metadata.read;

import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.planner.metadata.catalog.MetadataSchemaBuilder;

import org.pangrova.connector.api.enums.ReadSnapshotState;

import org.pangrova.planner.metadata.catalog.MetadataResolvedTable;
import org.pangrova.planner.metadata.catalog.PlanningReadBindingResolver;
import org.pangrova.planner.metadata.catalog.PlanningIncrementalSelectors;
import org.pangrova.planner.metadata.catalog.PlanningSnapshotSelectors;

import org.pangrova.coordinator.metadata.CatalogExecutionBindingFactory;
import org.pangrova.coordinator.metadata.ReadBindingSdkFixture;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.SnapshotRange;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PG/plugin/Calcite/SDK/split chain; does not execute a Native task or claim distributed E2E. */
class PlanningReadBindingResolverIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void calciteStatisticsCostStatisticsAndSplitWireShareOneActualSource() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var sdkTable = fixture.create("rows");
            ReadBindingSdkFixture.write(sdkTable, 3);
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session);
            var schema = new MetadataSchemaBuilder(
                    fixture.bootstrap.metadataService(), fixture.bootstrap.catalogRegistry())
                    .buildRootSchema(fixture.session, owner);
            var table = schema.plus().getSubSchema("bound").getSubSchema("db").getTable("rows");
            assertThat(table).isInstanceOf(MetadataResolvedTable.class);
            var binding = ((MetadataResolvedTable) table).unwrap(ConnectorReadBinding.class);
            assertThat(table.getStatistic().getRowCount()).isEqualTo(3.0);
            assertThat(owner.statistics(new QualifiedTableName("bound", "db", "rows"))).isSameAs(binding.statistics());
            var plan = new org.pangrova.planner.analysis.PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM rows").toPlannedQuery();
            assertThat(plan.splitSources()).singleElement().satisfies(source ->
                    assertThat(source.readBinding().orElseThrow()).isSameAs(binding));
            ReadBindingSdkFixture.write(sdkTable, 5);
            assertThat(table.getStatistic().getRowCount()).isEqualTo(3.0);
            assertThat(owner.statistics(new QualifiedTableName("bound", "db", "rows")).rowCount()).isEqualTo(3);
            assertThat(fixture.splits(binding, Map.of())).isNotEmpty().allSatisfy(split -> {
                try {
                    var wire = DataSplit.parseFrom(CanonicalConnectorSplit.from(split).toByteArray());
                    assertThat(wire.getSourceSnapshot()).isEqualTo(Long.toString(binding.snapshotId().orElseThrow()));
                    assertThat(wire.getCatalog()).isEqualTo(CatalogExecutionBindingFactory.from(
                            fixture.bootstrap.catalogRegistry().getCatalog("bound").getDescriptor()));
                } catch (com.google.protobuf.InvalidProtocolBufferException error) {
                    throw new AssertionError(error);
                }
            });
            assertThat(new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "rows").orElseThrow().statistics().rowCount()).isEqualTo(8);
        }
    }

    @Test
    void controlledReadRejectsAnExternalReplacementBeforeAnyWriteObservesIt() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var original = fixture.create("read_owner");
            ReadBindingSdkFixture.write(original, 1);
            var registry = fixture.bootstrap.catalogRegistry();
            var metadata = registry.getCatalog("bound").getMetadata(fixture.session);
            var handle = metadata.getTableHandle(fixture.session,
                    new org.pangrova.connector.api.metadata.table.SchemaTableName("db", "read_owner"));
            var record = registry.tableControl().registrations().adopt("bound", "db", "read_owner", "reader",
                    metadata.getTableIdentity(fixture.session, handle), metadata.observeVersion(fixture.session, handle));
            assertThat(new PlanningReadBindingResolver(registry, fixture.session).resolve("bound", "db", "read_owner"))
                    .isPresent();
            var pending = new PlanningReadBindingResolver(registry, fixture.session);
            var root = new MetadataSchemaBuilder(fixture.bootstrap.metadataService(), registry)
                    .buildRootSchema(fixture.session, pending).plus();
            var discovered = root.getSubSchema("bound").getSubSchema("db").getTable("read_owner");
            assertThat(discovered).isNotNull();
            fixture.sdk.dropTable(Identifier.create("db", "read_owner"), false);
            ReadBindingSdkFixture.write(fixture.create("read_owner"), 2);
            assertThatThrownBy(() -> discovered.getRowType(new JavaTypeFactoryImpl()))
                    .hasStackTraceContaining("format identity");
            assertThat(registry.tableControl().findById(record.tableId()).orElseThrow().state())
                    .isEqualTo(org.pangrova.table.api.enums.TableControlState.QUARANTINED);
        }
    }

    @Test
    void cachedBindingCannotInheritAnotherAdoptionOfTheSamePhysicalTable() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            ReadBindingSdkFixture.write(fixture.create("cached_owner"), 1);
            var registry = fixture.bootstrap.catalogRegistry();
            var metadata = registry.getCatalog("bound").getMetadata(fixture.session);
            var handle = metadata.getTableHandle(fixture.session,
                    new org.pangrova.connector.api.metadata.table.SchemaTableName("db", "cached_owner"));
            var identity = metadata.getTableIdentity(fixture.session, handle);
            var version = metadata.observeVersion(fixture.session, handle);
            var observed = new PlanningReadBindingResolver(registry, fixture.session);
            observed.resolve("bound", "db", "cached_owner").orElseThrow();
            var original = registry.tableControl().registrations().adopt("bound", "db", "cached_owner", "reader", identity, version);
            assertThatThrownBy(() -> observed.resolve("bound", "db", "cached_owner"))
                    .hasMessageContaining("ownership changed");
            var controlled = new PlanningReadBindingResolver(registry, fixture.session);
            controlled.resolve("bound", "db", "cached_owner").orElseThrow();
            var schema = new MetadataSchemaBuilder(fixture.bootstrap.metadataService(), registry)
                    .buildRootSchema(fixture.session, controlled).plus();
            var calciteTable = (MetadataResolvedTable) schema.getSubSchema("bound").getSubSchema("db")
                    .getTable("cached_owner");
            var types = new JavaTypeFactoryImpl();
            var selectedType = calciteTable.getRowType(types);
            long selectedUnits = calciteTable.unwrap(ConnectorReadBinding.class).statistics().sourceWorkUnits();
            assertThat(calciteTable.getStatistic().getRowCount()).isEqualTo(1.0);
            registry.tableControl().registrations().unlink("bound", "db", "cached_owner");
            var replacement = registry.tableControl().registrations().adopt("bound", "db", "cached_owner", "reader", identity, version);
            assertThat(replacement.tableId()).isNotEqualTo(original.tableId());
            assertThat(replacement.formatTableUuid()).isEqualTo(original.formatTableUuid());
            assertThatThrownBy(() -> controlled.resolve("bound", "db", "cached_owner"))
                    .hasMessageContaining("ownership or namespace changed");
            assertThat(calciteTable.getStatistic().getRowCount()).isEqualTo(1.0);
            assertThat(calciteTable.getRowType(types)).isEqualTo(selectedType);
            assertThat(calciteTable.selectedSourceWorkUnits()).isEqualTo(selectedUnits);
            assertThatThrownBy(() -> calciteTable.unwrap(ConnectorReadBinding.class))
                    .hasMessageContaining("ownership or namespace changed");
            assertThat(registry.tableControl().findById(replacement.tableId()).orElseThrow().state())
                    .isEqualTo(org.pangrova.table.api.enums.TableControlState.ACTIVE);
        }
    }

    @Test
    void cachedControlledSnapshotSurvivesDataAdvanceButNotPendingRename() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("cached_data");
            ReadBindingSdkFixture.write(table, 1);
            var registry = fixture.bootstrap.catalogRegistry();
            var metadata = registry.getCatalog("bound").getMetadata(fixture.session);
            var handle = metadata.getTableHandle(fixture.session,
                    new org.pangrova.connector.api.metadata.table.SchemaTableName("db", "cached_data"));
            var original = registry.tableControl().registrations().adopt("bound", "db", "cached_data", "reader",
                    metadata.getTableIdentity(fixture.session, handle), metadata.observeVersion(fixture.session, handle));
            var owner = new PlanningReadBindingResolver(registry, fixture.session);
            var selected = owner.resolve("bound", "db", "cached_data").orElseThrow();
            ReadBindingSdkFixture.write(table, 2);
            registry.tableControl().observations().observeVersion(original, metadata.getTableIdentity(fixture.session, handle),
                    metadata.observeVersion(fixture.session, handle), "EXTERNAL", java.time.Instant.now());
            assertThat(owner.resolve("bound", "db", "cached_data")).containsSame(selected);
            assertThat(selected.statistics().rowCount()).isEqualTo(1);
            registry.tableControl().renames().begin(registry.tableControl().findById(original.tableId()).orElseThrow(),
                    "db", "cached_data_renamed");
            assertThatThrownBy(() -> owner.resolve("bound", "db", "cached_data"))
                    .hasMessageContaining("ownership or namespace changed");
            assertThat(registry.tableControl().findById(original.tableId()).orElseThrow().state())
                    .isEqualTo(org.pangrova.table.api.enums.TableControlState.RENAMING);
        }
    }

    @Test
    void concurrentLookupsShareActualBindingAndDifferentTablesDoNotSharePins() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            ReadBindingSdkFixture.write(fixture.create("first"), 3);
            ReadBindingSdkFixture.write(fixture.create("second"), 7);
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session);
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
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), selectedSession);
            var schema = new MetadataSchemaBuilder(
                    fixture.bootstrap.metadataService(), fixture.bootstrap.catalogRegistry())
                    .buildRootSchema(selectedSession, owner);
            var declared = schema.plus().getSubSchema("bound").getSubSchema("db").getTable("history");
            assertThat(declared.getRowType(new JavaTypeFactoryImpl()).getFieldNames()).containsExactly("id");
            var currentOwner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session);
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
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session);
            var binding = owner.resolve("bound", "db", "empty").orElseThrow();
            assertThat(binding.state()).isEqualTo(ReadSnapshotState.EMPTY);
            ReadBindingSdkFixture.write(table, 2);
            assertThat(fixture.splits(binding, Map.of())).isEmpty();
            assertThat(owner.statistics(new QualifiedTableName("bound", "db", "empty")).rowCount()).isZero();
        }
    }

    @Test
    void realQuotedDotSourcesAndStatisticsCacheCannotCollide() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            fixture.sdk.createDatabase("a.b", false);
            fixture.sdk.createDatabase("a", false);
            ReadBindingSdkFixture.write(fixture.create("a.b", "rows"), 2);
            ReadBindingSdkFixture.write(fixture.create("a", "b.rows"), 7);
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session);
            var firstName = new QualifiedTableName("bound", "a.b", "rows");
            var secondName = new QualifiedTableName("bound", "a", "b.rows");
            assertThat(firstName.toString()).isEqualTo(secondName.toString());
            var first = owner.resolve(firstName.catalog(), firstName.schema(), firstName.table()).orElseThrow();
            var second = owner.resolve(secondName.catalog(), secondName.schema(), secondName.table()).orElseThrow();
            var statistics = new org.pangrova.planner.statistics.StatisticsCollector(owner::statistics);
            assertThat(statistics.getStatistics(firstName).orElseThrow().columns().get("id").rowCount()).isEqualTo(2);
            assertThat(statistics.getStatistics(secondName).orElseThrow().columns().get("id").rowCount()).isEqualTo(7);
            var schema = new MetadataSchemaBuilder(
                    fixture.bootstrap.metadataService(), fixture.bootstrap.catalogRegistry())
                    .buildRootSchema(fixture.session, owner);
            var firstPlan = new org.pangrova.planner.analysis.PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM bound.\"a.b\".rows").toPlannedQuery();
            var secondPlan = new org.pangrova.planner.analysis.PangrovaPlanner(schema, List.of("bound", "db"))
                    .planForPolicyApplication("SELECT id FROM bound.a.\"b.rows\"").toPlannedQuery();
            assertThat(firstPlan.splitSources()).singleElement().satisfies(source -> assertThat(source.readBinding()).contains(first));
            assertThat(secondPlan.splitSources()).singleElement().satisfies(source -> assertThat(source.readBinding()).contains(second));
            statistics.invalidate(firstName);
            assertThat(statistics.getStatistics(secondName).orElseThrow().tableName()).isEqualTo(secondName);
        }
    }

    @Test
    void persistedAnalyzeStatisticsFollowTheExactAuthorizedSourceAndLoadOnce() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var sdkTable = fixture.create("analyzed");
            ReadBindingSdkFixture.write(sdkTable, 3);
            long oldSnapshot = sdkTable.snapshotManager().latestSnapshotId();
            var registry = fixture.bootstrap.catalogRegistry();
            var metadata = registry.getCatalog("bound").getMetadata(fixture.session);
            var handle = metadata.getTableHandle(fixture.session, new SchemaTableName("db", "analyzed"));
            var version = metadata.observeVersion(fixture.session, handle).orElseThrow();
            var record = registry.tableControl().registrations().adopt("bound", "db", "analyzed", "reader",
                    metadata.getTableIdentity(fixture.session, handle), java.util.Optional.of(version));
            var store = registry.tableStatistics();
            store.record("bound", "db", "analyzed", new TableStatistics(999, 123_456), "ANALYZE");
            var lookups = new AtomicInteger();
            org.pangrova.table.api.statistics.CurrentTableStatistics persisted = captured -> {
                lookups.incrementAndGet();
                assertThat(captured.tableId()).isEqualTo(record.tableId());
                return store.fresh(captured);
            };
            var name = new QualifiedTableName("bound", "db", "analyzed");
            var original = new PlanningReadBindingResolver(registry, fixture.session, persisted);
            assertThat(original.resolve("bound", "db", "analyzed").orElseThrow().statisticsVersion()).contains(version);

            var executor = Executors.newFixedThreadPool(4);
            try {
                var lookupsInParallel = java.util.stream.IntStream.range(0, 8)
                        .mapToObj(index -> executor.submit(() -> original.statistics(name))).toList();
                for (var lookup : lookupsInParallel) {
                    assertThat(lookup.get(5, TimeUnit.SECONDS).rowCount()).isEqualTo(999);
                }
            } finally {
                executor.shutdownNow();
            }
            assertThat(lookups).hasValue(1);

            ReadBindingSdkFixture.write(sdkTable, 5);
            long newSnapshot = sdkTable.snapshotManager().latestSnapshotId();
            var advanced = new PlanningReadBindingResolver(registry, fixture.session, persisted);
            assertThat(advanced.statistics(name).rowCount()).isEqualTo(8);
            assertThat(original.statistics(name).rowCount()).isEqualTo(999);
            assertThat(lookups).hasValue(1);

            var currentVersion = metadata.observeVersion(fixture.session, handle).orElseThrow();
            store.record("bound", "db", "analyzed", new TableStatistics(1234, 123_456), "ANALYZE",
                    currentVersion.token());
            var selector = PlanningSnapshotSelectors.tableKey("bound", "db", "analyzed");
            var historicalSession = PlanningSnapshotSelectors.bind(fixture.session, Map.of(selector, oldSnapshot));
            var historical = new PlanningReadBindingResolver(registry, historicalSession, persisted);
            assertThat(historical.statistics(name).rowCount()).isEqualTo(3);

            var incrementalSession = PlanningIncrementalSelectors.bind(fixture.session,
                    Map.of(selector, new SnapshotRange(oldSnapshot, newSnapshot)));
            var incremental = new PlanningReadBindingResolver(registry, incrementalSession, persisted);
            assertThat(incremental.statistics(name)).isEqualTo(TableStatistics.UNKNOWN);
            assertThat(incremental.statistics(name)).isEqualTo(TableStatistics.UNKNOWN);
            assertThat(lookups).hasValue(1);
            assertThat(new PlanningReadBindingResolver(registry, fixture.session, persisted)
                    .statistics(name).rowCount()).isEqualTo(1234);
            assertThat(lookups).hasValue(2);
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
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), selected);
            var schema = new MetadataSchemaBuilder(
                    fixture.bootstrap.metadataService(), fixture.bootstrap.catalogRegistry())
                    .buildRootSchema(selected, owner);
            var binding = owner.resolve("bound", "db", "CamelRows").orElseThrow();
            assertThat(binding.statistics().rowCount()).isEqualTo(2);
            assertThat(binding.snapshotId()).hasValue(version);
            for (String reference : List.of("camelrows", "\"CAMELROWS\"", "\"CamelRows\"")) {
                var plan = new org.pangrova.planner.analysis.PangrovaPlanner(schema, List.of("bound", "db"))
                        .planForPolicyApplication("SELECT id FROM " + reference).toPlannedQuery();
                assertThat(plan.splitSources()).singleElement().satisfies(source -> {
                    assertThat(source.tableName()).isEqualTo("CamelRows");
                    assertThat(source.readBinding()).contains(binding);
                });
            }
            assertThat(owner.statistics(
                    new QualifiedTableName("bound", "db", "CamelRows"))).isSameAs(binding.statistics());
        }
    }

    @Test
    void duplicateActualSourcesCannotSilentlyMergeDifferentSnapshots() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("conflict");
            ReadBindingSdkFixture.write(table, 2);
            var first = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "conflict").orElseThrow();
            ReadBindingSdkFixture.write(table, 3);
            var second = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session)
                    .resolve("bound", "db", "conflict").orElseThrow();
            assertThat(first.snapshotId()).isNotEqualTo(second.snapshotId());
            var sources = java.util.stream.Stream.of(first, second).map(binding -> new org.pangrova.planner.analysis.SplitSourceDescriptor(
                    "bound", "db", "conflict", org.pangrova.connector.api.metadata.Constraint.EMPTY,
                    binding.declaration().metadata().columns(), java.util.Optional.of(binding))).toList();
            assertThatThrownBy(() -> org.pangrova.coordinator.query.execution.split.SplitFetcher.fetchRawConnectorSplits(
                    sources, fixture.bootstrap.catalogRegistry(), Map.of(), List.of("id"), fixture.context,
                    org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity.WHOLE_OBJECT))
                    .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                    .hasMessageContaining("Conflicting read bindings");
        }
    }

    @Test
    void expiredSnapshotFailsThroughTheRealSplitFetcherWithoutRebinding() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("expired");
            ReadBindingSdkFixture.write(table, 2);
            var owner = new PlanningReadBindingResolver(fixture.bootstrap.catalogRegistry(), fixture.session);
            var binding = owner.resolve("bound", "db", "expired").orElseThrow();
            ReadBindingSdkFixture.write(table, 3);
            assertThat(table.fileIO().delete(table.snapshotManager().snapshotPath(binding.snapshotId().orElseThrow()), false)).isTrue();
            assertThatThrownBy(() -> fixture.splits(binding, Map.of()))
                    .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class)
                    .hasMessageContaining("unavailable");
            assertThat(owner.resolve("bound", "db", "expired").orElseThrow()).isSameAs(binding);
        }
    }
}
