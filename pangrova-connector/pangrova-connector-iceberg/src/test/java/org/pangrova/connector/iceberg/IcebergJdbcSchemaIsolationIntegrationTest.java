package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.enums.SnapshotRelation;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK initialization must inspect only the connection's exact catalog and schema. */
class IcebergJdbcSchemaIsolationIntegrationTest extends ContainerTestBase {
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final TableIdentifier TABLE = TableIdentifier.of("data", "events");
    private static final ConnectorSession READER = new ConnectorSession() {
        @Override public String getUser() { return "reader"; }
        @Override public Optional<String> getCatalog() { return Optional.empty(); }
        @Override public Optional<String> getSchema() { return Optional.of("data"); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "jdbc-budget"); }
        @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return Optional.empty();
        }
    };

    @TempDir
    Path warehouse;

    @Test
    void currentIndexFilesUseOneCatalogReadPerObservation() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var table = selected.loadTable(TABLE);
            var metadata = new IcebergMetadata(selected, schema.name());
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            long emptyBefore = schema.catalogPointerLookups();
            assertThat(metadata.listDataFiles(schema.name(), "data", "events")).isEmpty();
            assertThat(schema.catalogPointerLookups() - emptyBefore).isEqualTo(1);

            String first = table.location() + "/data/first.parquet";
            table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(first).withFileSizeInBytes(128).withRecordCount(7).build()).commit();
            var pin = metadata.getSnapshotAwareMetadata("data", "events").orElseThrow();
            String originalSnapshot = pin.currentSnapshotId().orElseThrow();
            for (int sample = 0; sample < 3; sample++) {
                long before = schema.catalogPointerLookups();
                assertThat(metadata.listDataFiles(schema.name(), "data", "events")).containsExactly(first);
                long selects = schema.catalogPointerLookups() - before;
                assertThat(selects).isEqualTo(1);
                System.out.printf("event=index_snapshot_files sample=%d pointer_selects=%d files=1%n",
                        sample, selects);
            }
            String second = table.location() + "/data/second.parquet";
            table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(second).withFileSizeInBytes(64).withRecordCount(3).build()).commit();
            assertThat(metadata.listDataFiles(schema.name(), "data", "events")).containsExactly(first, second);
            assertThat(pin.listFilesAtSnapshot(originalSnapshot)).extracting(file -> file.path()).containsExactly(first);

            String appended = pin.currentSnapshotId().orElseThrow();
            for (int sample = 0; sample < 3; sample++) {
                long baselineStart = schema.catalogPointerLookups();
                var relation = pin.relation(originalSnapshot, appended);
                var expectedFiles = pin.listFilesAtSnapshot(appended);
                var expectedDelta = pin.diffFiles(originalSnapshot, appended);
                long baselineSelects = schema.catalogPointerLookups() - baselineStart;

                long candidateStart = schema.catalogPointerLookups();
                var observed = pin.observeAdvance(originalSnapshot, appended);
                long candidateSelects = schema.catalogPointerLookups() - candidateStart;

                assertThat(relation).isEqualTo(SnapshotRelation.LEFT_ANCESTOR);
                assertThat(observed.relation()).isEqualTo(relation);
                assertThat(observed.targetFiles()).isEqualTo(expectedFiles);
                assertThat(observed.delta()).isEqualTo(expectedDelta);
                assertThat(observed.delta().added()).extracting(file -> file.path()).containsExactly(second);
                assertThat(observed.delta().removed()).isEmpty();
                assertThat(baselineSelects).isEqualTo(3);
                assertThat(candidateSelects).isOne();
                System.out.printf("event=index_snapshot_advance sample=%d baseline_pointer_selects=%d "
                        + "fused_pointer_selects=%d target_files=%d%n",
                        sample, baselineSelects, candidateSelects, observed.targetFiles().size());
            }
            long predecessorStart = schema.catalogPointerLookups();
            var predecessor = pin.observeAdvance(appended, originalSnapshot);
            assertThat(schema.catalogPointerLookups() - predecessorStart).isOne();
            assertThat(predecessor.relation()).isEqualTo(SnapshotRelation.RIGHT_ANCESTOR);
            assertThat(predecessor.targetFiles()).isEmpty();
        }
    }

    @Test
    void postgresConnectorUsesTheExactSpellingWithoutListingTheNamespace() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            selected.createNamespace(Namespace.of("data"));
            selected.createTable(TableIdentifier.of("data", "CamelEvents"), TABLE_SCHEMA);
            var connector = new IcebergConnectorFactory().create(schema.name(), jdbcProperties(schema, "V1"), null);
            try {
                var metadata = connector.getMetadata(READER);
                assertThat(metadata.supportsExactTableNameLookup()).isTrue();
                schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
                long listings = schema.namespaceListings();
                long lookups = schema.catalogPointerLookups();

                assertThat(metadata.getTableHandle(READER, new SchemaTableName("data", "CamelEvents")))
                        .isInstanceOf(IcebergTableHandle.class);
                assertThat(schema.catalogPointerLookups() - lookups).isEqualTo(1);
                assertThat(metadata.getTableHandle(READER, new SchemaTableName("data", "camelevents"))).isNull();
                assertThat(schema.namespaceListings()).isEqualTo(listings);

                assertThat(metadata.listTables(READER, Optional.of("data")))
                        .containsExactly(new SchemaTableName("data", "CamelEvents"));
                assertThat(schema.namespaceListings()).isGreaterThan(listings);
            } finally {
                connector.shutdown();
            }
        }
    }

    @Test
    void namedReferenceDiscoveryUsesRequestedKindAndOneMetadataObservation() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            verifyNamedReferenceDiscovery(schema, selected);
        }
    }

    private static void verifyNamedReferenceDiscovery(DatabaseSchema schema, JdbcCatalog selected) throws SQLException {
        createTable(selected);
        var table = selected.loadTable(TABLE);
        table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(table.location() + "/data/first.parquet").withFileSizeInBytes(128)
                .withRecordCount(1).build()).commit();
        long original = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", original).createTag("release", original).commit();

        var metadata = new IcebergMetadata(selected, schema.name());
        schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
        long before = schema.catalogPointerLookups();
        var discovery = metadata.discoverRead(READER, new SchemaTableName("data", "events"));
        assertThat(discovery).isNotNull();
        assertThat(discovery.referenceSnapshot().apply("BRANCH", "feature")).hasValue(original);
        assertThat(discovery.referenceSnapshot().apply("TAG", "release")).hasValue(original);
        assertThat(discovery.referenceSnapshot().apply("BRANCH", "release")).isEmpty();
        assertThat(discovery.referenceSnapshot().apply("TAG", "feature")).isEmpty();
        assertThat(discovery.referenceSnapshot().apply("TAG", "missing")).isEmpty();
        assertThat(schema.catalogPointerLookups() - before).isOne();

        table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(table.location() + "/data/second.parquet").withFileSizeInBytes(128)
                .withRecordCount(1).build()).commit();
        table.manageSnapshots().replaceBranch("feature", table.currentSnapshot().snapshotId()).commit();
        assertThat(discovery.referenceSnapshot().apply("BRANCH", "feature")).hasValue(original);
        var binding = discovery.binder().bind(READER,
                discovery.handle().withTimeTravelSnapshot(Long.toString(original)), null);
        assertThat(binding.snapshotId()).hasValue(original);

        var manager = new IcebergSplitManager(schema.name(), selected, new IcebergExpressionToSubstrait());
        long beforeSplits = schema.catalogPointerLookups();
        var source = manager.getBoundSplits(ConnectorTransactionHandle.empty(),
                READER, metadata, binding, DynamicFilter.ALL, Constraint.EMPTY, SplitPlanningBudget.standard());
        try {
            assertThat(source.getNextBatch(10)).hasSize(1);
            assertThat(source.isFinished()).isTrue();
        } finally {
            source.close();
        }
        assertThat(schema.catalogPointerLookups() - beforeSplits).isOne();

        table.updateSchema().addColumn("added_after_snapshot", Types.StringType.get()).commit();
        long beforeEvolution = schema.catalogPointerLookups();
        var evolved = java.util.Objects.requireNonNull(metadata.discoverRead(READER, new SchemaTableName("data", "events")));
        var historical = evolved.binder().bind(READER,
                evolved.handle().withTimeTravelSnapshot(Long.toString(original)), null);
        assertThat(((IcebergTableHandle) evolved.handle()).schemaJson()).contains("added_after_snapshot");
        assertThat(((IcebergTableHandle) historical.handle()).schemaJson()).doesNotContain("added_after_snapshot");
        assertThat(historical.snapshotId()).hasValue(original);
        assertThat(schema.catalogPointerLookups() - beforeEvolution).isOne();
    }

    @Test
    void jdbcNamedSchemaEvidenceRespectsSpellingAndFirstLevelProjection() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            var metadata = new IcebergMetadata(selected, schema.name());
            selected.createNamespace(Namespace.of("Exact"));
            selected.createNamespace(Namespace.of("exact"));
            selected.createNamespace(Namespace.of("parent", "child"));
            selected.createNamespace(Namespace.of("axby", "child"));

            assertThat(metadata.hasExactSchemaName(READER, "Exact")).isTrue();
            assertThat(metadata.hasExactSchemaName(READER, "exact")).isTrue();
            assertThat(metadata.hasExactSchemaName(READER, "EXACT")).isFalse();
            assertThat(metadata.hasExactSchemaName(READER, "parent")).isTrue();
            assertThat(metadata.hasExactSchemaName(READER, "parent.child")).isFalse();
            assertThat(metadata.hasExactSchemaName(READER, "a_by")).isFalse();
            assertThat(metadata.hasExactSchemaName(READER, "a%by")).isFalse();
            assertThat(metadata.listSchemaNames(READER)).contains("Exact", "exact", "parent", "axby");

            assertThat(selected.dropNamespace(Namespace.of("exact"))).isTrue();
            assertThat(metadata.hasExactSchemaName(READER, "exact")).isFalse();
            assertThat(metadata.hasExactSchemaName(READER, "Exact")).isTrue();
        }
    }

    @Test
    void verifiesTheBoundSnapshotAndEnumeratesWithOneCatalogPointerRead() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var table = selected.loadTable(TABLE);
            table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(table.location() + "/data/bound.parquet").withFileSizeInBytes(128)
                    .withRecordCount(1).build()).commit();
            var metadata = new IcebergMetadata(selected, "bound");
            var name = new SchemaTableName("data", "events");
            var binding = metadata.bindRead(READER, metadata.getTableHandle(READER, name));
            var manager = new IcebergSplitManager("bound", selected, new IcebergExpressionToSubstrait());
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            long before = schema.catalogPointerLookups();

            var source = manager.getBoundSplits(ConnectorTransactionHandle.empty(),
                    READER, metadata, binding, DynamicFilter.ALL, Constraint.EMPTY, SplitPlanningBudget.standard());
            try {
                assertThat(source.getNextBatch(10)).hasSize(1);
                assertThat(source.isFinished()).isTrue();
            } finally {
                source.close();
            }
            assertThat(schema.catalogPointerLookups() - before).isEqualTo(1);
        }
    }

    @Test
    void namedDeclarationUsesOneCurrentJdbcPointerAndPreservesTheActualSchema() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var metadata = new IcebergMetadata(selected, schema.name());
            var name = new SchemaTableName("data", "events");
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");

            long baseline = schema.catalogPointerLookups();
            var handle = metadata.getTableHandle(READER, name);
            var previous = metadata.getTableMetadata(READER, handle);
            assertThat(schema.catalogPointerLookups() - baseline).isEqualTo(2);

            long first = schema.catalogPointerLookups();
            assertThat(metadata.describeNamedTable(READER, name)).contains(previous);
            assertThat(schema.catalogPointerLookups() - first).isEqualTo(1);

            try (var writer = catalog(schema, "V1")) {
                writer.loadTable(TABLE).updateSchema().addColumn("new_field", Types.LongType.get()).commit();
            }
            long second = schema.catalogPointerLookups();
            assertThat(metadata.describeNamedTable(READER, name)).hasValueSatisfying(declaration ->
                    assertThat(declaration.columns()).containsExactly("id", "new_field"));
            assertThat(schema.catalogPointerLookups() - second).isEqualTo(1);
        }
    }

    @Test
    void freshJdbcTableResolutionChecksCurrentMetadataPointerOnce() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var tables = new IcebergCatalogTables(selected);
            var first = (IcebergTableHandle) tables.getTableHandle(new SchemaTableName("data", "events"));
            try (var writer = catalog(schema, "V1")) {
                writer.loadTable(TABLE).updateSchema().addColumn("new_field", Types.LongType.get()).commit();
            }
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            long before = schema.catalogPointerLookups();

            assertThat(tables.getTableReadMetadata(first).metadata().columns()).contains("new_field");

            assertThat(schema.catalogPointerLookups() - before).isEqualTo(1);
        }
    }

    @Test
    void controlledReadChecksTheIdentityAndBindsTheSnapshotInOneCatalogLoad() throws Exception {
        try (var schema = DatabaseSchema.create(); var catalog = catalog(schema, "V1")) {
            createTable(catalog);
            var metadata = new IcebergMetadata(catalog, schema.name());
            var name = new SchemaTableName("data", "events");
            var handle = metadata.getTableHandle(READER, name);
            var expected = metadata.getTableIdentity(READER, handle);
            assertThat(expected).isPresent();
            var observed = new AtomicReference<Optional<TableIdentity>>();
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");

            long before = schema.catalogPointerLookups();
            var binding = metadata.bindReadWithIdentityCheck(READER, handle, handle,
                    identity -> assertThat(observed.compareAndSet(null, identity)).isTrue());
            assertThat(schema.catalogPointerLookups() - before).isOne();
            assertThat(observed.get()).isEqualTo(expected);
            assertThat(binding.schemaIdentity()).isPresent();

            long beforeRejected = schema.catalogPointerLookups();
            assertThatThrownBy(() -> metadata.bindReadWithIdentityCheck(READER, handle, handle, identity -> {
                throw new IllegalStateException("reject observed table");
            })).isInstanceOf(IllegalStateException.class).hasMessage("reject observed table");
            assertThat(schema.catalogPointerLookups() - beforeRejected).isOne();
        }
    }

    @Test
    void controlledReadNeverBindsAReplacementDuringTheIdentityCheck() throws Exception {
        try (var schema = DatabaseSchema.create(); var catalog = catalog(schema, "V1")) {
            createTable(catalog);
            var metadata = new IcebergMetadata(catalog, schema.name());
            var name = new SchemaTableName("data", "events");
            var handle = metadata.getTableHandle(READER, name);
            var original = metadata.getTableIdentity(READER, handle).orElseThrow();

            var binding = metadata.bindReadWithIdentityCheck(READER, handle, handle, observed -> {
                assertThat(observed).contains(original);
                assertThat(catalog.dropTable(TABLE, false)).isTrue();
                catalog.buildTable(TABLE, TABLE_SCHEMA).withLocation(warehouse.resolve("replacement").toString())
                        .create();
            });

            assertThat(binding.schemaIdentity()).hasValueSatisfying(identity ->
                    assertThat(identity.tableIdentity()).isEqualTo(original));
            assertThatThrownBy(() -> metadata.validateReadBinding(READER, binding))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Selected table or snapshot identity changed");
        }
    }

    @Test
    void discoveryAndControlledBindingReadTheCatalogPointerOnce() throws Exception {
        try (var schema = DatabaseSchema.create(); var catalog = catalog(schema, "V1")) {
            createTable(catalog);
            var metadata = new IcebergMetadata(catalog, schema.name());
            var name = new SchemaTableName("data", "events");
            var expected = metadata.getTableIdentity(READER, metadata.getTableHandle(READER, name));
            assertThat(expected).isPresent();
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            long before = schema.catalogPointerLookups();

            var discovered = java.util.Objects.requireNonNull(metadata.discoverRead(READER, name));
            var discoveredHandle = (IcebergTableHandle) discovered.handle();
            var binding = discovered.binder().bind(READER, discoveredHandle,
                    observed -> assertThat(observed).isEqualTo(expected));
            var boundHandle = (IcebergTableHandle) binding.handle();

            assertThat(boundHandle.schemaJson()).isSameAs(discoveredHandle.schemaJson());
            assertThat(boundHandle.partitionSpecJson()).isSameAs(discoveredHandle.partitionSpecJson());
            assertThat(schema.catalogPointerLookups() - before).isOne();
            assertThat(binding.schemaIdentity()).hasValueSatisfying(identity ->
                    assertThat(identity.tableIdentity()).isEqualTo(expected.orElseThrow()));
        }
    }

    @Test
    void readDiscoveryRetainsIdentityAndCannotExecuteAgainstAReplacement() throws Exception {
        try (var schema = DatabaseSchema.create();
                var reader = catalog(schema, "V1"); var writer = catalog(schema, "V1")) {
            createTable(reader);
            var metadata = new IcebergMetadata(reader, schema.name());
            var name = new SchemaTableName("data", "events");
            var discovered = java.util.Objects.requireNonNull(metadata.discoverRead(READER, name));
            var original = metadata.getTableIdentity(READER, discovered.handle()).orElseThrow();
            assertThat(writer.dropTable(TABLE, false)).isTrue();
            writer.buildTable(TABLE, TABLE_SCHEMA).withLocation(warehouse.resolve("discovery-replacement").toString())
                    .create();

            var binding = discovered.binder().bind(READER, discovered.handle(),
                    observed -> assertThat(observed).contains(original));

            assertThat(binding.schemaIdentity()).hasValueSatisfying(identity ->
                    assertThat(identity.tableIdentity()).isEqualTo(original));
            assertThatThrownBy(() -> metadata.validateReadBinding(READER, binding))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Selected table or snapshot identity changed");
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    void distributedWriteCatalogPointerBudget(boolean requiresPublication, boolean emptyInsert) throws Exception {
        try (var schema = DatabaseSchema.create(); var catalog = catalog(schema, "V1")) {
            createTable(catalog);
            var metadata = new IcebergMetadata(catalog, schema.name());
            var name = new SchemaTableName("data", "events");
            schema.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            var target = metadata.getTableHandle(READER, name);
            long beforeAllocation = schema.catalogPointerLookups();
            var handle = (IcebergOutputTableHandle) metadata.beginInsert(READER, target);
            assertThat(schema.catalogPointerLookups() - beforeAllocation)
                    .as("exact catalog pointer SELECTs during allocation").isOne();
            var table = catalog.loadTable(TABLE);
            var data = DataFiles.builder(table.spec()).withPath(table.location() + "/data/committed.parquet")
                    .withFileSizeInBytes(128).withRecordCount(1).build();
            var fragments = emptyInsert ? List.<byte[]>of() : List.of(
                    ("[" + ContentFileParser.toJson(data, table.spec()) + "]").getBytes(StandardCharsets.UTF_8));
            var binding = IcebergWriteBindingFixtures.binding(handle);
            var receipt = IcebergWriteReceiptFixtures.receipt();
            var tenant = READER.getProperties().get("tenant_id");
            var fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(
                    "exact-write", tenant, schema.name(), name.toString(), handle.writeMode(),
                    handle.baseSnapshotId(), binding, READER.getUser(), receipt, null), fragments);
            var context = new DistributedWriteCommitContext("exact-write", tenant, handle.baseSnapshotId(),
                    binding, fingerprint, READER.getUser(), receipt, null);
            long before = schema.catalogPointerLookups();

            var publication = requiresPublication
                    ? metadata.finishDistributedWriteWithPublication(READER, handle, fragments, context) : null;
            if (!requiresPublication) metadata.finishDistributedWrite(READER, handle, fragments, context);
            assertThat(schema.catalogPointerLookups() - before).as("exact catalog pointer SELECTs").isEqualTo(6);
            table.refresh();
            assertThat(table.snapshots()).hasSize(1);
            var committed = table.currentSnapshot();
            assertThat(committed.summary()).containsEntry(IcebergDistributedCommitMarker.FINGERPRINT,
                    context.commitFingerprint());
            if (publication != null) {
                assertThat(publication.publishedVersion()).isEqualTo(committed.snapshotId());
                try (var input = table.io().newInputFile(committed.manifestListLocation()).newStream()) {
                    assertThat(publication.manifestSha256()).isEqualTo(
                            java.security.MessageDigest.getInstance("SHA-256").digest(input.readAllBytes()));
                }
                table.newAppend().appendFile(DataFiles.builder(table.spec())
                        .withPath(table.location() + "/data/unrelated.parquet")
                        .withFileSizeInBytes(96).withRecordCount(1).build()).commit();
                assertThat(metadata.finishDistributedWriteWithPublication(READER, handle, fragments, context))
                        .isEqualTo(publication);
                table.refresh();
                assertThat(table.snapshots()).hasSize(2);
                assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(publication.publishedVersion());
            }
        }
    }

    @Test
    void anotherInitializedSchemaDoesNotPreventCreatingCatalogTables() throws Exception {
        try (var first = DatabaseSchema.create(); var second = DatabaseSchema.create();
                var firstCatalog = catalog(first, "V1")) {
            createTable(firstCatalog);
            try (var secondCatalog = catalog(second, "V1")) {
                createTable(secondCatalog);
                assertThat(secondCatalog.loadTable(TABLE).schema().columns()).hasSize(1);
                assertThat(second.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
                assertThat(firstCatalog.loadTable(TABLE).location())
                        .isNotEqualTo(secondCatalog.loadTable(TABLE).location());
            }
        }
    }

    @Test
    void anotherV1SchemaDoesNotPreventUpgradingTheCurrentV0Schema() throws Exception {
        try (var first = DatabaseSchema.create(); var second = DatabaseSchema.create();
                var secondV0 = catalog(second, "V0")) {
            createTable(secondV0);
            assertThat(second.hasColumn("iceberg_tables", "iceberg_type")).isFalse();
            try (var firstV1 = catalog(first, "V1")) {
                createTable(firstV1);
                try (var secondV1 = catalog(second, "V1")) {
                    assertThat(second.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
                    assertThat(secondV1.loadTable(TABLE).schema().columns()).hasSize(1);
                }
            }
        }
    }

    @Test
    void jdbcPatternWildcardsDoNotMatchUnrelatedTablesInTheSameSchema() throws Exception {
        try (var schema = DatabaseSchema.create()) {
            schema.execute("CREATE TABLE icebergXtables (icebergXtype VARCHAR(5))");
            schema.execute("CREATE TABLE icebergXnamespaceXproperties (id BIGINT)");
            try (var selected = catalog(schema, "V1")) {
                createTable(selected);
                assertThat(selected.loadTable(TABLE).schema().columns()).hasSize(1);
                assertThat(schema.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
            }
        }
    }

    @Test
    void jdbcPatternWildcardsDoNotMatchAnUnrelatedColumnDuringUpgrade() throws Exception {
        try (var schema = DatabaseSchema.create()) {
            try (var initial = catalog(schema, "V0")) {
                createTable(initial);
            }
            schema.execute("ALTER TABLE iceberg_tables ADD COLUMN icebergXtype VARCHAR(5)");
            try (var upgraded = catalog(schema, "V1")) {
                assertThat(schema.hasColumn("iceberg_tables", "iceberg_type")).isTrue();
                assertThat(upgraded.loadTable(TABLE).schema().columns()).hasSize(1);
            }
        }
    }

    @Test
    void crossNamespaceRenamePreservesTableUuidAndTheExactMetadataPointer() throws Exception {
        try (var schema = DatabaseSchema.create(); var selected = catalog(schema, "V1")) {
            createTable(selected);
            var before = ((org.apache.iceberg.BaseTable) selected.loadTable(TABLE)).operations().current();
            selected.createNamespace(Namespace.of("renamed_namespace"));
            var target = TableIdentifier.of("renamed_namespace", "renamed_table");
            selected.renameTable(TABLE, target);
            var after = ((org.apache.iceberg.BaseTable) selected.loadTable(target)).operations().current();
            assertThat(selected.tableExists(TABLE)).isFalse();
            assertThat(after.uuid()).isEqualTo(before.uuid());
            assertThat(after.metadataFileLocation()).isEqualTo(before.metadataFileLocation());
        }
    }

    private JdbcCatalog catalog(DatabaseSchema schema, String version) {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        return (JdbcCatalog) CatalogUtil.buildIcebergCatalog(schema.name(), jdbcProperties(schema, version), configuration);
    }

    private Map<String, String> jdbcProperties(DatabaseSchema schema, String version) {
        var postgres = IcebergJdbcTestDatabase.instance();
        return Map.of("type", "jdbc", "uri", schema.uri(),
                "warehouse", warehouse.resolve(schema.name()).toString(),
                "jdbc.user", postgres.getUsername(), "jdbc.password", postgres.getPassword(),
                "jdbc.schema-version", version);
    }

    private static void createTable(JdbcCatalog catalog) {
        catalog.createNamespace(Namespace.of("data"));
        catalog.createTable(TABLE, TABLE_SCHEMA);
    }

    private record DatabaseSchema(String name, String uri, Connection connection) implements AutoCloseable {
        static DatabaseSchema create() throws SQLException {
            var postgres = IcebergJdbcTestDatabase.instance();
            String name = "sdk_schema_" + UUID.randomUUID().toString().replace("-", "");
            String baseUri = postgres.getJdbcUrl();
            String uri = baseUri + (baseUri.contains("?") ? "&" : "?") + "currentSchema=" + name;
            var connection = DriverManager.getConnection(uri, postgres.getUsername(), postgres.getPassword());
            try {
                try (var statement = connection.createStatement()) {
                    statement.execute("CREATE SCHEMA " + name);
                }
                return new DatabaseSchema(name, uri, connection);
            } catch (SQLException failure) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        void execute(String sql) throws SQLException {
            try (var statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }

        boolean hasColumn(String table, String column) throws SQLException {
            try (var statement = connection.prepareStatement("SELECT COUNT(*) FROM information_schema.columns"
                    + " WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
                statement.setString(1, name);
                statement.setString(2, table);
                statement.setString(3, column);
                try (var rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getLong(1) == 1;
                }
            }
        }

        long catalogPointerLookups() throws SQLException {
            try (var statement = connection.createStatement();
                    var rows = statement.executeQuery("""
                            SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                            WHERE query LIKE 'SELECT * FROM iceberg_tables WHERE catalog_name = %'
                              AND query LIKE '%AND table_name = %'
                            """)) {
                rows.next();
                return rows.getLong(1);
            }
        }

        long namespaceListings() throws SQLException {
            try (var statement = connection.createStatement();
                    var rows = statement.executeQuery("""
                            SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                            WHERE query LIKE 'SELECT * FROM iceberg_tables WHERE catalog_name = %'
                              AND query LIKE '%AND table_namespace = %'
                              AND query NOT LIKE '%AND table_name = %'
                            """)) {
                rows.next();
                return rows.getLong(1);
            }
        }

        @Override
        public void close() throws SQLException {
            try (connection) {
                execute("DROP SCHEMA " + name + " CASCADE");
            }
        }
    }
}
