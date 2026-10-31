package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.metadata.service.RegistryTablePolicyAuthority;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.table.store.jdbc.control.JdbcTableControlStore;
import org.pangrova.table.api.control.OwnershipMode;
import org.pangrova.table.core.control.TableControlService;
import org.pangrova.table.core.control.TableControlSettings;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** HTTP SQL, native reads/writes, SDK external writer and durable ownership fences. */
class TableControlLifecycleE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "control_catalog";
    private static final String SCHEMA = "default";
    private static final String PAIMON = "control_paimon";
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()));

    private TableControlService previousControl;
    private JdbcTableControlStore store;
    private HadoopCatalog externalCatalog;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeEach
    void enableDurableEnforcement() {
        var registry = metadataBootstrap.catalogRegistry();
        previousControl = registry.tableControl();
        store = new JdbcTableControlStore(stateStoreDataSource());
        registry.setTableControlService(new TableControlService(store, registry::requireCurrentCatalogIdentity,
                new TableControlSettings(true, true, Set.of())));
        externalCatalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString());
    }

    @AfterEach
    void restoreEnforcement() throws Exception {
        metadataBootstrap.catalogRegistry().setTableControlService(previousControl);
        externalCatalog.close();
    }

    @Test
    void metadataApiCreationPreservesScalarParametersForActualNativeSql() {
        String table = "controlled_metadata_created";
        String source = PAIMON + "." + SCHEMA + "." + table;
        var service = metadataBootstrap.metadataService();
        // The trusted engine API receives the configured test actor's metadata-only context.
        // Writes and result reads below traverse authenticated HTTP SQL and actual Native execution.
        var actor = new org.pangrova.metadata.service.MetadataSessionContext("e2e-user", PAIMON, SCHEMA,
                "UTC", "en", java.util.Map.of(), "default", null, null);
        var ref = new org.pangrova.metadata.api.object.QualifiedTableRef(PAIMON, SCHEMA, table);
        var columns = List.of(
                new org.pangrova.metadata.api.object.ColumnMetadata("id",
                        org.pangrova.metadata.api.object.ColumnType.of("BIGINT"), true, "", java.util.Map.of()),
                new org.pangrova.metadata.api.object.ColumnMetadata("email",
                        org.pangrova.metadata.api.object.ColumnType.varchar(128), true, "", java.util.Map.of()),
                new org.pangrova.metadata.api.object.ColumnMetadata("amount",
                        org.pangrova.metadata.api.object.ColumnType.decimal(12, 2), true, "", java.util.Map.of()));
        service.createTable(actor, ref, new org.pangrova.metadata.api.object.TableMetadata(PAIMON, SCHEMA, table,
                columns, java.util.Map.of(), List.of(), java.util.Map.of(), null, null));
        assertThat(store.find(PAIMON, SCHEMA, table).orElseThrow().ownershipMode()).isEqualTo(OwnershipMode.MANAGED);
        sqlRunner.execute("INSERT INTO " + source + " VALUES (7, 'typed@example.com', 12.34)");
        service.setTag(actor, ref, "EMAIL", "pii_type", "PII_EMAIL");
        assertThat(sqlRunner.execute("SELECT id,email,CAST(amount AS VARCHAR) AS amount_text FROM " + source))
                .singleElement().satisfies(row -> {
                    assertThat(row.getLong("id")).isEqualTo(7L);
                    assertThat(row.getString("email")).isEqualTo("***@***");
                    assertThat(row.getString("amount_text")).isEqualTo("12.34");
                });
        sqlRunner.executeDdl("DROP TABLE " + source);
    }

    @Test
    void stableTagsMaskNativeResultsAcrossRenameAndNameReuse() {
        String source = PAIMON + "." + SCHEMA + ".controlled_private_contacts";
        String renamed = PAIMON + "." + SCHEMA + ".controlled_private_contacts_renamed";
        sqlRunner.executeDdl("CREATE TABLE " + source
                + " (id BIGINT, email VARCHAR, phone VARCHAR, ssn VARCHAR, person VARCHAR)");
        sqlRunner.execute("INSERT INTO " + source + " VALUES "
                + "(1, 'alice@example.com', '1234567890', '123-45-6789', 'Alice'),"
                + "(2, NULL, NULL, NULL, NULL), (3, 'invalid', '123', '123', '')");
        var original = store.find(PAIMON, SCHEMA, "controlled_private_contacts").orElseThrow();
        for (var entry : java.util.Map.of("email", "PII_EMAIL", "phone", "PII_PHONE",
                "ssn", "PII_SSN", "person", "PII_NAME").entrySet()) {
            sqlRunner.executeDdl("ALTER TABLE " + source + " ALTER COLUMN " + entry.getKey().toUpperCase(java.util.Locale.ROOT)
                    + " SET TAG pii_type = '" + entry.getValue() + "'");
        }
        assertThat(sqlRunner.executeExpectingError("ALTER TABLE " + source
                + " ALTER COLUMN id SET TAG pii_type = 'PII_EMAIL'").message()).contains("VARCHAR");
        assertPrivateContacts(source);

        // Recreate the actual query pipeline and its policy readers; no tag event replay is needed.
        clearFeatureSecurity();
        assertPrivateContacts(source);
        sqlRunner.executeDdl("ALTER TABLE " + source + " RENAME TO " + renamed);
        assertThat(store.find(PAIMON, SCHEMA, "controlled_private_contacts_renamed").orElseThrow().tableId())
                .isEqualTo(original.tableId());
        assertPrivateContacts(renamed);

        sqlRunner.executeDdl("CREATE TABLE " + source + " (id BIGINT, email VARCHAR)");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (9, 'replacement@example.com')");
        assertThat(sqlRunner.execute("SELECT email FROM " + source)).singleElement()
                .satisfies(row -> assertThat(row.getString("email")).isEqualTo("replacement@example.com"));
        sqlRunner.executeDdl("ALTER TABLE " + source + " ALTER COLUMN email SET TAG pii_type = 'PII_NAME'");
        for (String column : List.of("email", "phone", "ssn", "person")) {
            sqlRunner.executeDdl("ALTER TABLE " + renamed + " ALTER COLUMN " + column.toUpperCase(java.util.Locale.ROOT) + " UNSET TAG pii_type");
        }
        assertThat(sqlRunner.execute("SELECT email FROM " + renamed + " WHERE id=1")).singleElement()
                .satisfies(row -> assertThat(row.getString("email")).isEqualTo("alice@example.com"));
        sqlRunner.executeDdl("DROP TABLE " + renamed);
        assertThat(sqlRunner.execute("SELECT email FROM " + source)).singleElement()
                .satisfies(row -> assertThat(row.getString("email")).isEqualTo("***"));
        sqlRunner.executeDdl("DROP TABLE " + source);
    }

    @Test
    void rowPoliciesFollowStableIdentityAcrossRenameAndNameReuse() {
        String table = "controlled_private_rows";
        String renamedTable = table + "_renamed";
        String source = PAIMON + "." + SCHEMA + "." + table;
        String renamed = PAIMON + "." + SCHEMA + "." + renamedTable;
        sqlRunner.executeDdl("CREATE TABLE " + source + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1),(2)");
        var policies = new org.pangrova.metadata.service.TableControlRowPolicyStore(
                new RegistryTablePolicyAuthority(metadataBootstrap.catalogRegistry()),
                new org.pangrova.security.store.jdbc.PersistentRowPolicyStore(stateStoreDataSource()));
        policies.savePolicy(new org.pangrova.security.api.rowfilter.RowPolicy(0, "private_rows", PAIMON,
                SCHEMA, table, "id > 1", List.of(), true, 1, null));
        var original = policies.getPolicies(PAIMON, SCHEMA, table).get(0);
        assertThat(sqlRunner.execute("SELECT id FROM " + source))
                .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(2L));
        clearFeatureSecurity();
        sqlRunner.executeDdl("ALTER TABLE " + source + " RENAME TO " + renamed);
        assertThat(sqlRunner.execute("SELECT id FROM " + renamed))
                .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(2L));
        assertThat(policies.getPolicies(PAIMON, SCHEMA, renamedTable).get(0).policyId())
                .isEqualTo(original.policyId());
        sqlRunner.executeDdl("CREATE TABLE " + source + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (0),(3)");
        assertThat(sqlRunner.execute("SELECT id FROM " + source)).hasSize(2);
        policies.savePolicy(new org.pangrova.security.api.rowfilter.RowPolicy(0, "private_rows", PAIMON,
                SCHEMA, table, "id > 2", List.of(), true, 1, null));
        policies.deletePolicy(original.policyId());
        assertThat(sqlRunner.execute("SELECT id FROM " + source))
                .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(3L));
        assertThat(sqlRunner.execute("SELECT id FROM " + renamed)).hasSize(2);
        sqlRunner.executeDdl("DROP TABLE " + source);
        sqlRunner.executeDdl("DROP TABLE " + renamed);
    }

    private void assertPrivateContacts(String table) {
        var rows = sqlRunner.execute("SELECT id,email,phone,ssn,person FROM " + table + " ORDER BY id");
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString("email")).isEqualTo("***@***");
        assertThat(rows.get(0).getString("phone")).isEqualTo("***7890");
        assertThat(rows.get(0).getString("ssn")).isEqualTo("***6789");
        assertThat(rows.get(0).getString("person")).isEqualTo("***");
        for (String column : List.of("email", "phone", "ssn", "person")) {
            assertThat(rows.get(1).getRaw(column)).isNull();
        }
        assertThat(rows.get(2).getString("email")).isEqualTo("***@***");
        assertThat(rows.get(2).getString("phone")).isEqualTo("***");
        assertThat(rows.get(2).getString("ssn")).isEqualTo("***");
        assertThat(rows.get(2).getString("person")).isEqualTo("***");
    }

    @Test
    void recreatedCatalogCannotInheritAuthorityForAnUnchangedPhysicalTable() throws Exception {
        var registry = metadataBootstrap.catalogRegistry();
        var source = registry.getCatalog(CATALOG).getDescriptor();
        String alias = "control_recreated_catalog";
        String table = "control_catalog_identity";
        String qualified = alias + "." + SCHEMA + "." + table;
        registry.register(new org.pangrova.metadata.api.catalog.CatalogDescriptor(alias, source.type(),
                source.provider(), source.state(), source.properties(), source.comment(), source.allowedTenants()));
        try {
            sqlRunner.executeDdl("CREATE TABLE " + qualified + " (id BIGINT)");
            sqlRunner.execute("INSERT INTO " + qualified + " VALUES (7)");
            sqlRunner.execute("SELECT id FROM " + qualified);
            var original = store.find(alias, SCHEMA, table).orElseThrow();
            registry.unregister(alias);
            registry.register(new org.pangrova.metadata.api.catalog.CatalogDescriptor(alias, source.type(),
                    source.provider(), source.state(), source.properties(), source.comment(), source.allowedTenants()));

            assertThat(registry.getCatalog(alias).getDescriptor().incarnationId())
                    .isNotEqualTo(original.catalogIncarnationId());
            for (String statement : List.of("SELECT id FROM " + qualified,
                    "INSERT INTO " + qualified + " VALUES (8)", "DROP TABLE " + qualified,
                    "ANALYZE TABLE " + qualified,
                    "CALL " + alias + ".compact('" + SCHEMA + "." + table + "')")) {
                assertThat(sqlRunner.executeExpectingError(statement).message())
                        .contains("catalog deployment");
            }
            assertThat(store.find(alias, SCHEMA, table)).contains(original);
            var physical = externalCatalog.loadTable(identifier(table));
            assertThat(((org.apache.iceberg.BaseTable) physical).operations().current().uuid())
                    .isEqualTo(original.formatTableUuid());
            assertThat(physical.currentSnapshot().summary()).containsEntry("total-records", "1");
        } finally {
            registry.unregister(alias);
        }
    }

    @Test
    void shadowDropChecksTheCapturedCatalogBeforeDeletingActualSdkFiles() throws Exception {
        var registry = metadataBootstrap.catalogRegistry();
        registry.setTableControlService(new TableControlService(store, registry::requireCurrentCatalogIdentity,
                new TableControlSettings(true, false, Set.of())));
        var source = registry.getCatalog(CATALOG).getDescriptor();
        String alias = "control_shadow_recreated_catalog";
        String table = "control_shadow_catalog_identity";
        String qualified = alias + "." + SCHEMA + "." + table;
        registry.register(new org.pangrova.metadata.api.catalog.CatalogDescriptor(alias, source.type(),
                source.provider(), source.state(), source.properties(), source.comment(), source.allowedTenants()));
        try {
            sqlRunner.executeDdl("CREATE TABLE " + qualified + " (id BIGINT)");
            sqlRunner.execute("INSERT INTO " + qualified + " VALUES (7)");
            var original = store.find(alias, SCHEMA, table).orElseThrow();
            registry.unregister(alias);
            registry.register(new org.pangrova.metadata.api.catalog.CatalogDescriptor(alias, source.type(),
                    source.provider(), source.state(), source.properties(), source.comment(), source.allowedTenants()));

            assertThat(sqlRunner.executeExpectingError("DROP TABLE " + qualified).message())
                    .contains("catalog deployment");
            assertThat(store.find(alias, SCHEMA, table)).contains(original);
            var physical = externalCatalog.loadTable(identifier(table));
            assertThat(((org.apache.iceberg.BaseTable) physical).operations().current().uuid())
                    .isEqualTo(original.formatTableUuid());
            assertThat(physical.currentSnapshot().summary()).containsEntry("total-records", "1");
        } finally {
            registry.unregister(alias);
        }
    }

    @Test
    void externalDataRemainsReadableAcrossAdoptAndUnlinkButNeverWritable() throws Exception {
        String table = "control_external_lifecycle";
        Table external = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(external, 41L);

        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)).get(0).getLong("id"))
                .isEqualTo(41L);
        assertThat(store.find(CATALOG, SCHEMA, table)).isEmpty();
        assertThat(sqlRunner.executeExpectingError("INSERT INTO " + fqn(table) + " VALUES (42)").message())
                .contains("OBSERVED");
        assertThat(sqlRunner.executeExpectingError("DROP TABLE " + fqn(table)).message())
                .contains("OBSERVED");
        assertThat(sqlRunner.executeExpectingError(
                "CALL " + CATALOG + ".compact('" + SCHEMA + "." + table + "')").message())
                .contains("OBSERVED");

        sqlRunner.executeDdl("ALTER TABLE " + fqn(table) + " ADOPT");
        var linked = store.find(CATALOG, SCHEMA, table).orElseThrow();
        assertThat(linked.ownershipMode()).isEqualTo(OwnershipMode.LINKED);
        assertThat(new JdbcTableControlStore(stateStoreDataSource()).findById(linked.tableId()))
                .contains(linked);
        assertThat(sqlRunner.executeExpectingError("INSERT INTO " + fqn(table) + " VALUES (42)").message())
                .contains("LINKED");
        assertThat(sqlRunner.executeExpectingError("DELETE FROM " + fqn(table)).message())
                .contains("LINKED");
        assertThat(sqlRunner.executeExpectingError("DROP TABLE " + fqn(table)).message())
                .contains("LINKED");
        assertThat(sqlRunner.executeExpectingError(
                "CALL " + CATALOG + ".compact('" + SCHEMA + "." + table + "')").message())
                .contains("not enabled");

        sqlRunner.executeDdl("ALTER TABLE " + fqn(table) + " UNLINK");
        assertThat(store.find(CATALOG, SCHEMA, table)).isEmpty();
        assertThat(externalCatalog.tableExists(identifier(table))).isTrue();
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)))
                .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(41L));
    }

    @Test
    void managedCallAndAnalyzeExecuteThroughTheControlPlane() throws Exception {
        String table = "control_managed_maintenance";
        sqlRunner.executeDdl("CREATE TABLE " + fqn(table) + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (7)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (8)");

        assertThat(sqlRunner.execute("CALL " + CATALOG + ".compact('" + SCHEMA + "." + table + "')"))
                .hasSize(1);
        assertThat(sqlRunner.execute("ANALYZE TABLE " + fqn(table)))
                .singleElement().satisfies(row -> assertThat(row.getLong("row_count")).isEqualTo(2L));
        var record = store.find(CATALOG, SCHEMA, table).orElseThrow();
        assertThat(metadataBootstrap.catalogRegistry().tableStatistics().fresh(record))
                .hasValueSatisfying(stats -> assertThat(stats.statistics().rowCount()).isEqualTo(2L));
        assertThat(sqlRunner.executeExpectingError("CALL " + CATALOG
                + ".expire_snapshots('" + SCHEMA + "." + table + "', 1)").message())
                .contains("retention policy");
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table) + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(7L, 8L);
    }

    @Test
    void missingQualityRunnerRejectsActualNativeWriteAndPersistsFailedAudit() throws Exception {
        String table = "controlled_quality_runner";
        String source = PAIMON + "." + SCHEMA + "." + table;
        sqlRunner.executeDdl("CREATE TABLE " + source + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1)");
        var owner = store.find(PAIMON, SCHEMA, table).orElseThrow();
        var persisted = new org.pangrova.governance.store.jdbc.JdbcQualityRuleStore(stateStoreDataSource());
        var rules = new org.pangrova.metadata.adapter.governance.TableControlQualityRuleStore(
                new RegistryTablePolicyAuthority(metadataBootstrap.catalogRegistry()), persisted);
        long ruleId = java.util.UUID.randomUUID().getLeastSignificantBits() & Long.MAX_VALUE;
        var now = java.time.Instant.now();
        rules.saveRule(new org.pangrova.governance.api.quality.DataQualityRule(ruleId,
                "required_positive", PAIMON, SCHEMA, table, "id > 0",
                org.pangrova.governance.api.quality.Severity.ERROR, "msg", true, null, now, now,
                new org.pangrova.governance.api.quality.QualityExecutionIdentity("e2e-user", "e2e-tenant"), null));
        configureWriteQualityGate(new org.pangrova.governance.core.quality.RuleBasedWriteQualityGate(
                rules, new org.pangrova.governance.quality.executor.unconfigured.UnconfiguredQualityRuleExecutor()));
        try {
            assertThat(sqlRunner.executeExpectingError("INSERT INTO " + source + " VALUES (2)").message())
                    .contains("was not evaluated", "no authorized data-plane");
            assertThat(sqlRunner.execute("SELECT id FROM " + source))
                    .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(1L));
            try (var connection = stateStoreDataSource().getConnection();
                    var query = connection.prepareStatement("SELECT table_id,passed,total_rows,failed_rows,"
                            + "error_message FROM quality_check_results WHERE rule_id=?")) {
                query.setLong(1, ruleId);
                try (var audit = query.executeQuery()) {
                    assertThat(audit.next()).isTrue();
                    assertThat(audit.getObject("table_id", java.util.UUID.class)).isEqualTo(owner.tableId());
                    assertThat(audit.getBoolean("passed")).isFalse();
                    assertThat(audit.getLong("total_rows")).isZero();
                    assertThat(audit.getLong("failed_rows")).isZero();
                    assertThat(audit.getString("error_message")).contains("was not evaluated");
                    assertThat(audit.next()).isFalse();
                }
            }
        } finally {
            configureWriteQualityGate(null);
            persisted.deleteRule(ruleId);
            sqlRunner.executeDdl("DROP TABLE " + source);
        }
    }

    @Test
    void readsRejectAnExternalReplacementBeforeAnyWriteObservesIt() throws Exception {
        String table = "control_read_replacement";
        sqlRunner.executeDdl("CREATE TABLE " + fqn(table) + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (1)");
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)))
                .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(1L));
        var original = store.find(CATALOG, SCHEMA, table).orElseThrow();

        externalCatalog.dropTable(identifier(table), true);
        Table replacement = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(replacement, 99L);

        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + fqn(table)).message())
                .contains("identity");
        assertThat(store.findById(original.tableId()).orElseThrow().state())
                .isEqualTo(TableControlState.QUARANTINED);
        assertThat(externalCatalog.loadTable(identifier(table)).currentSnapshot().summary())
                .containsEntry("total-records", "1");
    }

    @Test
    void tagAdmissionRejectsExternalReplacementBeforeAReadObservesIt() throws Exception {
        String table = "control_tag_replacement";
        sqlRunner.executeDdl("CREATE TABLE " + fqn(table) + " (id BIGINT)");
        var original = store.find(CATALOG, SCHEMA, table).orElseThrow();
        externalCatalog.dropTable(identifier(table), true);
        externalCatalog.createTable(identifier(table), TABLE_SCHEMA);

        assertThat(sqlRunner.executeExpectingError("ALTER TABLE " + fqn(table)
                + " ALTER COLUMN ID SET TAG classification = 'private'").message()).contains("identity");
        assertThat(store.findById(original.tableId()).orElseThrow().state())
                .isEqualTo(TableControlState.QUARANTINED);
        assertThat(metadataBootstrap.catalogRegistry().tableGovernance().bindingsFor(original.tableId())).isEmpty();
        assertThat(externalCatalog.tableExists(identifier(table))).isTrue();
    }

    @Test
    void managedWritesSucceedButDropOfAnExternalReplacementIsRejected() throws Exception {
        String table = "control_managed_replacement";
        sqlRunner.executeDdl("CREATE TABLE " + fqn(table) + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (1)");
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)).get(0).getLong("id"))
                .isEqualTo(1L);
        assertThat(store.find(CATALOG, SCHEMA, table).orElseThrow().ownershipMode())
                .isEqualTo(OwnershipMode.MANAGED);

        externalCatalog.dropTable(identifier(table), true);
        Table replacement = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(replacement, 99L);
        assertThat(sqlRunner.executeExpectingError("DROP TABLE " + fqn(table)).message())
                .contains("identity");

        assertThat(externalCatalog.tableExists(identifier(table))).isTrue();
        assertThat(store.find(CATALOG, SCHEMA, table).orElseThrow().state())
                .isEqualTo(TableControlState.QUARANTINED);
        assertThat(externalCatalog.loadTable(identifier(table)).currentSnapshot().summary())
                .containsEntry("total-records", "1");
    }

    @Test
    void managedPaimonRenamePreservesOwnershipAndRefreshesPhysicalIdentity() {
        String before = PAIMON + "." + SCHEMA + ".control_before_rename";
        String after = PAIMON + "." + SCHEMA + ".control_after_rename";
        sqlRunner.executeDdl("CREATE TABLE " + before + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + before + " VALUES (7)");
        sqlRunner.execute("SELECT id FROM " + before);
        var original = store.find(PAIMON, SCHEMA, "control_before_rename").orElseThrow();

        sqlRunner.executeDdl("ALTER TABLE " + before + " RENAME TO " + after);
        var renamed = store.find(PAIMON, SCHEMA, "control_after_rename").orElseThrow();
        assertThat(store.find(PAIMON, SCHEMA, "control_before_rename")).isEmpty();
        assertThat(renamed.tableId()).isEqualTo(original.tableId());
        assertThat(renamed.state()).isEqualTo(TableControlState.ACTIVE);
        assertThat(renamed.formatLocation()).isNotEqualTo(original.formatLocation());
        sqlRunner.execute("INSERT INTO " + after + " VALUES (8)");
        assertThat(sqlRunner.execute("SELECT id FROM " + after + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(7L, 8L);
        sqlRunner.executeDdl("DROP TABLE " + after);
        assertThat(store.findById(original.tableId()).orElseThrow().state())
                .isEqualTo(TableControlState.DROPPED);
    }

    @Test
    void unqualifiedEqualityDeleteReadsFailClosedBeforeNativeExecution() throws Exception {
        String table = "control_external_equality_delete";
        Table external = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(external, 41L);
        var deleted = GenericRecord.create(TABLE_SCHEMA);
        deleted.setField("id", 41L);
        var deleteWriter = Parquet.writeDeletes(external.io().newOutputFile(
                        external.location() + "/data/external-equality-delete.parquet"))
                .forTable(external).withSpec(external.spec())
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(new int[] {1}).rowSchema(TABLE_SCHEMA)
                .buildEqualityWriter();
        try (deleteWriter) { deleteWriter.write(deleted); }
        external.newRowDelta().addDeletes(deleteWriter.toDeleteFile()).commit();

        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + fqn(table)).message())
                .contains("equality deletes").contains(table);
        assertThat(sqlRunner.executeExpectingError("SELECT COUNT(*) FROM " + fqn(table)).message())
                .contains("equality deletes").contains(table);
    }

    @Test
    void externalSdkDeletionVectorsSelectTheBlobForEachDataFile() throws Exception {
        String name = "control_external_deletion_vectors";
        Table table = externalCatalog.buildTable(identifier(name), TABLE_SCHEMA)
                .withProperty("format-version", "3").create();
        var first = append(table, 11L, 12L);
        var second = append(table, 21L, 22L);
        var writer = new org.apache.iceberg.deletes.BaseDVFileWriter(
                () -> table.io().newOutputFile(table.location() + "/data/external-dv.puffin"),
                ignored -> null);
        try (writer) {
            writer.delete(first.path().toString(), 0, table.spec(), first.partition());
            writer.delete(second.path().toString(), 1, table.spec(), second.partition());
        }
        var deletes = writer.result().deleteFiles();
        assertThat(deletes).hasSize(2);
        assertThat(deletes.get(0).path()).isEqualTo(deletes.get(1).path());
        var commit = table.newRowDelta();
        deletes.forEach(commit::addDeletes);
        commit.commit();
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(name) + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(12L, 21L);
        assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + fqn(name)))
                .singleElement().satisfies(row -> assertThat(row.getLong("n")).isEqualTo(2L));
    }

    private static org.apache.iceberg.DataFile append(Table table, long... ids) throws Exception {
        DataWriter<Record> writer = Parquet.writeData(
                        table.io().newOutputFile(table.location() + "/data/external-"
                                + java.util.UUID.randomUUID() + ".parquet"))
                .schema(table.schema()).withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            for (long id : ids) {
                GenericRecord row = GenericRecord.create(table.schema());
                row.setField("id", id);
                writer.write(row);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
        return writer.toDataFile();
    }

    private static TableIdentifier identifier(String table) {
        return TableIdentifier.of(SCHEMA, table);
    }

    private static String fqn(String table) {
        return CATALOG + "." + SCHEMA + "." + table;
    }
}
