package org.pangrova.testing.integration.e2e.materializedview;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL-to-Native full refresh of Materialized Views through the unified kernel. */
@ResourceLock("pangrova-shared-dynamic-table-runtime")
class MaterializedViewMaterializationE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String TENANT = "e2e-tenant";

    private DurableDynamicTableAuthority authority;
    private boolean restrictedAuthority;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void installProductionHandlers() {
        authority = installDurableDynamicTableRuntime();
    }

    @AfterEach
    void restoreSharedFixture() {
        if (restrictedAuthority) {
            restoreDefaultSqlAuthority();
            restrictedAuthority = false;
        }
    }

    @Test
    void createPublishesFirstGenerationAndRefreshAdvancesTheHead() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String source = CATALOG + "." + SCHEMA + ".mv_source_" + suffix;
        String logical = CATALOG + "." + SCHEMA + ".mv_totals_" + suffix;
        String sourceTable = "mv_source_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");

        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + source);

        PublishedMaterialization first = published(logical);
        assertThat(first.kind()).isEqualTo(MaterializationKind.MATERIALIZED_VIEW);
        assertThat(first.physicalTable().schema()).isEqualTo(InternalNamespace.SCHEMA_NAME);
        assertThat(first.physicalTable().table()).startsWith(InternalNamespace.CANDIDATE_PREFIX);
        assertThat(first.definitionRevision()).isEqualTo(1);
        assertIds(logical, 1L, 2L);
        assertPhysicalCommittedRows(first.physicalTable().qualifiedName(), 2);
        assertThat(sqlRunner.executeExpectingError(
                "SELECT id FROM " + first.physicalTable().qualifiedName())
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");

        List<TypedRow> shown = sqlRunner.execute("SHOW MATERIALIZED VIEWS");
        assertThat(shown).anySatisfy(row -> {
            assertThat(row.getString("mv_name")).isEqualTo(logical);
            assertThat(row.getString("refresh_mode")).isEqualTo("FULL");
            assertThat(row.getString("schedule_state")).isEqualTo("DISABLED");
            assertThat(row.getString("published_table"))
                    .isEqualTo(first.physicalTable().qualifiedName());
        });

        List<TypedRow> described = sqlRunner.execute("DESCRIBE MATERIALIZED VIEW " + logical);
        assertThat(described).hasSize(1);
        assertThat(described.get(0).getString("published_table"))
                .isEqualTo(first.physicalTable().qualifiedName());
        assertThat(described.get(0).getLong("revision")).isEqualTo(1L);

        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        assertIds(logical, 1L, 2L);

        sqlRunner.executeDdl("REFRESH MATERIALIZED VIEW " + logical);
        PublishedMaterialization second = published(logical);
        assertThat(second.physicalTable().qualifiedName())
                .isNotEqualTo(first.physicalTable().qualifiedName());
        assertThat(second.definitionRevision()).isEqualTo(1);
        assertThat(objectRevision(logical)).isEqualTo(2L);
        assertIds(logical, 1L, 2L, 3L);
        assertPhysicalCommittedRows(first.physicalTable().qualifiedName(), 2);
        assertPhysicalCommittedRows(second.physicalTable().qualifiedName(), 3);

        authority = installDurableDynamicTableRuntime();
        PublishedMaterialization recovered = reopenDynamicTablePublicationCatalog()
                .findPublished(id(logical)).orElseThrow();
        assertThat(recovered.physicalTable().qualifiedName())
                .isEqualTo(second.physicalTable().qualifiedName());
        assertIds(logical, 1L, 2L, 3L);

        sqlRunner.executeDdl("DROP MATERIALIZED VIEW " + logical);
        assertThat(authority.catalog().findPublished(id(logical))).isEmpty();
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + logical).message())
                .isNotBlank();
        assertPhysicalCommittedRows(second.physicalTable().qualifiedName(), 3);

        var tombstone = authority.repository().findObject(id(logical)).orElseThrow();
        assertThat(tombstone.state()).isEqualTo(ObjectState.Dropped.INSTANCE);

        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + source + " WHERE id >= 2");
        PublishedMaterialization reclaimed = published(logical);
        assertThat(authority.repository().findObject(id(logical)).orElseThrow().incarnation())
                .isNotEqualTo(tombstone.incarnation());
        assertThat(reclaimed.definitionRevision()).isEqualTo(1);
        assertThat(objectRevision(logical)).isEqualTo(1L);
        assertIds(logical, 2L, 3L);

        sqlRunner.executeDdl("DROP MATERIALIZED VIEW " + logical);
        assertThat(authority.catalog().findPublished(id(logical))).isEmpty();
    }

    @Test
    void replaceRenameAndScheduleChangesFollowTheKernelContract() {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "mv_ddl_source_" + suffix;
        String logicalTable = "mv_ddl_totals_" + suffix;
        String renamedTable = logicalTable + "_v2";
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        String renamed = CATALOG + "." + SCHEMA + "." + renamedTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");

        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + source);
        PublishedMaterialization first = published(logical);
        assertThat(first.definitionRevision()).isEqualTo(1);
        assertIds(logical, 1L, 2L);

        assertThat(sqlRunner.executeExpectingError("CREATE OR REPLACE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + CATALOG + "." + SCHEMA + ".mv_missing_" + suffix)
                .message()).isNotBlank();
        assertThat(published(logical).physicalTable().qualifiedName())
                .isEqualTo(first.physicalTable().qualifiedName());
        assertThat(objectRevision(logical)).isEqualTo(1L);
        assertIds(logical, 1L, 2L);

        sqlRunner.executeDdl("CREATE OR REPLACE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + source + " WHERE id >= 2");
        PublishedMaterialization replaced = published(logical);
        assertThat(replaced.definitionRevision()).isEqualTo(2);
        assertThat(replaced.physicalTable().qualifiedName())
                .isNotEqualTo(first.physicalTable().qualifiedName());
        assertIds(logical, 2L);

        sqlRunner.executeDdl("ALTER MATERIALIZED VIEW " + logical
                + " SET TARGET_LAG = 'PT2M'");
        List<TypedRow> scheduled = sqlRunner.execute(
                "SHOW MATERIALIZED VIEWS LIKE '%" + logicalTable + "%'");
        assertThat(scheduled).hasSize(1);
        assertThat(scheduled.get(0).getString("mv_name")).isEqualTo(logical);
        assertThat(scheduled.get(0).getString("schedule_state")).isEqualTo("ACTIVE");
        assertThat(scheduled.get(0).getString("target_lag")).isEqualTo("PT2M");

        sqlRunner.executeDdl("ALTER MATERIALIZED VIEW " + logical
                + " SET REFRESH_MODE = INCREMENTAL");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        sqlRunner.executeDdl("REFRESH MATERIALIZED VIEW " + logical);
        assertIds(logical, 2L, 3L);
        assertThat(authority.repository().findPublishedGeneration(id(logical))
                .orElseThrow().refreshKind())
                .isEqualTo(org.pangrova.materialization.domain.RefreshKind
                        .IncrementalAppendOnly.INSTANCE);

        String headBeforeRename = published(logical).physicalTable().qualifiedName();
        sqlRunner.executeDdl("ALTER MATERIALIZED VIEW " + logical
                + " RENAME TO " + renamed);
        assertThat(published(renamed).physicalTable().qualifiedName())
                .isEqualTo(headBeforeRename);
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + logical).message())
                .isNotBlank();
        assertIds(renamed, 2L, 3L);

        sqlRunner.executeDdl("DROP MATERIALIZED VIEW IF EXISTS " + CATALOG + "." + SCHEMA
                + ".mv_ddl_missing_" + suffix);
        sqlRunner.executeDdl("DROP MATERIALIZED VIEW " + renamed);
        assertThat(authority.catalog().findPublished(id(renamed))).isEmpty();
    }

    @Test
    void explainReportsWhyRewriteDidOrDidNotApply() {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "mv_explain_source_" + suffix;
        String logicalTable = "mv_explain_totals_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");

        assertThat(explain("SELECT id FROM " + source))
                .contains("materialized_view_rewrite: skipped reason=no_published_candidate");

        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + source);

        assertThat(explain("SELECT id FROM " + source))
                .contains(InternalNamespace.SCHEMA_NAME)
                .contains("materialized_view_rewrite: applied views=" + logical);
    }

    @Test
    void cascadeDropRemovesDependentMaterializationsBeforeTheirUpstream() {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "mv_dep_source_" + suffix;
        String upstreamTable = "mv_dep_upstream_" + suffix;
        String dependentTable = "mv_dep_dependent_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String upstream = CATALOG + "." + SCHEMA + "." + upstreamTable;
        String dependent = CATALOG + "." + SCHEMA + "." + dependentTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + upstream
                + " AS SELECT id FROM " + source);
        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + dependent
                + " AS SELECT id FROM " + upstream);
        assertIds(upstream, 1L, 2L);
        assertIds(dependent, 1L, 2L);

        assertThat(sqlRunner.executeExpectingError(
                "DROP MATERIALIZED VIEW " + upstream).message())
                .contains("referenced by")
                .contains(dependentTable);

        sqlRunner.executeDdl("DROP MATERIALIZED VIEW " + upstream + " CASCADE");
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + upstream).message())
                .isNotBlank();
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + dependent).message())
                .isNotBlank();
        assertIds(source, 1L, 2L);
    }

    @Test
    void logicalGrantsAuthorizeCreateRefreshAndDropWithoutPhysicalReads() {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "mv_secure_source_" + suffix;
        String logicalTable = "mv_secure_totals_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");

        String role = "mv_scoped_" + suffix;
        var roles = new JdbcRoleStore(stateStoreDataSource());
        roles.initSchema();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(
                UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        var grants = new JdbcGrantStore(stateStoreDataSource());
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
                "TABLE", CATALOG, SCHEMA, sourceTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.CREATE_TABLE,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        restrictDefaultSqlAuthority();
        restrictedAuthority = true;
        authority = installDurableDynamicTableRuntime();

        sqlRunner.executeDdl("CREATE MATERIALIZED VIEW " + logical
                + " AS SELECT id FROM " + source);
        PublishedMaterialization created = published(logical);
        assertIds(logical, 1L, 2L);
        String head = created.physicalTable().qualifiedName();

        assertThat(sqlRunner.executeExpectingError("REFRESH MATERIALIZED VIEW " + logical)
                .message()).contains("HTTP 403", "SECURITY-0002");
        assertThat(published(logical).physicalTable().qualifiedName()).isEqualTo(head);

        assertThat(sqlRunner.executeExpectingError("DROP MATERIALIZED VIEW " + logical)
                .message()).contains("HTTP 403", "SECURITY-0002");
        assertThat(authority.catalog().findPublished(id(logical))).isPresent();

        grants.addGrant(PrivilegeGrant.of(role, Privilege.ALTER,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        sqlRunner.executeDdl("REFRESH MATERIALIZED VIEW " + logical);
        assertThat(published(logical).definitionRevision()).isEqualTo(1);
        assertThat(objectRevision(logical)).isEqualTo(2L);

        grants.addGrant(PrivilegeGrant.of(role, Privilege.DROP,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        sqlRunner.executeDdl("DROP MATERIALIZED VIEW " + logical);
        assertThat(authority.catalog().findPublished(id(logical))).isEmpty();
    }

    private MaterializationId id(String logical) {
        return new MaterializationId(new TenantId(TENANT), QualifiedName.parse(logical));
    }

    private PublishedMaterialization published(String logical) {
        return authority.catalog().findPublished(id(logical)).orElseThrow();
    }

    private long objectRevision(String logical) {
        return authority.repository().findObject(id(logical)).orElseThrow().revision();
    }

    private String explain(String sql) {
        List<TypedRow> rows = executeWithBoundSession("EXPLAIN " + sql,
                Map.of("catalog", CATALOG, "schema", SCHEMA));
        assertThat(rows).hasSize(1);
        return rows.get(0).getString("plan");
    }

    private static void assertIds(String table, Long... expected) {
        List<TypedRow> rows = sqlRunner.execute("SELECT id FROM " + table + " ORDER BY id");
        assertThat(rows).extracting(row -> row.getLong("id")).containsExactly(expected);
    }

    private static void assertPhysicalCommittedRows(
            String qualifiedTable, long expected) throws Exception {
        String[] parts = qualifiedTable.split("\\.");
        try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
            var physical = catalog.loadTable(TableIdentifier.of(parts[1], parts[2]));
            assertThat(Long.parseLong(physical.currentSnapshot().summary().get("total-records")))
                    .isEqualTo(expected);
        }
    }
}
