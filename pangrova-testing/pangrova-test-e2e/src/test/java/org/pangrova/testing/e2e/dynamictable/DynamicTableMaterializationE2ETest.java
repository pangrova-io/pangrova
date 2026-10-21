package org.pangrova.testing.e2e.dynamictable;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.metadata.dynamictable.DynamicTablePublication;
import org.pangrova.metadata.dynamictable.JdbcDynamicTablePublicationStore;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL-to-Native full materialization with durable, atomic Dynamic Table publication. */
@ResourceLock("pangrova-shared-dynamic-table-runtime")
class DynamicTableMaterializationE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String TENANT = "e2e-tenant";

    private JdbcDynamicTablePublicationStore publications;
    private boolean restrictedAuthority;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void installProductionHandlers() {
        publications = installDurableDynamicTableRuntime();
    }

    @AfterEach
    void restoreSharedFixture() {
        if (restrictedAuthority) {
            restoreDefaultSqlAuthority();
            restrictedAuthority = false;
        }
        restoreTestDynamicTableRuntime();
    }

    @Test
    void preciseLogicalGrantsCannotImpersonateUnownedPhysicalWrite() {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_source_" + suffix;
        String logicalTable = "dt_rollup_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " TARGET_LAG = 'PT0S' REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        String originalHead = publications.load(TENANT, logical).orElseThrow().publishedTable();
        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");

        String role = "dt_scoped_" + suffix;
        var roles = new PersistentRoleStore(stateStoreDataSource());
        roles.initSchema();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        var grants = new PersistentGrantStore(stateStoreDataSource());
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
                "TABLE", CATALOG, SCHEMA, sourceTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.CREATE_TABLE,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.ALTER,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        restrictDefaultSqlAuthority();
        restrictedAuthority = true;

        assertThat(sqlRunner.executeExpectingError("ALTER DYNAMIC TABLE " + logical + " REFRESH")
                .message()).contains("HTTP 403", "SECURITY-0002");
        assertThat(publications.load(TENANT, logical).orElseThrow().publishedTable())
                .isEqualTo(originalHead);
        assertIds(logical, 1L, 2L);

        String anotherLogical = CATALOG + "." + SCHEMA + ".dt_new_" + suffix;
        grants.addGrant(PrivilegeGrant.of(role, Privilege.CREATE_TABLE,
                "TABLE", CATALOG, SCHEMA, "dt_new_" + suffix, null));
        assertThat(sqlRunner.executeExpectingError("CREATE DYNAMIC TABLE " + anotherLogical
                + " TARGET_LAG = 'PT0S' AS SELECT id FROM " + source)
                .message()).contains("HTTP 403", "SECURITY-0002");
        var unpublished = publications.load(TENANT, anotherLogical).orElseThrow();
        assertThat(unpublished.state()).isEqualTo(DynamicTablePublication.State.CREATING);
        assertThat(unpublished.publishedTable()).isNull();
        assertThat(unpublished.revision()).isZero();
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + anotherLogical).message()).isNotBlank();
    }

    @Test
    void emptyGenerationHasARealCommittedSnapshotAndChangesOnlyAfterRefresh() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String source = CATALOG + "." + SCHEMA + ".dt_empty_source_" + suffix;
        String logical = CATALOG + "." + SCHEMA + ".dt_empty_rollup_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, "dt_empty_source_" + suffix, "id BIGINT");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " TARGET_LAG = 'PT0S' REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        var empty = publications.load(TENANT, logical).orElseThrow();
        var emptyAttempt = assertPublishedAttempt(empty);
        assertIds(logical);
        sqlRunner.execute("INSERT INTO " + source + " VALUES (42)");
        assertIds(logical);
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " REFRESH");
        var refreshed = publications.load(TENANT, logical).orElseThrow();
        assertThat(refreshed.revision()).isEqualTo(empty.revision() + 1);
        assertThat(refreshed.publishedTable()).isNotEqualTo(empty.publishedTable());
        assertPublishedAttempt(refreshed);
        assertIds(logical, 42L);
        assertThat(publications.findRefresh(TENANT, emptyAttempt.executionId())).contains(emptyAttempt);
    }

    @Test
    void publishedHeadReadsCommittedGenerationUntilManualRefreshAndSurvivesStoreReopen() throws Exception {
        Set<String> before = queryManager.getStateMachines().keySet();
        String suffix = Long.toHexString(System.nanoTime());
        String source = CATALOG + "." + SCHEMA + ".dt_source_" + suffix;
        String logical = CATALOG + "." + SCHEMA + ".dt_rollup_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, "dt_source_" + suffix, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");

        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " TARGET_LAG = 'PT0S' REFRESH_MODE = SNAPSHOT AS "
                + "SELECT id FROM " + source);
        DynamicTablePublication first = publications.load(TENANT, logical).orElseThrow();
        assertThat(first.state()).isEqualTo(DynamicTablePublication.State.ACTIVE);
        assertThat(first.publishedTable()).startsWith(CATALOG + "." + SCHEMA + ".__pg_dt_");
        assertIds(logical, 1L, 2L);
        assertPhysicalCommittedRows(first.publishedTable(), 2);
        var firstAttempt = assertPublishedAttempt(first);
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + first.publishedTable())
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");
        assertThat(sqlRunner.executeExpectingError("SELECT l.id FROM " + logical
                + " l JOIN " + first.publishedTable() + " p ON l.id=p.id")
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");

        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        assertIds(source, 1L, 2L, 3L);
        assertIds(logical, 1L, 2L);

        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " REFRESH");
        DynamicTablePublication second = publications.load(TENANT, logical).orElseThrow();
        assertThat(second.publishedTable()).isNotEqualTo(first.publishedTable());
        assertThat(second.revision()).isEqualTo(first.revision() + 1);
        assertIds(logical, 1L, 2L, 3L);
        assertPhysicalCommittedRows(first.publishedTable(), 2);
        assertPhysicalCommittedRows(second.publishedTable(), 3);
        var secondAttempt = assertPublishedAttempt(second);
        assertThat(secondAttempt.executionId()).isNotEqualTo(firstAttempt.executionId());

        sqlRunner.executeDdl("DROP TABLE " + source);
        assertThat(sqlRunner.executeExpectingError("ALTER DYNAMIC TABLE " + logical + " REFRESH")
                .message()).isNotBlank();
        assertThat(publications.load(TENANT, logical).orElseThrow().publishedTable())
                .isEqualTo(second.publishedTable());
        assertIds(logical, 1L, 2L, 3L);

        assertThat(publications.findRefresh(TENANT, firstAttempt.executionId())).contains(firstAttempt);
        assertThat(publications.findRefresh(TENANT, secondAttempt.executionId())).contains(secondAttempt);
        restoreTestDynamicTableRuntime();
        publications = installDurableDynamicTableRuntime();
        DynamicTablePublication recovered = reopenDynamicTablePublicationStore()
                .load(TENANT, logical).orElseThrow();
        assertThat(recovered.publishedTable()).isEqualTo(second.publishedTable());
        assertIds(logical, 1L, 2L, 3L);

        Set<String> issuedQueries = new HashSet<>(queryManager.getStateMachines().keySet());
        issuedQueries.removeAll(before);
        assertThat(issuedQueries).as("E2E submitted real Coordinator queries").isNotEmpty();
        Set<String> nativeQueries = issuedQueries.stream()
                .filter(queryId -> !queryManager.getStateMachines().get(queryId)
                        .getStageStates().isEmpty())
                .collect(java.util.stream.Collectors.toSet());
        assertThat(nativeQueries).as("E2E scheduled real Native stages").isNotEmpty();
        Awaitility.await("dynamic-table-query-physical-drain")
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    assertThat(queryManager.getActiveQueryIds())
                            .doesNotContainAnyElementsOf(issuedQueries);
                    for (String queryId : nativeQueries) {
                        taskTracker.requireQueryExecutionDrained(queryId);
                    }
                    for (String queryId : issuedQueries) {
                        assertThat(taskTracker.getTaskIds(queryId)).isEmpty();
                    }
                });
    }

    private org.pangrova.metadata.dynamictable.DynamicTableRefreshAttempt assertPublishedAttempt(
            DynamicTablePublication publication) throws Exception {
        String execution;
        try (var connection = stateStoreDataSource().getConnection(); var query = connection.prepareStatement(
                "SELECT execution_id FROM dynamic_table_refresh_attempts WHERE tenant_id=? AND candidate_table=?")) {
            query.setString(1, TENANT);
            query.setString(2, publication.publishedTable());
            try (var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                execution = rows.getString(1);
                assertThat(rows.next()).isFalse();
            }
        }
        var attempt = publications.findRefresh(TENANT, execution).orElseThrow();
        assertThat(attempt.state()).isEqualTo(
                org.pangrova.metadata.dynamictable.DynamicTableRefreshAttempt.State.PUBLISHED);
        assertThat(attempt.logicalName()).isEqualTo(publication.logicalName());
        assertThat(attempt.incarnationId()).isEqualTo(publication.incarnationId());
        assertThat(attempt.baseRevision()).isEqualTo(publication.revision() - 1);
        assertThat(attempt.commitFingerprint()).matches("[0-9a-f]{64}");
        try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
            var physical = catalog.loadTable(TableIdentifier.of(SCHEMA,
                    publication.publishedTable().substring(publication.publishedTable().lastIndexOf('.') + 1)));
            assertThat(attempt.publication().publishedVersion()).isEqualTo(physical.currentSnapshot().snapshotId());
            try (var input = physical.io().newInputFile(physical.currentSnapshot().manifestListLocation()).newStream()) {
                assertThat(attempt.publication().manifestSha256()).isEqualTo(
                        java.security.MessageDigest.getInstance("SHA-256").digest(input.readAllBytes()));
            }
        }
        return attempt;
    }

    private static void assertIds(String table, Long... expected) {
        List<TypedRow> rows = sqlRunner.execute("SELECT id FROM " + table + " ORDER BY id");
        assertThat(rows).extracting(row -> row.getLong("id")).containsExactly(expected);
    }

    private static void assertPhysicalCommittedRows(String qualifiedTable, long expected) throws Exception {
        String table = qualifiedTable.substring(qualifiedTable.lastIndexOf('.') + 1);
        try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
            var physical = catalog.loadTable(TableIdentifier.of(SCHEMA, table));
            assertThat(Long.parseLong(physical.currentSnapshot().summary().get("total-records")))
                    .isEqualTo(expected);
        }
    }
}
