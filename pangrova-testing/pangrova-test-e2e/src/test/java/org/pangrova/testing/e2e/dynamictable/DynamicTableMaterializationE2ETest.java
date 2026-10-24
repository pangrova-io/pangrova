package org.pangrova.testing.e2e.dynamictable;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.materialization.domain.AttemptState;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.PublishedMaterialization;
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

/** SQL-to-Native full materialization through the unified kernel and durable head. */
@ResourceLock("pangrova-shared-dynamic-table-runtime")
class DynamicTableMaterializationE2ETest extends SqlE2ETestBase {

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
    void manualRefreshPublishesNewGenerationAndLogicalReadStaysOnHead() throws Exception {
        Set<String> before = queryManager.getStateMachines().keySet();
        String suffix = Long.toHexString(System.nanoTime());
        String source = CATALOG + "." + SCHEMA + ".dt_source_" + suffix;
        String logical = CATALOG + "." + SCHEMA + ".dt_rollup_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, "dt_source_" + suffix, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");

        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " REFRESH_MODE = SNAPSHOT AS "
                + "SELECT id FROM " + source);
        PublishedMaterialization first = published(logical);
        assertThat(first.physicalTable().schema()).isEqualTo(InternalNamespace.SCHEMA_NAME);
        assertThat(first.physicalTable().table()).startsWith(InternalNamespace.CANDIDATE_PREFIX);
        assertThat(revision(logical)).isEqualTo(1);
        assertIds(logical, 1L, 2L);
        assertPhysicalCommittedRows(first.physicalTable().qualifiedName(), 2);
        assertPublishedGeneration(logical);
        String firstPhysical = first.physicalTable().qualifiedName();
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + firstPhysical)
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");
        assertThat(sqlRunner.executeExpectingError("SELECT l.id FROM " + logical
                + " l JOIN " + firstPhysical + " p ON l.id=p.id")
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");

        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        assertIds(source, 1L, 2L, 3L);
        assertIds(logical, 1L, 2L);

        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " REFRESH");
        PublishedMaterialization second = published(logical);
        assertThat(second.physicalTable().qualifiedName()).isNotEqualTo(firstPhysical);
        assertThat(revision(logical)).isEqualTo(2);
        assertIds(logical, 1L, 2L, 3L);
        assertPhysicalCommittedRows(firstPhysical, 2);
        assertPhysicalCommittedRows(second.physicalTable().qualifiedName(), 3);
        assertThat(second.generation()).isNotEqualTo(first.generation());
        assertPublishedGeneration(logical);

        sqlRunner.executeDdl("DROP TABLE " + source);
        assertThat(sqlRunner.executeExpectingError("ALTER DYNAMIC TABLE " + logical + " REFRESH")
                .message()).isNotBlank();
        assertThat(published(logical).physicalTable().qualifiedName())
                .isEqualTo(second.physicalTable().qualifiedName());
        assertIds(logical, 1L, 2L, 3L);

        authority = installDurableDynamicTableRuntime();
        PublishedMaterialization recovered = reopenDynamicTablePublicationCatalog()
                .findPublished(id(logical)).orElseThrow();
        assertThat(recovered.physicalTable().qualifiedName())
                .isEqualTo(second.physicalTable().qualifiedName());
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

    @Test
    void autoRefreshModeAppendsOnlyTheDelta() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_inc_source_" + suffix;
        String logicalTable = "dt_inc_rollup_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " REFRESH_MODE = AUTO AS SELECT id FROM " + source);
        var first = authority.repository().findPublishedGeneration(id(logical)).orElseThrow();
        assertThat(first.refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertIds(logical, 1L, 2L);

        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " REFRESH");
        var second = authority.repository().findPublishedGeneration(id(logical)).orElseThrow();
        assertThat(second.refreshKind()).isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
        assertIds(logical, 1L, 2L, 3L);

        sqlRunner.execute("DELETE FROM " + source + " WHERE id = 1");
        assertThat(sqlRunner.executeExpectingError(
                "ALTER DYNAMIC TABLE " + logical + " REFRESH").message())
                .isNotBlank();
        assertIds(logical, 1L, 2L, 3L);
    }

    @Test
    void cascadeDropRemovesTransitiveDependentsBeforeTheirUpstream() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_dep_source_" + suffix;
        String upstreamTable = "dt_dep_upstream_" + suffix;
        String dependentTable = "dt_dep_dependent_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String upstream = CATALOG + "." + SCHEMA + "." + upstreamTable;
        String dependent = CATALOG + "." + SCHEMA + "." + dependentTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + upstream
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + dependent
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + upstream);
        assertIds(upstream, 1L, 2L);
        assertIds(dependent, 1L, 2L);

        assertThat(sqlRunner.executeExpectingError("DROP DYNAMIC TABLE " + upstream).message())
                .contains("referenced by")
                .contains(dependentTable);
        assertIds(upstream, 1L, 2L);
        assertIds(dependent, 1L, 2L);

        sqlRunner.executeDdl("DROP DYNAMIC TABLE " + upstream + " CASCADE");
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + upstream).message())
                .isNotBlank();
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + dependent).message())
                .isNotBlank();
        assertIds(source, 1L, 2L);
    }

    @Test
    void emptyGenerationHasARealCommittedSnapshotAndChangesOnlyAfterRefresh() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String source = CATALOG + "." + SCHEMA + ".dt_empty_source_" + suffix;
        String logical = CATALOG + "." + SCHEMA + ".dt_empty_rollup_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, "dt_empty_source_" + suffix, "id BIGINT");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        PublishedMaterialization empty = published(logical);
        assertPublishedGeneration(logical);
        assertPhysicalCommittedRows(empty.physicalTable().qualifiedName(), 0);
        assertIds(logical);
        sqlRunner.execute("INSERT INTO " + source + " VALUES (42)");
        assertIds(logical);
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " REFRESH");
        PublishedMaterialization refreshed = published(logical);
        assertThat(revision(logical)).isEqualTo(2);
        assertThat(refreshed.physicalTable().qualifiedName())
                .isNotEqualTo(empty.physicalTable().qualifiedName());
        assertPublishedGeneration(logical);
        assertPhysicalCommittedRows(refreshed.physicalTable().qualifiedName(), 1);
        assertIds(logical, 42L);
    }

    @Test
    void logicalGrantsAuthorizeManageAndCreationButNeverExposePhysicalGenerations() {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_source_" + suffix;
        String logicalTable = "dt_rollup_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        String originalHead = published(logical).physicalTable().qualifiedName();

        String role = "dt_scoped_" + suffix;
        var roles = new PersistentRoleStore(stateStoreDataSource());
        roles.initSchema();
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
        var grants = new PersistentGrantStore(stateStoreDataSource());
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
                "TABLE", CATALOG, SCHEMA, sourceTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.CREATE_TABLE,
                "TABLE", CATALOG, SCHEMA, logicalTable, null));
        restrictDefaultSqlAuthority();
        restrictedAuthority = true;
        authority = installDurableDynamicTableRuntime();

        assertThat(sqlRunner.executeExpectingError("ALTER DYNAMIC TABLE " + logical + " REFRESH")
                .message()).contains("HTTP 403", "SECURITY-0002");
        assertThat(published(logical).physicalTable().qualifiedName()).isEqualTo(originalHead);
        assertIds(logical, 1L, 2L);

        String anotherLogical = CATALOG + "." + SCHEMA + ".dt_new_" + suffix;
        grants.addGrant(PrivilegeGrant.of(role, Privilege.CREATE_TABLE,
                "TABLE", CATALOG, SCHEMA, "dt_new_" + suffix, null));
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + anotherLogical
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        PublishedMaterialization created = published(anotherLogical);
        assertIds(anotherLogical, 1L, 2L);
        assertThat(sqlRunner.executeExpectingError(
                "SELECT id FROM " + created.physicalTable().qualifiedName())
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT, "TABLE", CATALOG,
                InternalNamespace.SCHEMA_NAME, created.physicalTable().table(), null));
        assertThat(sqlRunner.executeExpectingError(
                "SELECT id FROM " + created.physicalTable().qualifiedName())
                .message()).contains("Direct access to a Dynamic Table generation is forbidden");
    }

    private MaterializationId id(String logical) {
        return new MaterializationId(new TenantId(TENANT), QualifiedName.parse(logical));
    }

    private PublishedMaterialization published(String logical) {
        return authority.catalog().findPublished(id(logical)).orElseThrow();
    }

    private MaterializationObject header(String logical) {
        return authority.repository().findObject(id(logical)).orElseThrow();
    }

    private long revision(String logical) {
        return header(logical).revision();
    }

    private void assertPublishedGeneration(String logical) throws Exception {
        var generation = authority.repository().findPublishedGeneration(id(logical)).orElseThrow();
        assertThat(generation.state()).isEqualTo(GenerationState.Published.INSTANCE);
        var evidence = generation.evidence().orElseThrow();
        var attempt = authority.repository().findAttempt(generation.producingAttempt()).orElseThrow();
        assertThat(attempt.state()).isEqualTo(AttemptState.Published.INSTANCE);
        assertThat(attempt.evidence()).contains(evidence);
        assertThat(evidence.commitFingerprint()).matches("[0-9a-f]{64}");
        assertThat(evidence.manifestHash()).matches("[0-9a-f]{64}");
        try (var iceberg = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
            var physical = iceberg.loadTable(TableIdentifier.of(
                    generation.physicalTable().schema(), generation.physicalTable().table()));
            try (var input = physical.io().newInputFile(
                    physical.currentSnapshot().manifestListLocation()).newStream()) {
                assertThat(evidence.manifestHash()).isEqualTo(java.util.HexFormat.of().formatHex(
                        java.security.MessageDigest.getInstance("SHA-256")
                                .digest(input.readAllBytes())));
            }
        }
    }

    private static void assertIds(String table, Long... expected) {
        List<TypedRow> rows = sqlRunner.execute("SELECT id FROM " + table + " ORDER BY id");
        assertThat(rows).extracting(row -> row.getLong("id")).containsExactly(expected);
    }

    private static void assertPhysicalCommittedRows(String qualifiedTable, long expected) throws Exception {
        String[] parts = qualifiedTable.split("\\.");
        try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
            var physical = catalog.loadTable(TableIdentifier.of(parts[1], parts[2]));
            assertThat(Long.parseLong(physical.currentSnapshot().summary().get("total-records")))
                    .isEqualTo(expected);
        }
    }
}
