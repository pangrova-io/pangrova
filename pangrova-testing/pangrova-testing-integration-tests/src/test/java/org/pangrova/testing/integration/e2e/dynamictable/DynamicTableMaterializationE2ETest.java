package org.pangrova.testing.integration.e2e.dynamictable;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.materialization.api.domain.AttemptState;
import org.pangrova.materialization.api.domain.GenerationState;
import org.pangrova.materialization.api.domain.InternalNamespace;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.PublishedMaterialization;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

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
    void publishedGenerationRemainsPinnedUntilPhysicalQueryDrain() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String source = CATALOG + "." + SCHEMA + ".pin_source_" + suffix;
        String logical = CATALOG + "." + SCHEMA + ".pin_rollup_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, "pin_source_" + suffix, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (17)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        PublishedMaterialization published = published(logical);
        Set<String> before = Set.copyOf(queryManager.getStateMachines().keySet());
        var client = java.util.concurrent.Executors.newSingleThreadExecutor();
        org.pangrova.connector.api.query.ConnectorQueryOperation heldOperation = null;
        String queryId = null;
        try (var lock = stateStoreDataSource().getConnection()) {
            lock.setAutoCommit(false);
            int lockPid;
            try (var statement = lock.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                assertThat(rows.next()).isTrue();
                lockPid = rows.getInt(1);
            }
            try (var statement = lock.prepareStatement(
                    "SELECT generation_id FROM materialization.generations WHERE generation_id=? FOR UPDATE")) {
                statement.setObject(1, published.generation().value());
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                }
            }
            var result = client.submit(() -> sqlRunner.execute("SELECT id FROM " + logical));
            // Observe the production pin transaction waiting on our real generation row lock.
            // A compiler using a no-op pinner never reaches this PostgreSQL wait.
            Awaitility.await("published generation pin acquisition").atMost(Duration.ofSeconds(20))
                    .until(() -> pinTransactionBlockedBy(lockPid));
            Set<String> issued = new HashSet<>(queryManager.getStateMachines().keySet());
            issued.removeAll(before);
            assertThat(issued).hasSize(1);
            queryId = issued.iterator().next();
            var state = queryManager.getStateMachines().get(queryId);
            heldOperation = state.connectorAuthority().enterOperation();
            lock.commit();
            assertThat(result.get(30, java.util.concurrent.TimeUnit.SECONDS))
                    .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(17L));
            String admittedQuery = queryId;
            Awaitility.await("logical query completion before physical drain").atMost(Duration.ofSeconds(20))
                    .untilAsserted(() -> assertThat(state.getState())
                            .isEqualTo(org.pangrova.query.api.enums.QueryState.FINISHED));
            assertThat(queryManager.getActiveQueryIds()).contains(admittedQuery);
            assertThat(authority.repository().findReaderPins(published.generation()))
                    .singleElement().satisfies(pin -> {
                        assertThat(pin.query().value()).isEqualTo(admittedQuery);
                        assertThat(pin.owner()).isEqualTo(e2eUserIdentity().principal());
                    });
            heldOperation.close();
            heldOperation = null;
            Awaitility.await("physical drain releases published generation pin")
                    .atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                        assertThat(queryManager.getActiveQueryIds()).doesNotContain(admittedQuery);
                        assertThat(authority.repository().findReaderPins(published.generation())).isEmpty();
                    });
        } finally {
            if (heldOperation != null) heldOperation.close();
            if (queryId != null) queryManager.cancelQuery(queryId);
            client.shutdownNow();
            assertThat(client.awaitTermination(20, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean pinTransactionBlockedBy(int lockPid) throws java.sql.SQLException {
        try (var connection = stateStoreDataSource().getConnection();
             var statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                             + "WHERE ? = ANY(pg_blocking_pids(pid)) "
                             + "AND query LIKE 'SELECT state FROM materialization.generations %')")) {
            statement.setInt(1, lockPid);
            try (var rows = statement.executeQuery()) {
                return rows.next() && rows.getBoolean(1);
            }
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
        assertThat(sqlRunner.execute("SHOW SCHEMAS"))
                .noneMatch(row -> InternalNamespace.SCHEMA_NAME.equals(
                        row.getString("schema_name")));
        assertThat(sqlRunner.executeExpectingError(
                "SHOW TABLES FROM " + InternalNamespace.SCHEMA_NAME)
                .message()).contains("materialization namespace is forbidden");
        assertThat(sqlRunner.executeExpectingError("DESCRIBE " + firstPhysical)
                .message()).contains("materialization namespace is forbidden");

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
    void downstreamRefreshObservesTheNewUpstreamHead() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_chain_source_" + suffix;
        String upstreamTable = "dt_chain_upstream_" + suffix;
        String downstreamTable = "dt_chain_downstream_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String upstream = CATALOG + "." + SCHEMA + "." + upstreamTable;
        String downstream = CATALOG + "." + SCHEMA + "." + downstreamTable;
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + upstream
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + downstream
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + upstream);
        assertIds(upstream, 1L, 2L);
        assertIds(downstream, 1L, 2L);

        assertThat(sqlRunner.executeExpectingError("CREATE DYNAMIC TABLE " + CATALOG + "."
                + SCHEMA + ".dt_chain_incremental_" + suffix
                + " REFRESH_MODE = INCREMENTAL AS SELECT id FROM " + upstream).message())
                .contains("no compatible incremental strategy");

        String upstreamHead = published(upstream).physicalTable().qualifiedName();
        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + upstream + " REFRESH");
        assertIds(upstream, 1L, 2L, 3L);
        assertThat(published(upstream).physicalTable().qualifiedName())
                .isNotEqualTo(upstreamHead);

        assertIds(downstream, 1L, 2L);
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + downstream + " REFRESH");
        assertIds(downstream, 1L, 2L, 3L);
    }

    @Test
    void incrementalAndFullRefreshesAgreeOnTheSameSource() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_oracle_source_" + suffix;
        String fullTable = "dt_oracle_full_" + suffix;
        String incrementalTable = "dt_oracle_incremental_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String full = CATALOG + "." + SCHEMA + "." + fullTable;
        String incremental = CATALOG + "." + SCHEMA + "." + incrementalTable;
        String definition = " AS SELECT id FROM " + source + " WHERE id % 2 = 1";
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + full
                + " REFRESH_MODE = SNAPSHOT" + definition);
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + incremental
                + " REFRESH_MODE = INCREMENTAL" + definition);
        assertIds(full, 1L);
        assertIds(incremental, 1L);

        for (String delta : new String[] {"(3), (4)", "(5), (6)"}) {
            sqlRunner.execute("INSERT INTO " + source + " VALUES " + delta);
            sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + full + " REFRESH");
            sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + incremental + " REFRESH");
            assertThat(published(incremental).refreshKind())
                    .isEqualTo(org.pangrova.materialization.api.domain.RefreshKind
                            .IncrementalAppendOnly.INSTANCE);
            assertThat(sqlRunner.execute("SELECT id FROM " + incremental + " ORDER BY id"))
                    .extracting(row -> row.getLong("id"))
                    .containsExactlyElementsOf(sqlRunner.execute(
                            "SELECT id FROM " + full + " ORDER BY id").stream()
                            .map(row -> row.getLong("id")).toList());
        }
        assertIds(full, 1L, 3L, 5L);
        assertIds(incremental, 1L, 3L, 5L);
    }

    @Test
    void partitionReplacementRefreshRecomputesOnlyChangedPartitions() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_pr_source_" + suffix;
        String scanTable = "dt_pr_scan_" + suffix;
        String aggregateTable = "dt_pr_agg_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String scan = CATALOG + "." + SCHEMA + "." + scanTable;
        String aggregate = CATALOG + "." + SCHEMA + "." + aggregateTable;
        sqlRunner.executeDdl("CREATE TABLE " + source
                + " (id BIGINT, region VARCHAR, amount DOUBLE) PARTITIONED BY (region)");
        sqlRunner.execute("INSERT INTO " + source
                + " VALUES (1, 'eu', 10.0), (2, 'us', 20.0)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + scan
                + " REFRESH_MODE = INCREMENTAL AS SELECT id, region, amount FROM " + source);
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + aggregate
                + " REFRESH_MODE = INCREMENTAL AS SELECT region, SUM(amount) AS total FROM "
                + source + " GROUP BY region");
        assertThat(published(scan).refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(published(aggregate).refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        String scanHead = published(scan).physicalTable().qualifiedName();
        String aggregateHead = published(aggregate).physicalTable().qualifiedName();

        // A delete and an insert in the same partition: append-only cannot prove
        // this interval, partition replacement can.
        sqlRunner.execute("DELETE FROM " + source + " WHERE id = 1");
        assertThat(sqlRunner.execute("SELECT id FROM " + source + " ORDER BY id"))
                .extracting(row -> row.getLong("id"))
                .containsExactly(2L);
        sqlRunner.execute("INSERT INTO " + source + " VALUES (3, 'eu', 30.0)");
        assertThat(sqlRunner.execute("SELECT id FROM " + source + " ORDER BY id"))
                .extracting(row -> row.getLong("id"))
                .containsExactly(2L, 3L);
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + scan + " REFRESH");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + aggregate + " REFRESH");

        assertThat(published(scan).refreshKind())
                .isEqualTo(RefreshKind.IncrementalPartitionReplace.INSTANCE);
        assertThat(published(aggregate).refreshKind())
                .isEqualTo(RefreshKind.IncrementalPartitionReplace.INSTANCE);
        assertThat(published(scan).physicalTable().qualifiedName()).isNotEqualTo(scanHead);
        assertThat(published(aggregate).physicalTable().qualifiedName())
                .isNotEqualTo(aggregateHead);
        assertThat(sqlRunner.execute("SELECT id, region, amount FROM " + scan + " ORDER BY id"))
                .extracting(row -> row.getLong("id") + ":" + row.getString("region")
                        + ":" + row.getDouble("amount"))
                .containsExactly("2:us:20.0", "3:eu:30.0");
        assertThat(sqlRunner.execute("SELECT region, total FROM " + aggregate + " ORDER BY region"))
                .extracting(row -> row.getString("region") + ":" + row.getDouble("total"))
                .containsExactly("eu:30.0", "us:20.0");
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
    void ddlChangesFollowTheKernelContract() throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        String sourceTable = "dt_ddl_source_" + suffix;
        String logicalTable = "dt_ddl_rollup_" + suffix;
        String source = CATALOG + "." + SCHEMA + "." + sourceTable;
        String logical = CATALOG + "." + SCHEMA + "." + logicalTable;
        String renamed = CATALOG + "." + SCHEMA + "." + logicalTable + "_v2";
        dataSetup.createTestTable(CATALOG, SCHEMA, sourceTable, "id BIGINT");
        sqlRunner.execute("INSERT INTO " + source + " VALUES (1), (2)");
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + logical
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + source);
        PublishedMaterialization first = published(logical);
        assertThat(header(logical).definitionRevision()).isEqualTo(1);
        assertIds(logical, 1L, 2L);

        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " SET TARGET_LAG = 'PT2M'");
        List<TypedRow> shown = sqlRunner.execute("SHOW DYNAMIC TABLES");
        assertThat(shown).anySatisfy(row -> {
            assertThat(row.getString("dt_name")).isEqualTo(logical);
            assertThat(row.getString("refresh_mode")).isEqualTo("FULL");
            assertThat(row.getString("schedule_state")).isEqualTo("ACTIVE");
            assertThat(row.getString("target_lag")).isEqualTo("PT2M");
        });

        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " SET REFRESH_MODE = INCREMENTAL");
        PublishedMaterialization reconfigured = published(logical);
        assertThat(reconfigured.refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(reconfigured.physicalTable().qualifiedName())
                .isNotEqualTo(first.physicalTable().qualifiedName());
        assertThat(header(logical).definitionRevision()).isEqualTo(1);

        sqlRunner.execute("INSERT INTO " + source + " VALUES (3)");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " REFRESH");
        var appended = authority.repository().findPublishedGeneration(id(logical)).orElseThrow();
        assertThat(appended.refreshKind()).isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
        assertIds(logical, 1L, 2L, 3L);

        String headBeforeRename = published(logical).physicalTable().qualifiedName();
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + logical + " RENAME TO " + renamed);
        assertThat(published(renamed).physicalTable().qualifiedName()).isEqualTo(headBeforeRename);
        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + logical).message())
                .isNotBlank();
        assertIds(renamed, 1L, 2L, 3L);

        sqlRunner.executeDdl("DROP DYNAMIC TABLE " + renamed);
        assertThat(authority.catalog().findPublished(id(renamed))).isEmpty();
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
        String privateName = CATALOG + "." + SCHEMA + ".dt_private_" + suffix;
        sqlRunner.executeDdl("CREATE DYNAMIC TABLE " + privateName
                + " REFRESH_MODE = SNAPSHOT AS SELECT id FROM " + logical);
        assertThat(sqlRunner.execute("SHOW MATERIALIZATION DEPENDENCIES FOR " + logical))
                .anyMatch(row -> privateName.equals(row.getString("downstream")));

        String role = "dt_scoped_" + suffix;
        var roles = new JdbcRoleStore(stateStoreDataSource());
        roles.createRole(Role.of(role));
        roles.grantRoleToUser(UserRoleAssignment.of(authenticatedCatalogSession(CATALOG).getUser(), role));
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

        assertThat(sqlRunner.execute("SHOW MATERIALIZATIONS"))
                .extracting(row -> row.getString("name")).contains(logical).doesNotContain(privateName);
        assertThat(sqlRunner.execute("SHOW MATERIALIZATION REFRESH HISTORY"))
                .extracting(row -> row.getString("object")).contains(logical).doesNotContain(privateName);
        assertThat(sqlRunner.execute("SHOW MATERIALIZATION DEPENDENCIES FOR " + logical))
                .noneMatch(row -> privateName.equals(row.getString("downstream")));
        for (String surface : List.of("REFRESH HISTORY", "GC HISTORY", "DEPENDENCIES")) {
            assertThat(sqlRunner.executeExpectingError("SHOW MATERIALIZATION " + surface + " FOR " + privateName)
                    .message()).contains("SECURITY-0002");
        }

        assertThat(sqlRunner.executeExpectingError("ALTER DYNAMIC TABLE " + logical + " REFRESH")
                .message()).contains("HTTP 403", "SECURITY-0002");
        assertThat(published(logical).physicalTable().qualifiedName()).isEqualTo(originalHead);
        assertIds(logical, 1L, 2L);

        String anotherLogical = CATALOG + "." + SCHEMA + ".dt_new_" + suffix;
        grants.addGrant(PrivilegeGrant.of(role, Privilege.CREATE_TABLE,
                "TABLE", CATALOG, SCHEMA, "dt_new_" + suffix, null));
        grants.addGrant(PrivilegeGrant.of(role, Privilege.SELECT,
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
