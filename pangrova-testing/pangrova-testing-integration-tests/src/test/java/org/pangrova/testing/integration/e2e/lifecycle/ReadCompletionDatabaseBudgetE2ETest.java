package org.pangrova.testing.integration.e2e.lifecycle;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Measures PostgreSQL reads from SQL submission through physical drain. */
class ReadCompletionDatabaseBudgetE2ETest extends SqlE2ETestBase {
    private static final String JOURNAL_LOOKUP =
            "SELECT query_id, catalog_name,%FROM write_commit_journal WHERE query_id = $1";
    private static final String RELEASE_JOURNAL_LOOKUP =
            "SELECT EXISTS (%FROM write_commit_journal j%WHERE j.query_id=$1%";
    private static final String ANCHOR_LOOKUP =
            "SELECT owner_id, owner_incarnation, fencing_token,%FROM execution_anchor "
                    + "WHERE execution_id = $1 AND expires_at > clock_timestamp()";
    private static final String PUBLICATION_LOOKUP =
            "SELECT tenant_id,requesting_principal,%FROM materialization.attempts WHERE execution_anchor=$1";
    private static final String READ_ADMISSION_LOOKUP =
            "SELECT control.*, catalog.incarnation_id AS observed_catalog_incarnation%";
    private static final String TABLE_REGISTRATION_LOOKUP =
            "SELECT table_id, catalog_name, schema_name, table_name, %"
                    + "FROM table_control_registry WHERE state <> % AND table_id IN (SELECT table_id "
                    + "FROM table_control_name_reservation%";
    private static final String CATALOG_DEPLOYMENT_LOOKUP =
            "SELECT name, incarnation_id, revision FROM catalog_definitions WHERE name%";
    private static final String ICEBERG_POINTER_LOOKUP =
            "SELECT * FROM iceberg_tables WHERE catalog_name = %AND table_name =%";
    private static final String ICEBERG_TABLE_NAMESPACE_LIST =
            "SELECT DISTINCT table_namespace FROM iceberg_tables WHERE catalog_name = %";
    private static final String ICEBERG_PROPERTY_NAMESPACE_LIST =
            "SELECT DISTINCT namespace FROM iceberg_namespace_properties WHERE catalog_name = %";
    private static final String ICEBERG_TABLE_LIST =
            "SELECT * FROM iceberg_tables WHERE catalog_name = $1 AND table_namespace = $2";
    private static final String ICEBERG_NAMED_NAMESPACE_LOOKUP =
            "SELECT table_namespace FROM iceberg_tables WHERE catalog_name = %"
                    + "AND%table_namespace = %OR table_namespace LIKE %LIMIT %";
    private static final String READ_SUBJECT_LOOKUP =
            "WITH effective AS (%SELECT registered.role_name%LEFT JOIN security_grants grants%";
    private static final String INDEPENDENT_ROLE_LOOKUP =
            "SELECT registered.role_name FROM pangrova_roles registered%";
    private static final String INDEPENDENT_GRANT_LOOKUP =
            "SELECT grant_id, role_name, privilege,%FROM security_grants WHERE role_name = ANY%";
    private static final String READ_POLICIES_LOOKUP =
            "WITH read_target(table_id, catalog_name, schema_name, table_name) AS (%UNION ALL%";
    private static final String INDEPENDENT_ROW_POLICY_LOOKUP =
            "SELECT policy_id, policy_name,%FROM row_policy WHERE enabled%";
    private static final String INDEPENDENT_MASK_LOOKUP =
            "SELECT mask_id, mask_name,%FROM column_mask WHERE catalog_name=%";
    private static final String READ_POLICY_IDENTITY_AND_BINDINGS_LOOKUP =
            "SELECT control.table_id, control.state,%LEFT JOIN table_control_governance_binding%";
    private static final String INDEPENDENT_GOVERNANCE_LOOKUP =
            "SELECT binding_id, table_id, binding_type, binding_ref,%FROM table_control_governance_binding%";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal("iceberg_catalog", "default", icebergWarehouse().toString(),
                Map.of("type", "jdbc", "uri", pgJdbcUrl(), "jdbc.user", pgUsername(),
                        "jdbc.password", pgPassword(), "jdbc.schema-version", "V1")));
    }

    @BeforeAll
    static void installStatementCounters() throws SQLException {
        try (var connection = stateStoreDataSource().getConnection();
                var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
        }
    }

    @Test
    void readCompletionChecksOwnershipWithoutQueryingTheWriteJournal() throws Exception {
        long previousJournalLookups = calls(JOURNAL_LOOKUP);
        long previousReleaseJournalLookups = calls(RELEASE_JOURNAL_LOOKUP);
        long previousOwnershipLookups = calls(ANCHOR_LOOKUP);
        String queryId;
        try (var client = PangrovaRestClient.builder().coordinatorHost("localhost")
                .coordinatorPort(coordinatorHttpPort()).bearerToken(E2E_USER_BEARER_TOKEN)
                .retryPolicy(RetryPolicy.NONE).build()) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            var execution = client.beginQuery("SELECT CAST(42 AS BIGINT) AS answer", Map.of());
            queryId = execution.submission().queryId();
            var endpoint = execution.submission().resultEndpoint();
            while (endpoint == null) {
                var status = execution.awaitStatus(QueryWaitTarget.RESULT_OR_TERMINAL, deadline);
                assertThat(status.state()).isNotIn("FAILED", "CANCELLED");
                if (status.resultDisposition() == org.pangrova.common.enums.ResultDisposition.PENDING) {
                    continue;
                }
                assertThat(status.resultDisposition())
                        .isEqualTo(org.pangrova.common.enums.ResultDisposition.STREAM);
                endpoint = status.resultEndpoint();
            }
            try (var cursor = client.openResult(endpoint, deadline)) {
                assertThat(cursor.next()).isTrue();
                assertThat(((Number) cursor.value(0)).longValue()).isEqualTo(42L);
                assertThat(cursor.next()).isFalse();
            }
            awaitSuccessfulDrain(execution, deadline);
        }
        assertThat(calls(JOURNAL_LOOKUP) - previousJournalLookups).as("write journal SELECTs").isZero();
        assertThat(calls(RELEASE_JOURNAL_LOOKUP) - previousReleaseJournalLookups)
                .as("separate write recovery SELECTs during anchor release").isZero();
        assertThat(calls(ANCHOR_LOOKUP) - previousOwnershipLookups).as("query finalization fence SELECTs").isOne();
        assertExactAttemptProfile(queryId);
        assertProfileIsScopedToTheSubmittingPrincipal(queryId);
        assertQueryLifecycleIsScopedToTheSubmittingPrincipal(queryId);
    }

    @Test
    void managedReadLoadsItsRegistrationAndCatalogTogether() throws SQLException {
        String table = "iceberg_catalog.default.read_admission_database_budget";
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, email VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (42, 'alice@example.com')");
        sqlRunner.executeDdl("ALTER TABLE " + table + " ALTER COLUMN email SET TAG pii_type = 'PII_EMAIL'");

        long previousAdmissionLookups = calls(READ_ADMISSION_LOOKUP);
        long previousRegistrationLookups = calls(TABLE_REGISTRATION_LOOKUP);
        long previousDeploymentLookups = calls(CATALOG_DEPLOYMENT_LOOKUP);
        long previousSubjectLookups = calls(READ_SUBJECT_LOOKUP);
        long previousRoleLookups = calls(INDEPENDENT_ROLE_LOOKUP);
        long previousGrantLookups = calls(INDEPENDENT_GRANT_LOOKUP);
        long previousPolicyLookups = calls(READ_POLICIES_LOOKUP);
        long previousPolicySnapshots = calls(READ_POLICY_SNAPSHOT);
        long previousRowLookups = calls(INDEPENDENT_ROW_POLICY_LOOKUP);
        long previousMaskLookups = calls(INDEPENDENT_MASK_LOOKUP);
        long previousPolicyIdentityLookups = calls(READ_POLICY_IDENTITY_AND_BINDINGS_LOOKUP);
        long previousGovernanceLookups = calls(INDEPENDENT_GOVERNANCE_LOOKUP);
        var rows = sqlRunner.execute("SELECT id, email FROM " + table + " WHERE id = 42");
        assertThat(rows).extracting(row -> row.getLong("id")).containsExactly(42L);
        assertThat(rows).extracting(row -> row.getString("email")).containsExactly("***@***");

        assertThat(calls(READ_ADMISSION_LOOKUP) - previousAdmissionLookups).isOne();
        assertThat(calls(TABLE_REGISTRATION_LOOKUP) - previousRegistrationLookups).isZero();
        assertThat(calls(CATALOG_DEPLOYMENT_LOOKUP) - previousDeploymentLookups).isZero();
        assertThat(calls(READ_SUBJECT_LOOKUP) - previousSubjectLookups).isOne();
        assertThat(calls(INDEPENDENT_ROLE_LOOKUP) - previousRoleLookups).isZero();
        assertThat(calls(INDEPENDENT_GRANT_LOOKUP) - previousGrantLookups).isZero();
        assertThat(calls(READ_POLICY_SNAPSHOT) - previousPolicySnapshots).isOne();
        assertThat(calls(READ_POLICIES_LOOKUP) - previousPolicyLookups).isZero();
        assertThat(calls(INDEPENDENT_ROW_POLICY_LOOKUP) - previousRowLookups).isZero();
        assertThat(calls(INDEPENDENT_MASK_LOOKUP) - previousMaskLookups).isZero();
        assertThat(calls(INDEPENDENT_GOVERNANCE_LOOKUP) - previousGovernanceLookups).isZero();
        assertThat(calls(READ_POLICY_IDENTITY_AND_BINDINGS_LOOKUP) - previousPolicyIdentityLookups).isZero();
    }

    @Test
    void managedJoinBatchesDistinctTablePoliciesAndReusesAliases() throws SQLException {
        String name = "iceberg_catalog.default.policy_reads_";
        int tables = 4;
        for (int index = 0; index < tables; index++) {
            String table = name + index;
            sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, email VARCHAR)");
            sqlRunner.execute("INSERT INTO " + table + " VALUES (" + index + ", 'alice@example.com')");
            sqlRunner.executeDdl("ALTER TABLE " + table + " ALTER COLUMN email SET TAG pii_type = 'PII_EMAIL'");
        }
        var projection = new java.util.StringJoiner(", ");
        var from = new java.util.StringJoiner(" CROSS JOIN ");
        for (int index = 0; index < tables; index++) {
            String alias = "p" + index;
            projection.add(alias + ".id AS id_" + index).add(alias + ".email AS email_" + index);
            from.add(name + index + " " + alias);
        }
        projection.add("q.id AS id_alias").add("q.email AS email_alias");
        from.add(name + "0 q");

        long subject = calls(READ_SUBJECT_LOOKUP);
        long policy = calls(READ_POLICY_SNAPSHOT);
        long admission = calls(READ_ADMISSION_LOOKUP);
        var existingQueries = new java.util.HashSet<>(queryManager.getStateMachines().keySet());
        var rows = sqlRunner.execute("SELECT " + projection + " FROM " + from);
        assertThat(rows).singleElement().satisfies(row -> {
            for (int index = 0; index < tables; index++) {
                assertThat(row.getLong("id_" + index)).isEqualTo((long) index);
                assertThat(row.getString("email_" + index)).isEqualTo("***@***");
            }
            assertThat(row.getLong("id_alias")).isZero();
            assertThat(row.getString("email_alias")).isEqualTo("***@***");
        });
        var issued = new java.util.HashSet<>(queryManager.getStateMachines().keySet());
        issued.removeAll(existingQueries);
        assertThat(issued).hasSize(1);
        String queryId = issued.iterator().next();
        org.awaitility.Awaitility.await("batched-policy query drained").atMost(Duration.ofSeconds(30))
                .ignoreExceptionsInstanceOf(IllegalStateException.class).untilAsserted(() -> {
                    taskTracker.requireQueryExecutionDrained(queryId);
                    assertThat(queryManager.getStateMachines().get(queryId).physicalCleanupApplied()).isTrue();
                });
        long subjectReads = calls(READ_SUBJECT_LOOKUP) - subject;
        long policyReads = calls(READ_POLICY_SNAPSHOT) - policy;
        long admissionReads = calls(READ_ADMISSION_LOOKUP) - admission;
        org.slf4j.LoggerFactory.getLogger(ReadCompletionDatabaseBudgetE2ETest.class).info(
                "event=multi_table_policy_reads distinctTables={} aliases=5 subjectReads={} "
                        + "policyReads={} admissionReads={}",
                tables, subjectReads, policyReads, admissionReads);
        assertThat(subjectReads).isOne();
        assertThat(policyReads).isOne();
        assertThat(admissionReads).isEqualTo(tables);
    }

    @Test
    void exactNamedVersionSharesTableDiscoveryWithReadBinding() throws SQLException {
        String table = "iceberg_catalog.default.named_version_database_budget";
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (42)");
        sqlRunner.executeDdl("ALTER TABLE " + table + " CREATE TAG pinned");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (43)");

        long pointerBefore = calls(ICEBERG_POINTER_LOOKUP);
        long namespaceBefore = calls(ICEBERG_TABLE_NAMESPACE_LIST) + calls(ICEBERG_PROPERTY_NAMESPACE_LIST);
        long tablesBefore = icebergTableListings();
        String named = table + " VERSION AS OF TAG 'pinned'";
        assertThat(sqlRunner.execute("SELECT id FROM " + named))
                .extracting(row -> row.getLong("id")).containsExactly(42L);

        long pointers = calls(ICEBERG_POINTER_LOOKUP) - pointerBefore;
        long namespaces = calls(ICEBERG_TABLE_NAMESPACE_LIST) + calls(ICEBERG_PROPERTY_NAMESPACE_LIST)
                - namespaceBefore;
        long tables = icebergTableListings() - tablesBefore;
        org.slf4j.LoggerFactory.getLogger(ReadCompletionDatabaseBudgetE2ETest.class).info(
                "event=named_version_catalog_reads pointers={} namespaceLists={} tableLists={}",
                pointers, namespaces, tables);
        assertThat(pointers).as("source discovery and live split validation").isEqualTo(2L);
        assertThat(namespaces).isZero();
        assertThat(tables).isZero();

        assertThat(sqlRunner.execute("SELECT a.id AS left_id, b.id AS right_id FROM " + named
                + " a CROSS JOIN " + named + " b")).singleElement().satisfies(row -> {
                    assertThat(row.getLong("left_id")).isEqualTo(42L);
                    assertThat(row.getLong("right_id")).isEqualTo(42L);
                });
        assertThat(sqlRunner.execute("SELECT id FROM iceberg_catalog.DEFAULT.NAMED_VERSION_DATABASE_BUDGET"
                + " VERSION AS OF TAG 'pinned'"))
                .extracting(row -> row.getLong("id")).containsExactly(42L);
        assertThatThrownBy(() -> sqlRunner.execute("SELECT id FROM " + table + " VERSION AS OF TAG 'missing'"))
                .hasMessageContaining("Selected source ref is unavailable: missing");
    }

    private static long icebergTableListings() throws SQLException {
        return calls(ICEBERG_TABLE_LIST) + calls(ICEBERG_TABLE_LIST + " AND (iceberg_type = %");
    }

    @Test
    void qualifiedSourceResolvesWithoutLookingUpItsCatalogNameAsATable() throws SQLException {
        String table = "iceberg_catalog.default.qualified_read_database_budget";
        String sameNameAsCatalog = "iceberg_catalog.default.iceberg_catalog";
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
        sqlRunner.executeDdl("CREATE TABLE " + sameNameAsCatalog + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (42)");
        sqlRunner.execute("INSERT INTO " + sameNameAsCatalog + " VALUES (7)");

        long previousPointerLookups = calls(ICEBERG_POINTER_LOOKUP);
        long previousTableNamespaceListings = calls(ICEBERG_TABLE_NAMESPACE_LIST);
        long previousPropertyNamespaceListings = calls(ICEBERG_PROPERTY_NAMESPACE_LIST);
        long previousNamedNamespaceLookups = calls(ICEBERG_NAMED_NAMESPACE_LOOKUP);
        assertThat(sqlRunner.execute("SELECT id FROM " + table))
                .extracting(row -> row.getLong("id")).containsExactly(42L);
        assertThat(calls(ICEBERG_POINTER_LOOKUP) - previousPointerLookups)
                .as("Iceberg pointer SELECTs during read selection and split planning").isEqualTo(2L);
        assertThat(calls(ICEBERG_TABLE_NAMESPACE_LIST) - previousTableNamespaceListings).isZero();
        assertThat(calls(ICEBERG_PROPERTY_NAMESPACE_LIST) - previousPropertyNamespaceListings).isZero();
        assertThat(calls(ICEBERG_NAMED_NAMESPACE_LOOKUP) - previousNamedNamespaceLookups)
                .as("exact Iceberg namespace SELECTs").isOne();

        assertThat(sqlRunner.execute("SELECT id FROM iceberg_catalog"))
                .extracting(row -> row.getLong("id")).containsExactly(7L);
        assertThat(sqlRunner.execute("SELECT id FROM \"iceberg_catalog\".\"default\".\"qualified_read_database_budget\""))
                .extracting(row -> row.getLong("id")).containsExactly(42L);
        assertThat(sqlRunner.execute("SELECT qualified.id AS qualified_id, local_table.id AS local_id FROM "
                + table + " AS qualified CROSS JOIN iceberg_catalog AS local_table"))
                .singleElement().satisfies(row -> {
                    assertThat(row.getLong("qualified_id")).isEqualTo(42L);
                    assertThat(row.getLong("local_id")).isEqualTo(7L);
                });
    }

    @Test
    void publicInsertPublishesWithoutLookingForAnInternalMaterializationAttempt() throws SQLException {
        String table = "iceberg_catalog.default.completion_database_budget";
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");
        long previousLookups = calls(PUBLICATION_LOOKUP);
        long previousOwnershipLookups = calls(ANCHOR_LOOKUP);
        try (var client = PangrovaRestClient.builder().coordinatorHost("localhost")
                .coordinatorPort(coordinatorHttpPort()).bearerToken(E2E_USER_BEARER_TOKEN)
                .retryPolicy(RetryPolicy.NONE).build()) {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            var execution = client.beginQuery("INSERT INTO " + table + " VALUES (7)",
                    Map.of("pangrova.materialization.internal-execution", "true"));
            awaitSuccessfulDrain(execution, deadline);
        }
        assertThat(calls(PUBLICATION_LOOKUP) - previousLookups).as("materialization attempt SELECTs").isZero();
        assertThat(calls(ANCHOR_LOOKUP) - previousOwnershipLookups).as("distinct write fence SELECTs").isEqualTo(3L);
        assertThat(sqlRunner.execute("SELECT id FROM " + table)).extracting(row -> row.getLong("id"))
                .containsExactly(7L);
    }

    private static void awaitSuccessfulDrain(QueryExecution execution, long deadline) {
        var status = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        while (!Boolean.TRUE.equals(status.drained())) {
            assertThat(status.state()).isNotIn("FAILED", "CANCELLED");
            status = execution.awaitStatus(QueryWaitTarget.DRAINED, deadline);
        }
        assertThat(status.state()).isEqualTo("FINISHED");
    }

    private static void assertProfileIsScopedToTheSubmittingPrincipal(String queryId) throws Exception {
        String sql = "SELECT%FROM query_audit_records WHERE query_id =%";
        long before = calls(sql);
        var http = HttpClient.newHttpClient();
        String base = "http://localhost:" + coordinatorHttpPort();
        for (String route : java.util.List.of("/v1/query/profile/" + queryId + "/json",
                "/api/queries/" + queryId + "/profile")) {
            var request = java.net.http.HttpRequest.newBuilder(URI.create(base + route))
                    .header("Authorization", "Bearer " + E2E_SECOND_USER_BEARER_TOKEN).GET().build();
            var response = http.send(request, HttpResponse.BodyHandlers.discarding());
            assertThat(response.statusCode()).isEqualTo(404);
        }
        assertThat(calls(sql) - before).as("foreign live query profile SELECTs").isZero();
    }

    private static void assertQueryLifecycleIsScopedToTheSubmittingPrincipal(String queryId) throws Exception {
        String journal = "SELECT%FROM query_state_journal%";
        long before = calls(journal);
        var http = HttpClient.newHttpClient();
        String query = "http://localhost:" + coordinatorHttpPort() + "/v1/query/" + queryId;
        String bearer = "Bearer " + E2E_SECOND_USER_BEARER_TOKEN;
        for (String path : java.util.List.of("/state", "/state?afterVersion=0&waitMs=0")) {
            var state = java.net.http.HttpRequest.newBuilder(URI.create(query + path))
                    .header("Authorization", bearer).GET().build();
            assertThat(http.send(state, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
        }
        var cancel = java.net.http.HttpRequest.newBuilder(URI.create(query + "/cancel"))
                .header("Authorization", bearer).POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build();
        assertThat(http.send(cancel, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
        assertThat(calls(journal) - before).as("foreign live query journal SELECTs").isZero();
        var state = http.send(authenticatedHttpRequest(URI.create(query + "/state")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(state.statusCode()).isEqualTo(200);
        assertThat(JsonMapperFactory.shared().readTree(state.body()).path("state").asText()).isEqualTo("FINISHED");
    }

    private static void assertExactAttemptProfile(String queryId) {
        var uri = URI.create("http://localhost:" + coordinatorHttpPort() + "/v1/query/profile/" + queryId + "/json");
        var http = HttpClient.newHttpClient();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String sql = "SELECT%FROM query_audit_records WHERE query_id =%AND user_id =%";
            long before = calls(sql);
            var response = http.send(authenticatedHttpRequest(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(calls(sql) - before).as("owner's scoped profile SELECTs").isOne();
            var body = JsonMapperFactory.shared().readTree(response.body());
            assertThat(body.path("queryId").asText()).isEqualTo(queryId);
            assertThat(body.path("telemetryAvailable").asBoolean()).isTrue();
            assertThat(body.path("attempts").size()).isPositive();
            for (var attempt : body.path("attempts")) {
                assertThat(attempt.path("taskId").asText()).isNotBlank();
                assertThat(attempt.path("workerId").asText()).isNotBlank();
                assertThat(attempt.path("planEpoch").asLong()).isPositive();
                assertThat(attempt.path("operators").size()).isPositive();
            }
            var graph = body.path("executionGraph");
            assertThat(graph.path("stages").size()).isOne();
            assertThat(graph.path("exchanges").size()).isZero();
            assertThat(java.util.stream.StreamSupport.stream(graph.path("transitions").spliterator(), false)
                    .map(transition -> transition.path("state").asText()))
                    .containsSubsequence("SCHEDULING", "RUNNING", "FLUSHING", "FINISHED");
            assertThat(body.has("operatorTree")).isFalse();
            assertThat(body.path("timing").has("ioWaitMs")).isFalse();
        });
    }

    @Test
    void measuresVersionedStatisticsReadsForRepeatedAndDistinctManagedSources() throws SQLException {
        String left = "iceberg_catalog.default.statistics_budget_l";
        String right = "iceberg_catalog.default.statistics_budget_r";
        for (String table : List.of(left, right)) {
            sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, score BIGINT)");
            sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 11), (2, 12)");
        }
        String point = "SELECT id, score FROM " + left + " WHERE id = 1";
        String join = "SELECT l.id, l.score FROM " + left + " l JOIN " + right + " r ON r.id = l.id "
                + "JOIN " + left + " repeated ON repeated.id = l.id WHERE l.id = 1";
        collectStatisticsReads("no_persisted_statistics", point, join);
        for (String table : List.of(left, right)) {
            assertThat(sqlRunner.execute("ANALYZE TABLE " + table)).singleElement()
                    .satisfies(result -> assertThat(result.getLong("row_count")).isEqualTo(2L));
        }
        collectStatisticsReads("analyzed", point, join);
        sqlRunner.execute("INSERT INTO " + left + " VALUES (3, 13)");
        collectStatisticsReads("one_table_advanced", point, join);
    }

    private void collectStatisticsReads(String scenario, String point, String join) throws SQLException {
        String statistics = "SELECT table_id, stats_version, version_token, row_count, data_size, "
                + "source_work_units, column_stats, source, computed_at "
                + "FROM table_control_statistics WHERE table_id=$1";
        String combined = "SELECT target.target_ordinal, catalog.incarnation_id AS observed_incarnation,%"
                + "FROM unnest(%LEFT JOIN table_control_statistics stats%";
        for (String query : List.of(point, join)) {
            long statsBefore = calls(statistics);
            long combinedBefore = calls(combined);
            long admissionBefore = calls(READ_ADMISSION_LOOKUP);
            long catalogBefore = calls(CATALOG_DEPLOYMENT_LOOKUP);
            long pointerBefore = calls(ICEBERG_POINTER_LOOKUP);
            var existing = new java.util.HashSet<>(queryManager.getStateMachines().keySet());
            assertThat(sqlRunner.execute(query)).singleElement().satisfies(result -> {
                assertThat(result.getLong("id")).isOne();
                assertThat(result.getLong("score")).isEqualTo(11L);
            });
            var issued = new java.util.HashSet<>(queryManager.getStateMachines().keySet());
            issued.removeAll(existing);
            assertThat(issued).hasSize(1);
            String queryId = issued.iterator().next();
            await().atMost(Duration.ofSeconds(30)).ignoreExceptionsInstanceOf(IllegalStateException.class)
                    .untilAsserted(() -> {
                        taskTracker.requireQueryExecutionDrained(queryId);
                        assertThat(queryManager.getStateMachines().get(queryId).physicalCleanupApplied()).isTrue();
                    });
            long standaloneStatistics = calls(statistics) - statsBefore;
            long standaloneCatalog = calls(CATALOG_DEPLOYMENT_LOOKUP) - catalogBefore;
            long combinedReads = calls(combined) - combinedBefore;
            boolean joined = query.equals(join);
            org.slf4j.LoggerFactory.getLogger(ReadCompletionDatabaseBudgetE2ETest.class).info(
                    "event=versioned_statistics_read_budget scenario={} join={} statisticsReads={} "
                            + "initialAdmissions={} standaloneCatalogReads={} icebergPointers={} combinedReads={}",
                    scenario, joined, standaloneStatistics, calls(READ_ADMISSION_LOOKUP) - admissionBefore,
                    standaloneCatalog, calls(ICEBERG_POINTER_LOOKUP) - pointerBefore, combinedReads);
            assertThat(standaloneStatistics).isZero();
            assertThat(standaloneCatalog).isZero();
            assertThat(combinedReads).isEqualTo(joined && !scenario.equals("no_persisted_statistics") ? 1 : 0);
        }
    }

    private static long calls(String sqlPattern) throws SQLException {
        try (var connection = stateStoreDataSource().getConnection();
                var statement = connection.prepareStatement("""
                        SELECT COALESCE(SUM(calls), 0)
                        FROM public.pg_stat_statements
                        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                          AND query LIKE ?
                        """)) {
            statement.setString(1, sqlPattern);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }
}
