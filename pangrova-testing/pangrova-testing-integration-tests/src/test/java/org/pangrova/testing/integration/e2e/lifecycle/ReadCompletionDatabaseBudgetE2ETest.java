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

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal("iceberg_catalog", "default", icebergWarehouse().toString()));
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
                endpoint = status.resultEndpoint();
                if ("FINISHED".equals(status.state())) assertThat(endpoint).isNotNull();
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
