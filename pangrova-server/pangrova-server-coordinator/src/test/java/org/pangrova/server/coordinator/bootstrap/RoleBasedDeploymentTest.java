package org.pangrova.server.coordinator.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.server.coordinator.enums.CoordinatorDeploymentRole;
import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.FlightResultFetcher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.server.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Verifies role-based coordinator deployment: a QUERY-only coordinator
 * starts HTTP + QueryManager but does NOT start indexing or streaming subsystems.
 */
@DisplayName("Role-Based Coordinator Deployment")
class RoleBasedDeploymentTest {
    private static final String PROFILE_LOOKUP = "SELECT%FROM query_audit_records WHERE query_id =%AND user_id =%";
    private static final String JOURNAL_LOOKUP = "SELECT%FROM query_state_journal%";

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null && bootstrap.isStarted()) {
            bootstrap.shutdown();
        }
    }

    @Test
    @DisplayName("QUERY-only coordinator starts HTTP and QueryManager")
    void queryOnlyCoordinatorStartsQueryPath() throws Exception {
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-1", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorDeploymentRole.QUERY, "0.0.0.0"),
                CoordinatorBootstrapConfig.FeatureFlags.allDisabled(),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();

        var request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + bootstrap.getHttpPort() + "/v1/query/submit"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"query\":\"SHOW DYNAMIC TABLES\"}"))
                .build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        var receipt = JsonMapperFactory.create().readValue(response.body(), SubmitQueryResponse.class);
        assertThat(receipt.resultDisposition()).isEqualTo(ResultDisposition.STREAM);
        assertThat(receipt.resultEndpoint()).isNotNull();
        try (var fetcher = new FlightResultFetcher();
             var cursor = fetcher.open(receipt.resultEndpoint(), System.currentTimeMillis() + 5_000)) {
            assertThat(cursor.columns()).extracting(ColumnDescriptor::name).contains("dt_name", "schedule_state");
            assertThat(cursor.next()).isFalse();
        }
        assertProfileReadBudget(receipt.queryId());
    }

    private void assertProfileReadBudget(String queryId) throws Exception {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(
                () -> bootstrap.getQueryManager().getStateMachine(queryId).status().drained());
        try (var connection = bootstrap.metadataDataSource().getConnection();
             var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public");
        }
        long before = postgresCalls(PROFILE_LOOKUP);
        String direct = "/v1/query/profile/" + queryId + "/json";
        String dashboard = "/api/queries/" + queryId + "/profile";
        var client = HttpClient.newHttpClient();
        var local = queryProfile(client.send(getRequest(direct), HttpResponse.BodyHandlers.ofString()));
        assertThat(local.path("queryId").asText()).isEqualTo(queryId);
        assertThat(local.path("sql").asText()).isEqualTo("SHOW DYNAMIC TABLES");
        var requests = IntStream.range(0, 32)
                .mapToObj(index -> client.sendAsync(getRequest(index % 2 == 0 ? direct : dashboard),
                        HttpResponse.BodyHandlers.ofString()))
                .toList();
        for (var request : requests) {
            assertThat(queryProfile(request.get(5, TimeUnit.SECONDS))).isEqualTo(local);
        }
        assertThat(postgresCalls(PROFILE_LOOKUP) - before).isZero();

        assertThat(bootstrap.getQueryManager().cleanupTerminalQueries(Duration.ZERO)).isOne();
        long journalBefore = postgresCalls(JOURNAL_LOOKUP);
        assertThat(client.send(getRequest(direct), HttpResponse.BodyHandlers.discarding()).statusCode())
                .isEqualTo(404);
        assertThat(client.send(getRequest("/v1/query/" + queryId + "/state"),
                HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
        assertThat(postgresCalls(JOURNAL_LOOKUP) - journalBefore).isZero();
        assertThat(postgresCalls(PROFILE_LOOKUP) - before).isZero();
        var history = client.send(getRequest(direct + "?history=true"), HttpResponse.BodyHandlers.ofString());
        assertThat(queryProfile(history)).isEqualTo(local);
        assertThat(postgresCalls(PROFILE_LOOKUP) - before).isOne();
    }

    private HttpRequest getRequest(String path) {
        var uri = URI.create("http://127.0.0.1:" + bootstrap.getHttpPort() + path);
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).GET().build();
    }

    private static JsonNode queryProfile(HttpResponse<String> response) throws Exception {
        assertThat(response.statusCode()).as("%s", response.body()).isEqualTo(200);
        return JsonMapperFactory.create().readTree(response.body());
    }

    private long postgresCalls(String pattern) throws java.sql.SQLException {
        try (var connection = bootstrap.metadataDataSource().getConnection();
             var statement = connection.prepareStatement("""
                     SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                     WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                       AND query LIKE ?
                     """)) {
            statement.setString(1, pattern);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    @Test
    @DisplayName("QUERY-only coordinator does not start indexing or streaming")
    void queryOnlyCoordinatorSkipsMaintenanceSubsystems() {
        // Indexing and streaming require metadata + datasource which we don't provide.
        // A QUERY-only coordinator should NOT attempt to start them even if feature flags
        // are enabled, because role.isMaintenance() returns false.
        CoordinatorBootstrapConfig config = new CoordinatorBootstrapConfig(
                new CoordinatorBootstrapConfig.CoreConfig(
                        "query-coord-2", 0,
                        CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                        CoordinatorDeploymentRole.QUERY, "0.0.0.0"),
                // Enable streaming + indexing flags — but QUERY role should skip them
                new CoordinatorBootstrapConfig.FeatureFlags(
                        false, false, false, true, false, false, false, false,
                        false, false, true),
                CoordinatorBootstrapConfig.QueryConfig.defaults(),
                CoordinatorBootstrapConfig.TransportConfig.defaults(),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                null, null, null);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        // Coordinator starts successfully — proves it didn't try to initialize
        // indexing/streaming subsystems (which would fail without metadata datasource)
        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
    }

    @Test
    @DisplayName("COMBINED role starts both query and maintenance paths")
    void combinedRoleStartsBothPaths() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("combined-1", 0);

        bootstrap = new CoordinatorBootstrap(withAuthority(config));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getQueryManager()).isNotNull();
    }

    @Test
    @DisplayName("CoordinatorDeploymentRole.QUERY returns isQuery=true, isMaintenance=false")
    void queryRolePredicates() {
        assertThat(CoordinatorDeploymentRole.QUERY.isQuery()).isTrue();
        assertThat(CoordinatorDeploymentRole.QUERY.isMaintenance()).isFalse();
    }

    @Test
    @DisplayName("CoordinatorDeploymentRole.MAINTENANCE returns isQuery=false, isMaintenance=true")
    void maintenanceRolePredicates() {
        assertThat(CoordinatorDeploymentRole.MAINTENANCE.isQuery()).isFalse();
        assertThat(CoordinatorDeploymentRole.MAINTENANCE.isMaintenance()).isTrue();
    }

    @Test
    @DisplayName("CoordinatorDeploymentRole.COMBINED returns both true")
    void combinedRolePredicates() {
        assertThat(CoordinatorDeploymentRole.COMBINED.isQuery()).isTrue();
        assertThat(CoordinatorDeploymentRole.COMBINED.isMaintenance()).isTrue();
    }
}
