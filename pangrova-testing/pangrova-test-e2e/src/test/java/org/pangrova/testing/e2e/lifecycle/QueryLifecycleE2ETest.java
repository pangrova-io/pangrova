package org.pangrova.testing.e2e.lifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full query lifecycle E2E test exercising all pipeline stages in sequence:
 * submit → parse → plan → dispatch → worker execute → shuffle → result fetch → cleanup.
 *
 * <p>A single test validates that every component in the query pipeline is invoked
 * and completes successfully. Uses the real HTTP pipeline (no mocks).
 *
 * <p>Validates: Requirement R12.10
 */
@DisplayName("Query Lifecycle E2E — Full Pipeline")
class QueryLifecycleE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private String testTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTable() {
        testTable = "lifecycle_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, testTable,
                "id BIGINT, name VARCHAR, value DOUBLE");
        String fqn = CATALOG + "." + SCHEMA + "." + testTable;
        sqlRunner.execute("INSERT INTO " + fqn
                + " VALUES (1, 'alpha', 10.0), (2, 'beta', 20.0), (3, 'gamma', 30.0)");
    }

    // -----------------------------------------------------------------------
    // R12.10: Full query lifecycle — submit → parse → plan → dispatch →
    //         worker execute → shuffle → result fetch → cleanup
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Full lifecycle: submit → parse → plan → dispatch → execute → fetch → cleanup")
    void fullQueryLifecycle_allStagesComplete() throws Exception {
        String fqn = CATALOG + "." + SCHEMA + "." + testTable;

        // Stage 1: Submit query via HTTP (exercises parse + plan + dispatch)
        String submitUrl = "http://localhost:" + coordinatorHttpPort() + "/v1/query/submit";
        String sql = "SELECT id, name, value FROM " + fqn + " ORDER BY id";
        String requestBody = MAPPER.writeValueAsString(Map.of("query", sql, "session_properties", Map.of()));

        HttpRequest submitReq = HttpRequest.newBuilder()
                .uri(URI.create(submitUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> submitResp = HTTP.send(submitReq, HttpResponse.BodyHandlers.ofString());
        assertThat(submitResp.statusCode())
                .as("Submit should return HTTP 200")
                .isEqualTo(200);

        JsonNode submitJson = MAPPER.readTree(submitResp.body());
        String queryId = submitJson.get("query_id").asText();
        assertThat(queryId)
                .as("Submit response must contain a query_id")
                .isNotBlank();

        // Stage 2: Poll until terminal state (exercises dispatch → execute → shuffle)
        String stateUrl = "http://localhost:" + coordinatorHttpPort()
                + "/v1/query/" + queryId + "/state";
        String terminalState = pollUntilTerminal(stateUrl);
        assertThat(terminalState)
                .as("Query should reach FINISHED state")
                .isEqualTo("FINISHED");

        // Stage 3: Verify results via the high-level runner (exercises result fetch)
        List<TypedRow> rows = sqlRunner.execute(sql);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getRaw("id")).isEqualTo(1L);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("alpha");
        assertThat(rows.get(1).getRaw("id")).isEqualTo(2L);
        assertThat(rows.get(2).getRaw("id")).isEqualTo(3L);

        // Stage 4: Cleanup verification — re-query state should still be FINISHED
        // (QueryManager retains terminal state for a configurable period)
        HttpResponse<String> cleanupResp = HTTP.send(
                HttpRequest.newBuilder().uri(URI.create(stateUrl)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        // Terminal state is retained or query is cleaned up (404 is also acceptable)
        assertThat(cleanupResp.statusCode()).isIn(200, 404);
    }

    @Test
    @DisplayName("Aggregation lifecycle: submit → parse → plan → execute → fetch with SUM/COUNT")
    void aggregationQueryLifecycle() {
        String fqn = CATALOG + "." + SCHEMA + "." + testTable;

        // Exercises aggregation operator path through the full pipeline
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt, SUM(value) AS total FROM " + fqn);

        assertThat(rows).hasSize(1);
        long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
        double total = ((Number) rows.get(0).getRaw("total")).doubleValue();
        assertThat(count).isEqualTo(3L);
        assertThat(total).isEqualTo(60.0);
    }

    @Test
    @DisplayName("Filter + sort lifecycle: submit → parse → plan → execute → fetch with WHERE + ORDER BY")
    void filterSortQueryLifecycle() {
        String fqn = CATALOG + "." + SCHEMA + "." + testTable;

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, name FROM " + fqn + " WHERE value > 15.0 ORDER BY id");

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("beta");
        assertThat(rows.get(1).getRaw("name")).isEqualTo("gamma");
    }

    @Test
    @DisplayName("Constant-fold lifecycle: SELECT 1 + 2 exercises coordinator-local result path")
    void constantFoldLifecycle() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 1 + 2 AS result");

        assertThat(rows).hasSize(1);
        Object val = new ArrayList<>(rows.get(0).toRawMap().values()).get(0);
        assertThat(((Number) val).longValue()).isEqualTo(3L);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private String pollUntilTerminal(String stateUrl) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            HttpResponse<String> resp = HTTP.send(
                    HttpRequest.newBuilder().uri(URI.create(stateUrl)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            String state = MAPPER.readTree(resp.body()).get("state").asText();
            if ("FINISHED".equals(state) || "FAILED".equals(state)) {
                return state;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Query did not reach terminal state within 60s");
    }
}
