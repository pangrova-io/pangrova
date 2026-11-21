package org.pangrova.testing.integration.e2e.observability;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Native SQL execution with disabled telemetry and valid execution correlation.
 * The canonical-request test verifies generated identities at real dispatch and
 * consumes actual Native results. HTTP header smoke cases do not prove parent
 * propagation: this fixture selects NoOp tracing. SDK parent/activation and JNI
 * boundary tests cover their respective segments; no exported-span claim is made.
 */
@DisplayName("Trace Propagation E2E")
class TracePropagationE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private String testTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTable() {
        testTable = "trace_e2e_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, testTable,
                "id BIGINT, name VARCHAR, value DOUBLE");
        String fqn = CATALOG + "." + SCHEMA + "." + testTable;
        sqlRunner.execute("INSERT INTO " + fqn
                + " VALUES (1, 'alpha', 10.0), (2, 'beta', 20.0), (3, 'gamma', 30.0)");
    }

    @Test
    void disabledTelemetryStillIssuesDistinctValidNativeExecutionIdentities() throws Exception {
        var traces = new java.util.HashSet<String>();
        for (int iteration = 0; iteration < 2; iteration++) {
            var submitted = submitQueryViaHttp("SELECT CAST(1 AS BIGINT) AS trace_probe");
            try {
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(30))
                        .until(() -> taskTracker.getTaskIds(submitted.queryId()).stream()
                                .anyMatch(id -> taskTracker.getTaskRequest(id) != null));
                var requests = taskTracker.getTaskIds(submitted.queryId()).stream()
                        .map(taskTracker::getTaskRequest).filter(java.util.Objects::nonNull).toList();
                assertThat(requests).isNotEmpty();
                var firstTrace = requests.get(0).taskRequest().getTraceContext().getTraceId();
                assertThat(traces.add(firstTrace)).as("different query roots").isTrue();
                for (var request : requests) {
                    var trace = request.taskRequest().getTraceContext();
                    assertThat(trace.getTraceId()).matches("[0-9a-f]{32}")
                            .isNotEqualTo("0".repeat(32)).isEqualTo(firstTrace);
                    assertThat(trace.getSpanId()).matches("[0-9a-f]{16}")
                            .isNotEqualTo("0".repeat(16));
                    assertThat(trace.getTraceFlags()).isZero();
                }
                assertThat(consumeSubmittedQuery(submitted)).singleElement()
                        .satisfies(row -> assertThat(row.getLong("trace_probe")).isEqualTo(1));
            } finally {
                queryManager.cancelQuery(submitted.queryId());
            }
        }
    }

    @Nested
    @DisplayName("HTTP Trace Header Compatibility")
    class HttpTraceHeaderTest {

        @Test
        @DisplayName("Query with traceparent header returns correct results")
        void queryWithTraceparent_returnsCorrectResults() throws Exception {
            String traceparent = generateW3CTraceparent();
            String url = "http://localhost:" + coordinatorHttpPort()
                    + "/v1/query/submit";
            String body = """
                    {"query":"SELECT id, name FROM %s.%s.%s ORDER BY id","session_properties":{}}"""
                    .formatted(CATALOG, SCHEMA, testTable);

            HttpRequest request = authenticatedHttpRequest(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("traceparent", traceparent)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = HTTP.send(request,
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("query_id");
        }

        @Test
        @DisplayName("Query without traceparent still succeeds (auto-generated trace)")
        void queryWithoutTraceparent_stillSucceeds() throws Exception {
            String url = "http://localhost:" + coordinatorHttpPort()
                    + "/v1/query/submit";
            String body = """
                    {"query":"SELECT 1 AS one","session_properties":{}}""";

            HttpRequest request = authenticatedHttpRequest(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = HTTP.send(request,
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
        }

        @Test
        @DisplayName("Health endpoint accessible with trace context")
        void healthEndpointWithTraceContext() throws Exception {
            String traceparent = generateW3CTraceparent();
            String url = "http://localhost:" + coordinatorHttpPort()
                    + "/v1/health";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("traceparent", traceparent)
                    .GET()
                    .build();

            HttpResponse<String> response = HTTP.send(request,
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isIn(200, 503);
        }
    }

    @Nested
    @DisplayName("Query Execution with Trace Context")
    class QueryExecutionTraceTest {

        @Test
        @DisplayName("Full pipeline query with trace context returns correct data (SUM)")
        void fullPipelineQuery_withTraceContext_returnsCorrectData() {
            String fqn = CATALOG + "." + SCHEMA + "." + testTable;

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT SUM(value) AS total FROM " + fqn);

            assertThat(rows).hasSize(1);
            double total = ((Number) rows.get(0).getRaw("total")).doubleValue();
            assertThat(total).isEqualTo(60.0);
        }

        @Test
        @DisplayName("Native query returns all rows with disabled telemetry")
        void fullQueryWithTraceparentReturnsResults() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, value FROM " + CATALOG + "." + SCHEMA
                            + "." + testTable + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(1L);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("beta");
            assertThat(rows.get(2).getRaw("value")).isEqualTo(30.0);
        }

        @Test
        @DisplayName("Sequential Native aggregates succeed with disabled telemetry")
        void multipleQueriesWithDifferentTraceIds() {
            String fqn = CATALOG + "." + SCHEMA + "." + testTable;

            List<TypedRow> r1 = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + fqn);
            List<TypedRow> r2 = sqlRunner.execute(
                    "SELECT SUM(value) AS total FROM " + fqn);

            assertThat(r1).hasSize(1);
            assertThat(((Number) ColumnValue.toRaw(r1.get(0).get("cnt"))).longValue())
                    .isEqualTo(3L);
            assertThat(r2).hasSize(1);
            assertThat(((Number) ColumnValue.toRaw(r2.get(0).get("total"))).doubleValue())
                    .isEqualTo(60.0);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Generates a valid W3C traceparent header value.
     * Format: version-traceId-parentId-flags (00-{32hex}-{16hex}-01)
     */
    private static String generateW3CTraceparent() {
        String traceId = UUID.randomUUID().toString().replace("-", "");
        String parentId = UUID.randomUUID().toString().replace("-", "")
                .substring(0, 16);
        return "00-" + traceId + "-" + parentId + "-01";
    }
}
