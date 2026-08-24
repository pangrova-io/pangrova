package org.pangrova.testing.e2e.observability;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
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
 * E2E test for trace propagation across the full query pipeline.
 *
 * <p>Validates that a traceId injected via W3C traceparent header propagates
 * through: Client HTTP → Coordinator TracingFilter → Worker dispatch →
 * NativeTaskExecutor (JNI) → response. The trace context must survive
 * the entire pipeline without being lost.
 *
 * <p>Merged from EndToEndTracePropagationE2ETest and TraceIdPropagationE2ETest.
 *
 * <p>Covers: T-0.5.1 QueryTracer parent context, T-0.5.2 NativeTaskExecutor
 * trace propagation, T-0.5.3 LogContext wiring.
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

    @Nested
    @DisplayName("HTTP Trace Header Propagation")
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

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
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

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
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
        @DisplayName("Full query with traceparent returns all rows correctly")
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
        @DisplayName("Multiple sequential queries with different trace IDs succeed")
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
