package org.pangrova.testing.integration.e2e.read;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for constant queries through the unified pipeline.
 *
 * <p>Covers constant folding (SELECT 1, multi-expression, CAST, boolean equality,
 * string literal, double precision) and expression variety (arithmetic, string
 * operations, NULL handling, COALESCE, boolean logic, nested expressions, type
 * coercion).</p>
 *
 * <p>Constant expressions follow the canonical planner-to-Native task path and
 * are served from a Worker result endpoint. No table data is required.</p>
 *
 * <p>Merged from {@code ConstantFoldingE2ETest} and {@code ConstantExpressionTest}.</p>
 */
@DisplayName("Constant Query E2E — Folding + Expression Variety")
class ConstantQueryE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    // ── SELECT 1 ──

    @Test
    @DisplayName("SQL HTTP entry rejects a request without authenticated identity")
    void unauthenticatedQuerySubmissionIsRejected() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + coordinatorHttpPort()
                        + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"query\":\"SELECT 1\",\"session_properties\":{}}"))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient().send(
                request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    @DisplayName("SELECT 1 returns single row with integer value 1 via transport")
    void selectOneReturnsOneViaTransport() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 1");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isInstanceOf(Number.class);
        assertThat(((Number) firstColumnValue(rows.get(0))).intValue()).isEqualTo(1);
    }

    @Test
    @DisplayName("Native result remains nonterminal until Flight DoGet drains it")
    void nativeResultIsNotFinishedBeforeDoGet() throws Exception {
        var submitted = submitQueryViaHttp("SELECT CAST(42 AS BIGINT)");
        String queryId = submitted.queryId();
        var endpoint = awaitResultEndpoint(queryId, submitted.resultEndpoint());
        String canonicalTaskId = requireNativeResultTaskId(endpoint, queryId, taskTracker);
        String workerTaskId = null;
        boolean finished = false;
        try {
            assertThat(submitted.state()).isEqualTo("RUNNING");
            assertThat(endpoint).isNotNull();
            workerTaskId = awaitWorkerTaskId(canonicalTaskId);
            assertThat(workerTaskId).isNotBlank();

            List<TypedRow> rows = fetchResultsViaTransport(
                    endpoint, queryId);
            assertThat(rows).hasSize(1);
            assertThat(((Number) firstColumnValue(rows.get(0))).longValue())
                    .isEqualTo(42L);

            var terminal = org.pangrova.common.json.JsonMapperFactory.shared()
                    .readTree(pollUntilTerminal(queryId));
            assertThat(terminal.path("state").asText()).isEqualTo("FINISHED");
            finished = true;
        } finally {
            if (finished) {
                releaseNativeTaskAfterTerminal(workerTaskId);
            } else {
                cancelNativeTaskAfterFetch(canonicalTaskId, workerTaskId);
            }
        }
    }

    // ── Multi-expression ──

    @Test
    @DisplayName("SELECT 1+2, 'hello' returns correct types and values via transport")
    void multiExpressionReturnsCorrectTypesAndValues() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 1+2, 'hello'");

        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        List<Object> values = row.columnNames().stream()
                .map(row::getRaw)
                .collect(java.util.stream.Collectors.toList());

        Object numericValue = values.stream()
                .filter(v -> v instanceof Number)
                .findFirst().orElseThrow(() -> new AssertionError("Expected a numeric column"));
        Object stringValue = values.stream()
                .filter(v -> v instanceof String)
                .findFirst().orElseThrow(() -> new AssertionError("Expected a string column"));

        assertThat(((Number) numericValue).intValue()).isEqualTo(3);
        assertThat(stringValue).isEqualTo("hello");
    }

    // ── Type coercion ──

    @Test
    @DisplayName("SELECT CAST(42 AS BIGINT) returns correct type via transport")
    void castToBigintReturnsLongValue() {
        List<TypedRow> rows = sqlRunner.execute("SELECT CAST(42 AS BIGINT)");

        assertThat(rows).hasSize(1);
        Object value = firstColumnValue(rows.get(0));
        assertThat(value).isInstanceOf(Number.class);
        assertThat(((Number) value).longValue()).isEqualTo(42L);
    }

    @Test
    @DisplayName("SELECT CAST(1 AS DOUBLE) returns double type via transport")
    void castIntToDouble() {
        List<TypedRow> rows = sqlRunner.execute("SELECT CAST(1 AS DOUBLE)");

        assertThat(rows).hasSize(1);
        Object value = firstColumnValue(rows.get(0));
        assertThat(value).isInstanceOf(Number.class);
        assertThat(((Number) value).doubleValue()).isEqualTo(1.0);
    }

    // ── Boolean expressions ──

    @Test
    @DisplayName("SELECT 1 = 1 returns true via transport")
    void booleanExpressionReturnsTrue() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 1 = 1");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isEqualTo(true);
    }

    @Test
    @DisplayName("SELECT 1 > 0 AND 2 > 1 returns true via transport")
    void booleanAndExpression() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 1 > 0 AND 2 > 1");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isEqualTo(true);
    }

    // ── Double precision ──

    @Test
    @DisplayName("SELECT CAST(3.14 AS DOUBLE) returns double value via transport")
    void doublePrecisionConstant() {
        List<TypedRow> rows = sqlRunner.execute("SELECT CAST(3.14 AS DOUBLE)");

        assertThat(rows).hasSize(1);
        Object value = firstColumnValue(rows.get(0));
        assertThat(((Number) value).doubleValue()).isEqualTo(3.14);
    }

    // ── String constants ──

    @Test
    @DisplayName("SELECT 'pangrova' returns string value via transport")
    void stringConstantReturnedViaTransport() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 'pangrova'");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isEqualTo("pangrova");
    }

    @Test
    @DisplayName("SELECT 'hello' || ' ' || 'world' returns concatenated string via transport")
    void stringConcatenation() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 'hello' || ' ' || 'world'");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isEqualTo("hello world");
    }

    @Test
    @DisplayName("SELECT UPPER('hello') returns 'HELLO' via transport")
    void upperStringFunction() {
        List<TypedRow> rows = sqlRunner.execute("SELECT UPPER('hello')");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isEqualTo("HELLO");
    }

    // ── Arithmetic ──

    @Test
    @DisplayName("SELECT 10 * 3 + 5 returns 35 via transport")
    void arithmeticMultiplyAndAdd() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 10 * 3 + 5");

        assertThat(rows).hasSize(1);
        assertThat(((Number) firstColumnValue(rows.get(0))).intValue()).isEqualTo(35);
    }

    @Test
    @DisplayName("SELECT (10 + 20) * 3 / 2 returns 45 via transport")
    void nestedArithmeticWithParentheses() {
        List<TypedRow> rows = sqlRunner.execute("SELECT (10 + 20) * 3 / 2");

        assertThat(rows).hasSize(1);
        assertThat(((Number) firstColumnValue(rows.get(0))).intValue()).isEqualTo(45);
    }

    // ── NULL handling ──

    @Test
    @DisplayName("SELECT NULL returns single row with null value via transport")
    void selectNullReturnsNull() {
        List<TypedRow> rows = sqlRunner.execute("SELECT NULL");

        assertThat(rows).hasSize(1);
        assertThat(firstColumnValue(rows.get(0))).isNull();
    }

    @Test
    @DisplayName("SELECT COALESCE(NULL, 42) returns 42 via transport")
    void coalesceSkipsNullAndReturnsFallback() {
        List<TypedRow> rows = sqlRunner.execute("SELECT COALESCE(NULL, 42)");

        assertThat(rows).hasSize(1);
        assertThat(((Number) firstColumnValue(rows.get(0))).intValue()).isEqualTo(42);
    }

    // ── Helper ──

    /** Extracts the raw value of the first column from a TypedRow. */
    private static Object firstColumnValue(TypedRow row) {
        String firstColumn = row.columnNames().iterator().next();
        return row.getRaw(firstColumn);
    }
}
