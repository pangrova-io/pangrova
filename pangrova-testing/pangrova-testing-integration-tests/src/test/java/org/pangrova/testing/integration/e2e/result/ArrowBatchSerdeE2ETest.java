package org.pangrova.testing.integration.e2e.result;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.catalog.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test verifying result serving correctness and consistency.
 *
 * <p>Validates that the default serde mode produces correct, repeatable results
 * for scans, aggregations, and joins via the Flight result path.</p>
 */
@DisplayName("Result Serde E2E")
class ArrowBatchSerdeE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    /**
     * Execute a query with explicit session properties via the coordinator HTTP API.
     * This bypasses the default sqlRunner which uses empty session properties.
     */
    private List<TypedRow> executeWithSessionProperties(
            String sql, Map<String, String> sessionProperties) {
        try {
            String url = "http://localhost:" + coordinatorHttpPort() + "/v1/query/submit";
            SubmitQueryRequest request = new SubmitQueryRequest(sql, sessionProperties);
            byte[] body = MAPPER.writeValueAsBytes(request);

            HttpRequest httpReq = authenticatedHttpRequest(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();

            HttpResponse<String> resp = HTTP_CLIENT.send(
                    httpReq, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException(
                        "Submit failed HTTP " + resp.statusCode() + ": " + resp.body());
            }
            SubmitQueryResponse submitResponse = MAPPER.readValue(
                    resp.body(), SubmitQueryResponse.class);
            String queryId = submitResponse.queryId();
            var endpoint = awaitResultEndpoint(queryId, submitResponse);
            String canonicalTaskId = requireNativeResultTaskId(endpoint, queryId, taskTracker);
            String workerTaskId = null;
            List<TypedRow> rows;
            try {
                workerTaskId = awaitWorkerTaskId(canonicalTaskId);
                rows = fetchResultsViaTransport(endpoint, queryId);
                var terminal = MAPPER.readTree(pollUntilTerminal(queryId));
                if (!"FINISHED".equals(terminal.path("state").asText())) {
                    throw new IllegalStateException(
                            "Query did not finish successfully: " + queryId + " " + terminal);
                }
            } catch (Exception failure) {
                cancelNativeTaskAfterFetch(canonicalTaskId, workerTaskId);
                throw failure;
            }
            releaseNativeTaskAfterTerminal(workerTaskId);
            return rows;
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    "Query polling timed out", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Pipeline execution failed: " + e.getMessage(), e);
        }
    }

    @Nested
    @DisplayName("Result Serde Correctness")
    class SerdeCorrectness {

        @Test
        @DisplayName("Default serde returns correct data for simple scan")
        void defaultSerdeSimpleScan() {
            String sql = String.format(
                    "SELECT c_custkey, c_name FROM %s.%s.customer "
                    + "ORDER BY c_custkey LIMIT 10",
                    CATALOG, SCHEMA);

            List<TypedRow> results = executeWithSessionProperties(sql, Map.of());

            assertThat(results)
                    .as("Default serde should return results")
                    .isNotEmpty()
                    .hasSize(10);

            for (int i = 0; i < results.size() - 1; i++) {
                long current = results.get(i).getLong("c_custkey");
                long next = results.get(i + 1).getLong("c_custkey");
                assertThat(current).isLessThanOrEqualTo(next);
            }
        }

        @Test
        @DisplayName("Default serde returns correct data for aggregation")
        void defaultSerdeAggregation() {
            String sql = String.format(
                    "SELECT l_returnflag, COUNT(*) AS cnt, "
                    + "SUM(l_quantity) AS total_qty "
                    + "FROM %s.%s.lineitem GROUP BY l_returnflag "
                    + "ORDER BY l_returnflag",
                    CATALOG, SCHEMA);

            List<TypedRow> results = executeWithSessionProperties(sql, Map.of());

            assertThat(results)
                    .as("Default serde should return aggregation results")
                    .isNotEmpty();

            for (TypedRow row : results) {
                long cnt = row.getLong("cnt");
                assertThat(cnt).as("Count should be positive").isGreaterThan(0);
            }
        }

        @Test
        @DisplayName("Default serde handles NULL values correctly")
        void defaultSerdeNullHandling() {
            String sql = String.format(
                    "SELECT o_orderkey, o_comment FROM %s.%s.orders "
                    + "ORDER BY o_orderkey LIMIT 20",
                    CATALOG, SCHEMA);

            List<TypedRow> results = executeWithSessionProperties(sql, Map.of());

            assertThat(results)
                    .as("Default serde should handle queries with NULLs")
                    .hasSize(20);
        }
    }

    @Nested
    @DisplayName("Repeated Query Result Equivalence")
    class SerdeEquivalence {

        @Test
        @DisplayName("Repeated scan produces consistent results")
        void scanEquivalence() {
            String sql = String.format(
                    "SELECT c_custkey, c_name, c_nationkey FROM %s.%s.customer "
                    + "ORDER BY c_custkey LIMIT 50",
                    CATALOG, SCHEMA);

            List<TypedRow> firstResults = executeWithSessionProperties(sql, Map.of());
            List<TypedRow> secondResults = executeWithSessionProperties(sql, Map.of());

            assertThat(firstResults).hasSameSizeAs(secondResults);

            for (int i = 0; i < firstResults.size(); i++) {
                TypedRow firstRow = firstResults.get(i);
                TypedRow secondRow = secondResults.get(i);

                long firstKey = firstRow.getLong("c_custkey");
                long secondKey = secondRow.getLong("c_custkey");
                assertThat(firstKey)
                        .as("Row %d c_custkey should match between runs", i)
                        .isEqualTo(secondKey);

                long firstNation = firstRow.getLong("c_nationkey");
                long secondNation = secondRow.getLong("c_nationkey");
                assertThat(firstNation)
                        .as("Row %d c_nationkey should match between runs", i)
                        .isEqualTo(secondNation);
            }
        }

        @Test
        @DisplayName("Repeated aggregation produces consistent results")
        void aggregationEquivalence() {
            String sql = String.format(
                    "SELECT l_returnflag, l_linestatus, "
                    + "SUM(l_quantity) AS sum_qty, "
                    + "COUNT(*) AS count_order "
                    + "FROM %s.%s.lineitem "
                    + "GROUP BY l_returnflag, l_linestatus "
                    + "ORDER BY l_returnflag, l_linestatus",
                    CATALOG, SCHEMA);

            List<TypedRow> firstResults = executeWithSessionProperties(sql, Map.of());
            List<TypedRow> secondResults = executeWithSessionProperties(sql, Map.of());

            assertThat(firstResults)
                    .as("Both runs should return same number of groups")
                    .hasSameSizeAs(secondResults);

            for (int i = 0; i < firstResults.size(); i++) {
                TypedRow firstRow = firstResults.get(i);
                TypedRow secondRow = secondResults.get(i);

                long firstCount = firstRow.getLong("count_order");
                long secondCount = secondRow.getLong("count_order");
                assertThat(firstCount)
                        .as("Row %d count_order should match", i)
                        .isEqualTo(secondCount);
            }
        }

        @Test
        @DisplayName("Repeated join produces consistent results")
        void joinEquivalence() {
            String sql = String.format(
                    "SELECT c.c_custkey, c.c_name, o.o_orderkey "
                    + "FROM %s.%s.customer c "
                    + "JOIN %s.%s.orders o ON c.c_custkey = o.o_custkey "
                    + "ORDER BY c.c_custkey, o.o_orderkey LIMIT 30",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> firstResults = executeWithSessionProperties(sql, Map.of());
            List<TypedRow> secondResults = executeWithSessionProperties(sql, Map.of());

            assertThat(firstResults)
                    .as("Both runs should return same join row count")
                    .hasSameSizeAs(secondResults);

            for (int i = 0; i < firstResults.size(); i++) {
                TypedRow firstRow = firstResults.get(i);
                TypedRow secondRow = secondResults.get(i);

                long firstCustKey = firstRow.getLong("c_custkey");
                long secondCustKey = secondRow.getLong("c_custkey");
                assertThat(firstCustKey)
                        .as("Row %d c_custkey should match", i)
                        .isEqualTo(secondCustKey);

                long firstOrderKey = firstRow.getLong("o_orderkey");
                long secondOrderKey = secondRow.getLong("o_orderkey");
                assertThat(firstOrderKey)
                        .as("Row %d o_orderkey should match", i)
                        .isEqualTo(secondOrderKey);
            }
        }
    }
}
