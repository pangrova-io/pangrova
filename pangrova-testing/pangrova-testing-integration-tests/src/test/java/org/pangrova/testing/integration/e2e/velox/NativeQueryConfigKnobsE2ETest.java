package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.client.protocol.SubmitQueryRequest;
import org.pangrova.client.protocol.SubmitQueryResponse;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.TestConnectorFactory;
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
 * E2E test verifying QueryConfig performance knobs correctness.
 *
 * <p>Runs the same queries with performance knobs enabled (default) and disabled
 * (via session properties), verifying identical results. This validates that
 * enabling the knobs does not change query result correctness.
 *
 * <p>Validates: Requirements 6.1-6.6</p>
 */
@DisplayName("Native QueryConfig Performance Knobs E2E")
class NativeQueryConfigKnobsE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    /**
     * Session properties that disable all performance knobs.
     * Used as the "baseline" configuration to compare against the default.
     */
    private static final Map<String, String> KNOBS_DISABLED = Map.of(
            "velox.hash_probe_finish_early_on_empty_build", "false",
            "velox.hash_probe_string_dynamic_filter_pushdown", "false",
            "velox.adaptive_filter_reordering", "false",
            "velox.parallel_output_join_build_rows", "false",
            "velox.table_scan_scaled_processing", "false",
            "velox.max_split_preload_per_driver", "0"
    );

    /** Session properties with all knobs explicitly enabled. */
    private static final Map<String, String> KNOBS_ENABLED = Map.of(
            "velox.hash_probe_finish_early_on_empty_build", "true",
            "velox.hash_probe_string_dynamic_filter_pushdown", "true",
            "velox.adaptive_filter_reordering", "true",
            "velox.parallel_output_join_build_rows", "true",
            "velox.table_scan_scaled_processing", "true",
            "velox.max_split_preload_per_driver", "2"
    );

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

    private List<TypedRow> executeWithSessionProperties(
            String sql, Map<String, String> sessionProperties) {
        try {
            String url = "http://localhost:" + coordinatorHttpPort()
                    + "/v1/query/submit";
            SubmitQueryRequest request = new SubmitQueryRequest(
                    sql, sessionProperties);
            byte[] body = MAPPER.writeValueAsBytes(request);

            HttpRequest httpReq = authenticatedHttpRequest(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();

            HttpResponse<String> resp = HTTP_CLIENT.send(
                    httpReq, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException(
                        "Submit failed HTTP " + resp.statusCode()
                        + ": " + resp.body());
            }
            SubmitQueryResponse submitResponse = MAPPER.readValue(
                    resp.body(), SubmitQueryResponse.class);
            String queryId = submitResponse.queryId();
            var endpoint = awaitResultEndpoint(queryId, submitResponse.resultEndpoint());
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
    @DisplayName("Knobs Enabled vs Disabled — Result Equivalence")
    class KnobsResultEquivalence {

        @Test
        @DisplayName("Table scan results identical with knobs enabled vs disabled")
        void tableScanEquivalence() {
            String sql = String.format(
                    "SELECT c_custkey, c_name, c_nationkey "
                    + "FROM %s.%s.customer "
                    + "ORDER BY c_custkey, c_nationkey LIMIT 50",
                    CATALOG, SCHEMA);

            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);
            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);

            assertThat(enabled).hasSameSizeAs(disabled);

            for (int i = 0; i < enabled.size(); i++) {
                long ek = enabled.get(i).getLong("c_custkey");
                long dk = disabled.get(i).getLong("c_custkey");
                assertThat(ek)
                        .as("Row %d c_custkey mismatch", i)
                        .isEqualTo(dk);

                long en = enabled.get(i).getLong("c_nationkey");
                long dn = disabled.get(i).getLong("c_nationkey");
                assertThat(en)
                        .as("Row %d c_nationkey mismatch", i)
                        .isEqualTo(dn);
            }
        }

        @Test
        @DisplayName("Hash join results identical with knobs enabled vs disabled")
        void hashJoinEquivalence() {
            String sql = String.format(
                    "SELECT c.c_custkey, o.o_orderkey, o.o_totalprice "
                    + "FROM %s.%s.customer c "
                    + "JOIN %s.%s.orders o "
                    + "ON c.c_custkey = o.o_custkey "
                    + "ORDER BY c.c_custkey, o.o_orderkey LIMIT 50",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);
            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);

            assertThat(enabled).hasSameSizeAs(disabled);

            for (int i = 0; i < enabled.size(); i++) {
                long eCust = enabled.get(i).getLong("c_custkey");
                long dCust = disabled.get(i).getLong("c_custkey");
                assertThat(eCust)
                        .as("Row %d c_custkey mismatch", i)
                        .isEqualTo(dCust);

                long eOrd = enabled.get(i).getLong("o_orderkey");
                long dOrd = disabled.get(i).getLong("o_orderkey");
                assertThat(eOrd)
                        .as("Row %d o_orderkey mismatch", i)
                        .isEqualTo(dOrd);
            }
        }

        @Test
        @DisplayName("Aggregation results identical with knobs enabled vs disabled")
        void aggregationEquivalence() {
            String sql = String.format(
                    "SELECT l_returnflag, l_linestatus, "
                    + "SUM(l_quantity) AS sum_qty, "
                    + "COUNT(*) AS count_order "
                    + "FROM %s.%s.lineitem "
                    + "GROUP BY l_returnflag, l_linestatus "
                    + "ORDER BY l_returnflag, l_linestatus",
                    CATALOG, SCHEMA);

            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);
            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);

            assertThat(enabled).hasSameSizeAs(disabled);

            for (int i = 0; i < enabled.size(); i++) {
                long eCount = enabled.get(i).getLong("count_order");
                long dCount = disabled.get(i).getLong("count_order");
                assertThat(eCount)
                        .as("Row %d count_order mismatch", i)
                        .isEqualTo(dCount);
            }
        }

        @Test
        @DisplayName("Multi-predicate filter identical with adaptive reordering")
        void filterReorderingEquivalence() {
            String sql = String.format(
                    "SELECT l_orderkey, l_quantity, l_discount "
                    + "FROM %s.%s.lineitem "
                    + "WHERE l_quantity > 20 "
                    + "AND l_discount < 0.05 "
                    + "AND l_shipdate >= DATE '1994-01-01' "
                    + "ORDER BY l_orderkey LIMIT 50",
                    CATALOG, SCHEMA);

            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);
            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);

            assertThat(enabled).hasSameSizeAs(disabled);

            for (int i = 0; i < enabled.size(); i++) {
                long eKey = enabled.get(i).getLong("l_orderkey");
                long dKey = disabled.get(i).getLong("l_orderkey");
                assertThat(eKey)
                        .as("Row %d l_orderkey mismatch", i)
                        .isEqualTo(dKey);
            }
        }

        @Test
        @DisplayName("Empty build side join correct with early termination")
        void emptyBuildSideJoin() {
            String sql = String.format(
                    "SELECT c.c_custkey, o.o_orderkey "
                    + "FROM %s.%s.customer c "
                    + "JOIN (SELECT o_custkey, o_orderkey "
                    + "      FROM %s.%s.orders "
                    + "      WHERE o_totalprice > 999999999) o "
                    + "ON c.c_custkey = o.o_custkey "
                    + "ORDER BY c.c_custkey",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);
            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);

            assertThat(enabled)
                    .as("Empty build side → empty result (knobs enabled)")
                    .isEmpty();
            assertThat(disabled)
                    .as("Empty build side → empty result (knobs disabled)")
                    .isEmpty();
        }

        @Test
        @DisplayName("String-keyed join identical with VARCHAR DF pushdown")
        void stringDynamicFilterEquivalence() {
            String sql = String.format(
                    "SELECT n.n_name, c.c_custkey "
                    + "FROM %s.%s.nation n "
                    + "JOIN %s.%s.customer c "
                    + "ON n.n_name = c.c_mktsegment "
                    + "ORDER BY n.n_name, c.c_custkey LIMIT 30",
                    CATALOG, SCHEMA, CATALOG, SCHEMA);

            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);
            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);

            assertThat(enabled).hasSameSizeAs(disabled);

            for (int i = 0; i < enabled.size(); i++) {
                long eKey = enabled.get(i).getLong("c_custkey");
                long dKey = disabled.get(i).getLong("c_custkey");
                assertThat(eKey)
                        .as("Row %d c_custkey mismatch", i)
                        .isEqualTo(dKey);
            }
        }
    }

    @Nested
    @DisplayName("Performance Knobs — Execution Timing")
    class KnobsPerformanceTiming {

        @Test
        @DisplayName("Complex join completes correctly with all knobs enabled")
        void complexJoinWithKnobsEnabled() {
            String sql = String.format(
                    "SELECT c.c_name, "
                    + "COUNT(o.o_orderkey) AS order_count, "
                    + "SUM(l.l_extendedprice * (1 - l.l_discount)) "
                    + "  AS revenue "
                    + "FROM %s.%s.customer c "
                    + "JOIN %s.%s.orders o "
                    + "  ON c.c_custkey = o.o_custkey "
                    + "JOIN %s.%s.lineitem l "
                    + "  ON o.o_orderkey = l.l_orderkey "
                    + "WHERE l.l_quantity > 10 "
                    + "  AND l.l_discount < 0.08 "
                    + "GROUP BY c.c_name "
                    + "ORDER BY revenue DESC LIMIT 10",
                    CATALOG, SCHEMA, CATALOG, SCHEMA,
                    CATALOG, SCHEMA);

            long startEnabled = System.nanoTime();
            List<TypedRow> enabled = executeWithSessionProperties(
                    sql, KNOBS_ENABLED);
            long enabledNs = System.nanoTime() - startEnabled;

            long startDisabled = System.nanoTime();
            List<TypedRow> disabled = executeWithSessionProperties(
                    sql, KNOBS_DISABLED);
            long disabledNs = System.nanoTime() - startDisabled;

            assertThat(enabled).hasSameSizeAs(disabled);

            System.out.printf(
                    "[QueryConfig Timing] enabled=%dms disabled=%dms%n",
                    enabledNs / 1_000_000, disabledNs / 1_000_000);
        }
    }
}
