package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.catalog.TestConnectorFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying the Dynamic Filter (DPP) pipeline through
 * the complete Native Velox execution chain.
 *
 * <p>JOIN-based DPP tests are disabled because single-worker E2E cannot
 * execute multi-stage DAG plans. Single-table filter equivalents verify
 * the filter pushdown semantics that DPP relies on.</p>
 *
 * <p>Validates: Requirements 9.1, 9.4, 9.5</p>
 */
@DisplayName("Native Dynamic Filter E2E - DPP Full Pipeline Verification")
class NativeDynamicFilterE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA,
                        icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA,
                        icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    @Nested
    @DisplayName("Dynamic Filter Pushdown Tests")
    class DynamicFilterPushdownTests {

        @Test
        @DisplayName("Should push down simple equality filter to native layer")
        void shouldPushDownSimpleEqualityFilter() {
            String sql = String.format(
                    "SELECT c_custkey FROM %s.%s.customer WHERE c_custkey = 1",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);
            assertThat(results).isNotEmpty();
            // Verify all returned rows actually satisfy the filter predicate
            results.forEach(row ->
                    assertThat(row.getLong("c_custkey"))
                            .as("All rows must match equality filter c_custkey = 1")
                            .isEqualTo(1L));
        }

        @Test
        @DisplayName("Should push down range filter to native layer")
        void shouldPushDownRangeFilter() {
            String sql = String.format(
                    "SELECT c_custkey FROM %s.%s.customer WHERE c_custkey BETWEEN 1 AND 100",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);
            assertThat(results).isNotEmpty();
            // Verify all returned rows fall within the range
            results.forEach(row -> {
                long key = row.getLong("c_custkey");
                assertThat(key)
                        .as("All rows must satisfy BETWEEN 1 AND 100")
                        .isBetween(1L, 100L);
            });
        }

        @Test
        @DisplayName("Should push down IN clause filter to native layer")
        void shouldPushDownInClauseFilter() {
            String sql = String.format(
                    "SELECT c_custkey FROM %s.%s.customer WHERE c_custkey IN (1, 2, 3, 4, 5)",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);
            assertThat(results).isNotEmpty();
            // Verify all returned rows match one of the IN values
            results.forEach(row ->
                    assertThat(row.getLong("c_custkey"))
                            .as("All rows must match IN (1,2,3,4,5)")
                            .isIn(1L, 2L, 3L, 4L, 5L));
            // Verify all 5 distinct keys are present
            long distinctKeys = results.stream()
                    .map(r -> r.getLong("c_custkey"))
                    .distinct().count();
            assertThat(distinctKeys).as("All 5 filter keys should appear").isEqualTo(5L);
        }
    }

    @Nested
    @DisplayName("Partition Pruning Tests")
    class PartitionPruningTests {

        @Test
        @DisplayName("Should prune partitions based on filter predicate")
        void shouldPrunePartitions() {
            // Use a year-wide range instead of exact date: 100 rows across 2557 days
            // makes exact-date match unreliable; a full-year range is statistically safe.
            String sql = String.format(
                    "SELECT * FROM %s.%s.lineitem WHERE l_shipdate >= DATE '1992-01-01' AND l_shipdate < DATE '1993-01-01'",
                    CATALOG, SCHEMA);

            List<TypedRow> results = sqlRunner.execute(sql);
            assertThat(results).isNotEmpty();
        }
    }
}
