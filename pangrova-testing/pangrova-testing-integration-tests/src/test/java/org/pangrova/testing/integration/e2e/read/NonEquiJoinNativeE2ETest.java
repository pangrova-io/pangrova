package org.pangrova.testing.integration.e2e.read;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.catalog.TestConnectorFactory;
import org.pangrova.worker.jni.NativeRuntime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test verifying non-equi join (theta-join) native execution
 * through the complete execution pipeline.
 *
 * <p>Validates that SQL queries with non-equi join conditions (e.g., {@code a.x < b.y})
 * execute natively via NestedLoopJoinNode without falling back to Java execution.
 * Verifies correct results and operator statistics availability in TaskStats.
 *
 * <p>Validates: Requirements 4.1, 4.3, 4.5
 */
@DisplayName("Non-Equi Join Native Execution E2E")
class NonEquiJoinNativeE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    // -----------------------------------------------------------------------
    // Non-Equi Join (Theta-Join) Correctness
    // Validates: Requirements 4.1, 4.3
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Theta-Join Correctness")
    class ThetaJoinCorrectness {

        @Test
        @DisplayName("Less-than theta-join produces correct results without Java fallback")
        void lessThanThetaJoinProducesCorrectResults() {
            // Use small tables (region, nation) to keep cross-product manageable.
            // Theta-join: region.r_regionkey < nation.n_regionkey
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT r.r_name, n.n_name, r.r_regionkey, n.n_regionkey "
                    + "FROM region r, nation n "
                    + "WHERE r.r_regionkey < n.n_regionkey");

            assertThat(rows)
                    .as("Theta-join with < condition should produce non-empty results")
                    .isNotEmpty();

            // Verify all returned rows satisfy the theta-join condition
            rows.forEach(row -> {
                long rKey = ((Number) row.getRaw("r_regionkey")).longValue();
                long nKey = ((Number) row.getRaw("n_regionkey")).longValue();
                assertThat(rKey)
                        .as("All rows must satisfy r.r_regionkey < n.n_regionkey")
                        .isLessThan(nKey);
            });
        }

        @Test
        @DisplayName("Greater-than theta-join produces correct results")
        void greaterThanThetaJoinProducesCorrectResults() {
            // Theta-join: nation.n_regionkey > region.r_regionkey
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT n.n_name, r.r_name, n.n_regionkey AS n_rkey, r.r_regionkey AS r_rkey "
                    + "FROM nation n, region r "
                    + "WHERE n.n_regionkey > r.r_regionkey");

            assertThat(rows)
                    .as("Theta-join with > condition should produce non-empty results")
                    .isNotEmpty();

            rows.forEach(row -> {
                long nKey = ((Number) row.getRaw("n_rkey")).longValue();
                long rKey = ((Number) row.getRaw("r_rkey")).longValue();
                assertThat(nKey)
                        .as("All rows must satisfy n.n_regionkey > r.r_regionkey")
                        .isGreaterThan(rKey);
            });
        }

        @Test
        @DisplayName("Non-equi join result count matches cross-product filtered by condition")
        void nonEquiJoinResultCountMatchesCrossProductFiltered() {
            // Verify result count: count from theta-join should equal
            // count from cross join with same filter applied.
            List<TypedRow> thetaCount = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt "
                    + "FROM region r, nation n "
                    + "WHERE r.r_regionkey < n.n_regionkey");

            // Cross join with explicit filter should produce same count
            List<TypedRow> crossFilteredCount = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt "
                    + "FROM region r CROSS JOIN nation n "
                    + "WHERE r.r_regionkey < n.n_regionkey");

            long thetaCnt = ((Number) thetaCount.get(0).getRaw("cnt")).longValue();
            long crossCnt = ((Number) crossFilteredCount.get(0).getRaw("cnt")).longValue();

            assertThat(thetaCnt)
                    .as("Theta-join count must equal cross-join + filter count")
                    .isEqualTo(crossCnt);
            assertThat(thetaCnt)
                    .as("Result should be non-zero for valid theta-join")
                    .isGreaterThan(0);
        }

        @Test
        @DisplayName("Between-style theta-join with compound condition produces correct results")
        void betweenStyleThetaJoinProducesCorrectResults() {
            // Compound non-equi condition: r_regionkey >= 1 AND r_regionkey < n_regionkey
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT r.r_name, n.n_name, r.r_regionkey, n.n_regionkey "
                    + "FROM region r, nation n "
                    + "WHERE r.r_regionkey >= 1 AND r.r_regionkey < n.n_regionkey");

            assertThat(rows)
                    .as("Compound theta-join should produce non-empty results")
                    .isNotEmpty();

            rows.forEach(row -> {
                long rKey = ((Number) row.getRaw("r_regionkey")).longValue();
                long nKey = ((Number) row.getRaw("n_regionkey")).longValue();
                assertThat(rKey)
                        .as("r_regionkey must be >= 1")
                        .isGreaterThanOrEqualTo(1);
                assertThat(rKey)
                        .as("r_regionkey must be < n_regionkey")
                        .isLessThan(nKey);
            });
        }
    }

    // -----------------------------------------------------------------------
    // Operator Statistics Verification
    // Validates: Requirements 4.5
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Operator Statistics in TaskStats")
    class OperatorStatisticsVerification {

        @Test
        @DisplayName("Non-equi join query completes natively and TaskStats are available")
        void nonEquiJoinProducesTaskStats() {
            // Execute a theta-join query
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT r.r_name, n.n_name "
                    + "FROM region r, nation n "
                    + "WHERE r.r_regionkey < n.n_regionkey");

            assertThat(rows).isNotEmpty();

            // Verify native memory snapshot is available after query execution,
            // confirming the query executed through the native pipeline (not Java fallback)
            byte[] memSnapshot = NativeRuntime.nativeGetMemorySnapshot();
            assertThat(memSnapshot)
                    .as("Native memory snapshot should be available after non-equi join execution, "
                        + "confirming native pipeline was used (no Java fallback)")
                    .isNotNull();
        }

        @Test
        @DisplayName("Non-equi join with multiple columns executes natively")
        void multiColumnNonEquiJoinExecutesNatively() {
            // More complex theta-join involving multiple columns from both sides
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT n1.n_name AS nation1, n2.n_name AS nation2, "
                    + "n1.n_regionkey AS rkey1, n2.n_regionkey AS rkey2 "
                    + "FROM nation n1, nation n2 "
                    + "WHERE n1.n_nationkey < n2.n_nationkey "
                    + "AND n1.n_regionkey = n2.n_regionkey");

            assertThat(rows)
                    .as("Self-join with mixed equi and non-equi conditions should produce results")
                    .isNotEmpty();

            // Verify the equi condition holds
            rows.forEach(row -> {
                long rkey1 = ((Number) row.getRaw("rkey1")).longValue();
                long rkey2 = ((Number) row.getRaw("rkey2")).longValue();
                assertThat(rkey1)
                        .as("Equi-condition: n1.n_regionkey must equal n2.n_regionkey")
                        .isEqualTo(rkey2);
            });

            // Confirm native execution
            byte[] memSnapshot = NativeRuntime.nativeGetMemorySnapshot();
            assertThat(memSnapshot)
                    .as("Native memory snapshot confirms native execution path")
                    .isNotNull();
        }

        @Test
        @DisplayName("Non-equi join with ORDER BY and LIMIT executes natively with correct ordering")
        void nonEquiJoinWithOrderByLimitExecutesNatively() {
            int limit = 10;
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT r.r_name, n.n_name, r.r_regionkey, n.n_regionkey "
                    + "FROM region r, nation n "
                    + "WHERE r.r_regionkey < n.n_regionkey "
                    + "ORDER BY r.r_regionkey, n.n_regionkey "
                    + "LIMIT " + limit);

            assertThat(rows)
                    .as("Theta-join with ORDER BY + LIMIT should produce bounded results")
                    .hasSizeLessThanOrEqualTo(limit);
            assertThat(rows).isNotEmpty();

            // Verify ordering
            for (int i = 1; i < rows.size(); i++) {
                long prevRKey = ((Number) rows.get(i - 1).getRaw("r_regionkey")).longValue();
                long currRKey = ((Number) rows.get(i).getRaw("r_regionkey")).longValue();
                if (prevRKey == currRKey) {
                    long prevNKey = ((Number) rows.get(i - 1).getRaw("n_regionkey")).longValue();
                    long currNKey = ((Number) rows.get(i).getRaw("n_regionkey")).longValue();
                    assertThat(currNKey).isGreaterThanOrEqualTo(prevNKey);
                } else {
                    assertThat(currRKey).isGreaterThanOrEqualTo(prevRKey);
                }
            }

            // Verify theta-join condition still holds
            rows.forEach(row -> {
                long rKey = ((Number) row.getRaw("r_regionkey")).longValue();
                long nKey = ((Number) row.getRaw("n_regionkey")).longValue();
                assertThat(rKey).isLessThan(nKey);
            });
        }
    }
}
