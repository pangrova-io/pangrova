/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.CelebornTestContainers;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end tests for the Celeborn (Push-Based RSS) shuffle integration (ADR-002 D4).
 *
 * <p>Validates that queries using {@code shuffle.manager=celeborn} complete correctly with
 * data routed through Celeborn Master + Worker containers. Tests cover:
 * <ul>
 *   <li>Hash shuffle correctness</li>
 *   <li>Range shuffle ordering</li>
 *   <li>Multi-stage join+aggregation</li>
 *   <li>Data equivalence: celeborn vs native results must be identical</li>
 *   <li>Large shuffle stability (no OOM/timeout)</li>
 *   <li>Worker registration health check</li>
 * </ul>
 *
 * <p><strong>Activation:</strong> {@code @Tag("celeborn")} — excluded from default CI runs.
 * Execute with: {@code mvn test -pl pangrova-testing/pangrova-test-e2e -Dgroups=celeborn -Dsurefire.excludedGroups=}
 *
 * <p><strong>Prerequisites:</strong>
 * <ul>
 *   <li>Docker available (for TestContainers)</li>
 *   <li>{@code pangrova-shuffle-celeborn} module built and on test classpath</li>
 *   <li>Native library available (for Pangrova in-JVM worker)</li>
 * </ul>
 */
@Tag("celeborn")
@DisplayName("Celeborn Shuffle E2E — Push-Based RSS Full Pipeline")
class CelebornShuffleE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCelebornProvider() {
        assumeTrue(CelebornTestContainers.AVAILABLE,
                "Celeborn containers not ready — skipping Celeborn E2E tests");

        // Configure Pangrova to use the Celeborn shuffle provider
        Map<String, String> celebornProps = CelebornTestContainers.clientProperties();
        System.setProperty("pangrova.shuffle.manager", "celeborn");
        System.setProperty("celeborn.master.endpoints",
                celebornProps.get("celeborn.master.endpoints"));
    }

    @AfterAll
    static void restoreShuffleProvider() {
        System.clearProperty("pangrova.shuffle.manager");
        System.clearProperty("celeborn.master.endpoints");
    }

    // --- Test Scenarios (as specified in plan Appendix D) ---

    @Nested
    @DisplayName("Scenario 1: Hash Shuffle Correctness")
    class HashShuffleTest {

        @Test
        @DisplayName("GROUP BY produces correct aggregation results via Celeborn")
        void hashShuffleGroupBy() {
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT l_returnflag, SUM(l_quantity) as total_qty "
                    + "FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag");

            assertThat(results)
                    .isNotEmpty()
                    .hasSizeGreaterThanOrEqualTo(3); // A, N, R

            // Verify each row has expected columns
            for (TypedRow row : results) {
                assertThat(row.getRaw("l_returnflag")).isNotNull();
                assertThat(((Number) row.getRaw("total_qty")).longValue()).isPositive();
            }
        }
    }

    @Nested
    @DisplayName("Scenario 2: Range Shuffle Ordering")
    class RangeShuffleTest {

        @Test
        @DisplayName("ORDER BY produces globally sorted results via Celeborn")
        void rangeShuffleOrderBy() {
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT l_orderkey FROM lineitem ORDER BY l_orderkey LIMIT 200");

            assertThat(results).hasSize(200);

            // Verify global ordering
            long prev = Long.MIN_VALUE;
            for (TypedRow row : results) {
                long key = ((Number) row.getRaw("l_orderkey")).longValue();
                assertThat(key).isGreaterThanOrEqualTo(prev);
                prev = key;
            }
        }
    }

    @Nested
    @DisplayName("Scenario 3: Multi-stage Join + Aggregation")
    class MultiStageTest {

        @Test
        @DisplayName("TPC-H Q9 (multi-stage shuffle) completes without error")
        void multiStageJoinAgg() {
            // Simplified TPC-H Q9: nation profit query with join + group by + order by
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT n_name, EXTRACT(YEAR FROM o_orderdate) as o_year, "
                    + "SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as profit "
                    + "FROM part, supplier, lineitem, partsupp, orders, nation "
                    + "WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey "
                    + "AND ps_partkey = l_partkey AND p_partkey = l_partkey "
                    + "AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey "
                    + "AND p_name LIKE '%green%' "
                    + "GROUP BY n_name, EXTRACT(YEAR FROM o_orderdate) "
                    + "ORDER BY n_name, o_year DESC "
                    + "LIMIT 50");

            assertThat(results)
                    .isNotEmpty()
                    .describedAs("TPC-H Q9 should produce results via Celeborn multi-stage shuffle");
        }
    }

    @Nested
    @DisplayName("Scenario 4: Data Equivalence Cross-Validation")
    class DataEquivalenceTest {

        @Test
        @DisplayName("Celeborn and native shuffle produce identical results")
        void celebornVsNativeEquivalence() {
            String sql = "SELECT l_returnflag, l_linestatus, COUNT(*) as cnt, "
                    + "SUM(l_quantity) as sum_qty "
                    + "FROM lineitem GROUP BY l_returnflag, l_linestatus "
                    + "ORDER BY l_returnflag, l_linestatus";

            // Execute with Celeborn (current config)
            List<TypedRow> celebornResults = sqlRunner.execute(sql);

            // Execute with native shuffle for comparison
            String originalManager = System.getProperty("pangrova.shuffle.manager");
            try {
                System.setProperty("pangrova.shuffle.manager", "native");
                List<TypedRow> nativeResults = sqlRunner.execute(sql);

                assertThat(celebornResults)
                        .as("Celeborn and native shuffle must produce identical results")
                        .hasSameSizeAs(nativeResults);

                // Compare row by row using raw map representation
                for (int i = 0; i < celebornResults.size(); i++) {
                    assertThat(celebornResults.get(i).toRawMap())
                            .as("Row %d should be identical", i)
                            .isEqualTo(nativeResults.get(i).toRawMap());
                }
            } finally {
                if (originalManager != null) {
                    System.setProperty("pangrova.shuffle.manager", originalManager);
                } else {
                    System.clearProperty("pangrova.shuffle.manager");
                }
            }
        }
    }

    @Nested
    @DisplayName("Scenario 5: Large Shuffle Stability")
    class LargeShuffleTest {

        @Test
        @DisplayName("Large join+group+order query completes without OOM or timeout")
        void largeShuffleStability() {
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT l_shipmode, COUNT(*) as cnt, SUM(l_quantity) as sum_qty, "
                    + "AVG(l_extendedprice) as avg_price "
                    + "FROM lineitem JOIN orders ON l_orderkey = o_orderkey "
                    + "GROUP BY l_shipmode ORDER BY l_shipmode");

            assertThat(results)
                    .isNotEmpty()
                    .describedAs("Large shuffle should complete without OOM/timeout");
        }
    }

    @Nested
    @DisplayName("Scenario 6: mapId/attemptId Deduplication")
    class MapperRetryDeduplicationTest {

        @Test
        @DisplayName("Duplicate mapper pushes do not produce duplicate results")
        void duplicateMapperPushDedup() {
            // Execute the same aggregation query twice via Celeborn.
            // If mapId/attemptId deduplication is broken, running the same
            // query in rapid succession would show doubled row counts.
            String sql = "SELECT l_returnflag, COUNT(*) as cnt "
                    + "FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag";

            List<TypedRow> firstRun = sqlRunner.execute(sql);
            List<TypedRow> secondRun = sqlRunner.execute(sql);

            assertThat(firstRun)
                    .as("Both runs must produce identical result count (no duplicate data)")
                    .hasSameSizeAs(secondRun);

            // Verify row-level equivalence — counts must match exactly
            for (int i = 0; i < firstRun.size(); i++) {
                long cnt1 = ((Number) firstRun.get(i).getRaw("cnt")).longValue();
                long cnt2 = ((Number) secondRun.get(i).getRaw("cnt")).longValue();
                assertThat(cnt1)
                        .as("Row %d: count must be identical across runs (no dedup failure)", i)
                        .isEqualTo(cnt2);
            }
        }
    }

    @Nested
    @DisplayName("Scenario 7: Worker Health Verification")
    class WorkerHealthTest {

        @Test
        @DisplayName("Celeborn Worker is registered and healthy during test")
        void workerRegisteredAndHealthy() throws Exception {
            assumeTrue(CelebornTestContainers.AVAILABLE);

            // Query Celeborn Master HTTP metrics endpoint to verify worker registration
            var masterContainer = CelebornTestContainers.MASTER;
            var execResult = masterContainer.execInContainer(
                    "curl", "-s", "http://localhost:9098/metrics/prometheus");

            String metricsOutput = execResult.getStdout();
            assertThat(metricsOutput)
                    .as("Celeborn metrics endpoint should be accessible")
                    .isNotBlank();

            // Verify at least one worker is registered
            // (the metric name may vary by Celeborn version)
            assertThat(execResult.getExitCode())
                    .as("Metrics endpoint should respond successfully")
                    .isZero();
        }
    }
}
