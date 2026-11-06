package org.pangrova.testing.integration.e2e.index.correctness;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying R_TREE spatial index full-path: SQL with range predicates
 * on spatial columns through Parser, Planner, Substrait, Native execution, and
 * back to results. Uses standard BETWEEN predicates (Velox-compatible) that
 * trigger R_TREE index evaluation at the coordinator split-filtering layer.
 *
 * <p>Test data: 4 separate INSERT batches placing points in 4 non-overlapping
 * spatial quadrants: [0,10]², [100,110]², [200,210]², [300,310]².
 * Each batch forces a separate data file with distinct coordinate ranges.</p>
 *
 * <p>Validates: Requirements R1.1, R1.2, R1.3, R1.4</p>
 */
@DisplayName("R_TREE Index Full-Path E2E")
class RTreeIndexE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createSpatialTableWith4Quadrants(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_rtree_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, x DOUBLE, y DOUBLE, label VARCHAR)");

        // Quadrant 1: [0,10]²
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, x, y, label) VALUES"
                + " (1, 2.0, 3.0, 'q1_a'),"
                + " (2, 8.0, 7.0, 'q1_b')");
        // Quadrant 2: [100,110]²
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, x, y, label) VALUES"
                + " (3, 102.0, 105.0, 'q2_a'),"
                + " (4, 108.0, 109.0, 'q2_b')");
        // Quadrant 3: [200,210]²
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, x, y, label) VALUES"
                + " (5, 201.0, 204.0, 'q3_a'),"
                + " (6, 209.0, 206.0, 'q3_b')");
        // Quadrant 4: [300,310]²
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, x, y, label) VALUES"
                + " (7, 303.0, 301.0, 'q4_a'),"
                + " (8, 307.0, 308.0, 'q4_b')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // 9.1 + 9.2: Spatial query tests
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Spatial Query Tests")
    class SpatialQueryTests {

        @Test
        @DisplayName("Range predicate covering first quadrant returns only quadrant 1 rows")
        void spatialQueryFirstQuadrant() {
            sqlRunner.executeDdl("CREATE INDEX idx_rt ON " + fqn()
                    + "(x, y) USING R_TREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE (x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                double rx = ((Number) r.getRaw("x")).doubleValue();
                double ry = ((Number) r.getRaw("y")).doubleValue();
                return rx >= 0.0 && rx <= 10.0 && ry >= 0.0 && ry <= 10.0;
            });
            assertThat(rows).allMatch(r ->
                    r.getRaw("label").toString().startsWith("q1_"));
        }

        @Test
        @DisplayName("Range predicate matching no data returns empty result")
        void spatialQueryNoMatch() {
            sqlRunner.executeDdl("CREATE INDEX idx_rt ON " + fqn()
                    + "(x, y) USING R_TREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE (x BETWEEN 500.0 AND 600.0) AND (y BETWEEN 500.0 AND 600.0)");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("Range predicate covering all quadrants returns all rows")
        void spatialQueryAllQuadrants() {
            sqlRunner.executeDdl("CREATE INDEX idx_rt ON " + fqn()
                    + "(x, y) USING R_TREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE (x BETWEEN 0.0 AND 310.0) AND (y BETWEEN 0.0 AND 310.0)");
            assertThat(rows).hasSize(8);
        }
    }

    // -----------------------------------------------------------------------
    // 9.3: R_TREE result correctness invariant
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Result Correctness Invariant")
    class ResultCorrectness {

        @Test
        @DisplayName("R_TREE query with index returns superset of without-index baseline")
        void rtreeResultCorrectness() {
            // Capture baseline without index
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE (x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)");

            sqlRunner.executeDdl("CREATE INDEX idx_rt_corr ON " + fqn()
                    + "(x, y) USING R_TREE");

            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE (x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)");

            // Index must not cause false negatives
            assertThat(withIndex).containsAll(baseline);
        }
    }
}
