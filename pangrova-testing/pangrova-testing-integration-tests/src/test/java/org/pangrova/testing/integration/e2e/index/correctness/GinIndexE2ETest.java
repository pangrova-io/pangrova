package org.pangrova.testing.integration.e2e.index.correctness;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E tests verifying GIN (Generalized Inverted Index) full-path: SQL with
 * LIKE predicates on a VARCHAR tags column through Parser, Planner, Substrait,
 * Native execution, and back to results. Uses standard LIKE predicates
 * (Velox-compatible) that trigger GIN index evaluation at the coordinator
 * split-filtering layer.
 *
 * <p>Test data: 4 separate INSERT batches with non-overlapping tag sets in a
 * VARCHAR column. GIN's {@code ElementExtractor} parses composite values from
 * a single VARCHAR column (comma-separated, JSON array, or single value).</p>
 *
 * <p>Validates: Requirements R2.1, R2.2, R2.3, R2.4</p>
 */
@DisplayName("GIN Index Full-Path E2E")
class GinIndexE2ETest extends SqlE2ETestBase {

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
    void createTagTableWith4Files(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_gin_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, tags VARCHAR, data VARCHAR)");

        // File 1: tags contain 'alpha', 'beta'
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, tags, data) VALUES"
                + " (1, 'alpha,beta', 'data_f1_a'),"
                + " (2, 'beta,alpha', 'data_f1_b')");
        // File 2: tags contain 'gamma', 'delta'
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, tags, data) VALUES"
                + " (3, 'gamma,delta', 'data_f2_a'),"
                + " (4, 'delta,gamma', 'data_f2_b')");
        // File 3: tags contain 'epsilon', 'zeta'
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, tags, data) VALUES"
                + " (5, 'epsilon,zeta', 'data_f3_a'),"
                + " (6, 'zeta,epsilon', 'data_f3_b')");
        // File 4: tags contain 'eta', 'theta'
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, tags, data) VALUES"
                + " (7, 'eta,theta', 'data_f4_a'),"
                + " (8, 'theta,eta', 'data_f4_b')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // 10.1 + 10.2: Containment query tests
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Containment Query Tests")
    class ContainmentQueryTests {

        @Test
        @DisplayName("LIKE '%alpha%' returns only file 1 rows")
        void containsAlphaReturnsFile1Only() {
            sqlRunner.executeDdl("CREATE INDEX idx_gin ON " + fqn()
                    + "(tags) USING GIN");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE tags LIKE '%alpha%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("tags").toString().contains("alpha"));
            assertThat(rows).allMatch(r ->
                    r.getRaw("data").toString().startsWith("data_f1_"));
        }

        @Test
        @DisplayName("LIKE '%nonexistent%' returns empty result")
        void containsNonexistentReturnsEmpty() {
            sqlRunner.executeDdl("CREATE INDEX idx_gin ON " + fqn()
                    + "(tags) USING GIN");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE tags LIKE '%nonexistent%'");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("LIKE '%gamma%' returns only file 2 rows")
        void containsGammaReturnsFile2Only() {
            sqlRunner.executeDdl("CREATE INDEX idx_gin ON " + fqn()
                    + "(tags) USING GIN");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE tags LIKE '%gamma%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("tags").toString().contains("gamma"));
            assertThat(rows).allMatch(r ->
                    r.getRaw("data").toString().startsWith("data_f2_"));
        }
    }

    // -----------------------------------------------------------------------
    // 10.3: GIN result correctness invariant
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Result Correctness Invariant")
    class ResultCorrectness {

        @Test
        @DisplayName("GIN query with index returns superset of without-index baseline")
        void ginResultCorrectness() {
            // Capture baseline without index
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE tags LIKE '%alpha%'");

            sqlRunner.executeDdl("CREATE INDEX idx_gin_corr ON " + fqn()
                    + "(tags) USING GIN");

            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE tags LIKE '%alpha%'");

            // Index must not cause false negatives
            assertThat(withIndex).containsAll(baseline);
        }
    }
}
