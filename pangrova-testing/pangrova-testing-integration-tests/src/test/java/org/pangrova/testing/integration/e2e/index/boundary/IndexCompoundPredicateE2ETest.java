package org.pangrova.testing.integration.e2e.index.boundary;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.IndexTestFixture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying compound AND predicate queries with multiple index types.
 *
 * <p>Test data: 4 separate INSERT batches to force 4 data files with distinct
 * value ranges. Each test creates two indexes on different columns, runs a
 * compound WHERE clause, and verifies result correctness via containsAll.</p>
 *
 * <p>Validates: Requirements R10.1, R10.2, R10.3</p>
 */
@DisplayName("Index Compound Predicate E2E — AND combination with multiple indexes")
class IndexCompoundPredicateE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    // -----------------------------------------------------------------------
    // 16.2: BTREE + BLOOM_FILTER compound query
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BTREE + BLOOM_FILTER Compound")
    class BTreeBloomFilterCompound {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_compound_bt_bf", info);
        }

        @Test
        @DisplayName("Compound AND query returns correct results with BTREE + BLOOM_FILTER")
        void compoundAndReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE (amount BETWEEN 40.0 AND 80.0) AND user_id = 1");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                long uid = ((Number) r.getRaw("user_id")).longValue();
                return amt >= 40.0 && amt <= 80.0 && uid == 1L;
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for BTREE + BLOOM_FILTER compound")
        void compoundCorrectnessInvariant() {
            // Capture baseline without any indexes
            String query = "(amount BETWEEN 40.0 AND 80.0) AND user_id = 1";
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE " + query);

            // Create both indexes
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE " + query);
            assertThat(withIndex).containsAll(baseline);
        }
    }

    // -----------------------------------------------------------------------
    // 16.3: PREFIX + BITMAP compound query
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("PREFIX + BITMAP Compound")
    class PrefixBitmapCompound {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_compound_pfx_bm", info);
        }

        @Test
        @DisplayName("Compound AND query returns correct results with PREFIX + BITMAP")
        void compoundAndReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn
                    + "(name) USING PREFIX");
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE name LIKE 'Ali%' AND status = 'ACTIVE'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                String name = r.getRaw("name").toString();
                String st = r.getRaw("status").toString();
                return name.startsWith("Ali") && "ACTIVE".equals(st);
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for PREFIX + BITMAP compound")
        void compoundCorrectnessInvariant() {
            // Capture baseline without any indexes
            String query = "name LIKE 'Ali%' AND status = 'ACTIVE'";
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE " + query);

            // Create both indexes
            sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn
                    + "(name) USING PREFIX");
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");

            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE " + query);
            assertThat(withIndex).containsAll(baseline);
        }
    }
}
