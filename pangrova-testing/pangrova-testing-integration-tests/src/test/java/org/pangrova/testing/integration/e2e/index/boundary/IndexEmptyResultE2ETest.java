package org.pangrova.testing.integration.e2e.index.boundary;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * E2E tests verifying that all 10 index types (excluding INVERTED) return
 * an empty result set without error when a query predicate matches no rows.
 *
 * <p>For each index type, a table is created with 4 INSERT batches of real
 * data, an index is created, and then a query with a value that does not
 * exist in the data is executed. The test verifies empty result and no
 * error (no NPE, no timeout).</p>
 *
 * <p>Validates: Requirements R6.1, R6.2</p>
 */
@DisplayName("Index Empty Result E2E — no-match queries return empty, no error")
class IndexEmptyResultE2ETest extends SqlE2ETestBase {

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
    // Equality-based indexes: BLOOM_FILTER, HASH, BITMAP
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Equality-Based Empty Results")
    class EqualityBasedEmptyResults {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_empty_eq", info);
        }

        @Test
        @DisplayName("BLOOM_FILTER: non-matching equality returns empty result")
        void bloomFilterEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id = 99999");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("HASH: non-matching equality returns empty result")
        void hashEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_hash ON " + fqn
                    + "(session_id) USING HASH");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE session_id = 'nonexistent'");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("BITMAP: non-matching equality returns empty result")
        void bitmapEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE status = 'NONEXISTENT'");
            assertThat(rows).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Range-based indexes: BTREE, SPARSE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Range-Based Empty Results")
    class RangeBasedEmptyResults {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_empty_rng", info);
        }

        @Test
        @DisplayName("BTREE: non-matching range returns empty result")
        void btreeEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 9000.0 AND 9999.0");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("SPARSE: non-matching range returns empty result")
        void sparseEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_sp ON " + fqn
                    + "(user_id) USING SPARSE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id BETWEEN 50000 AND 60000");
            assertThat(rows).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Text-based indexes: PREFIX, NGRAM_BLOOM_FILTER
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Text-Based Empty Results")
    class TextBasedEmptyResults {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_empty_txt", info);
        }

        @Test
        @DisplayName("PREFIX: non-matching LIKE prefix returns empty result")
        void prefixEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn
                    + "(name) USING PREFIX");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE 'ZZZZZ%'");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("NGRAM_BLOOM_FILTER: non-matching LIKE substring returns empty result")
        void ngramEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_ngram ON " + fqn
                    + "(name) USING NGRAM_BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE '%ZZZZZ%'");
            assertThat(rows).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Spatial: R_TREE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Spatial Empty Results")
    class SpatialEmptyResults {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createSpatialTable(sqlRunner, CATALOG, SCHEMA, "t_empty_rt", info);
        }

        @Test
        @DisplayName("R_TREE: non-matching spatial query returns empty result")
        void rtreeEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_rt ON " + fqn
                    + "(x, y) USING R_TREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE (x BETWEEN 9000.0 AND 9999.0) AND (y BETWEEN 9000.0 AND 9999.0)");
            assertThat(rows).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Containment: GIN
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Containment Empty Results")
    class ContainmentEmptyResults {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGinTable(sqlRunner, CATALOG, SCHEMA, "t_empty_gin", info);
        }

        @Test
        @DisplayName("GIN: non-matching LIKE returns empty result")
        void ginEmptyResult() {
            sqlRunner.executeDdl("CREATE INDEX idx_gin ON " + fqn
                    + "(tags) USING GIN");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE tags LIKE '%nonexistent%'");
            assertThat(rows).isEmpty();
        }
    }
}
