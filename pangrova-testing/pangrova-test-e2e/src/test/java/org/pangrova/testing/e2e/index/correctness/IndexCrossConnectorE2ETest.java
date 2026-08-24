package org.pangrova.testing.e2e.index.correctness;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying cross-connector index consistency between Iceberg and Paimon.
 *
 * <p>For each index type, identical data is loaded into both connectors, the same
 * index is created, and the same query is executed. Results must be equivalent
 * (both contain the same logical rows).</p>
 *
 * <p>Covers 9 index types (INVERTED excluded — needs TextMatch, not LikePattern).</p>
 *
 * <p>Validates: Requirements R8.1, R8.2, R8.3</p>
 */
@DisplayName("Index Cross-Connector E2E — Iceberg vs Paimon consistency")
class IndexCrossConnectorE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String PMN_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String ICE_FQN = ICE_CATALOG + "." + SCHEMA + ".";
    private static final String PMN_FQN = PMN_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PMN_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalogs() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PMN_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    // -----------------------------------------------------------------------
    // Helpers: create identical tables on both connectors
    // -----------------------------------------------------------------------

    private String iceTable;
    private String pmnTable;

    private String iceFqn() { return ICE_FQN + iceTable; }
    private String pmnFqn() { return PMN_FQN + pmnTable; }

    private void createGenericDualTables(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        iceTable = "t_xconn_ice_" + method + "_" + ts;
        pmnTable = "t_xconn_pmn_" + method + "_" + ts;
        String ddl = " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)";
        sqlRunner.executeDdl("CREATE TABLE " + iceFqn() + ddl);
        sqlRunner.executeDdl("CREATE TABLE " + pmnFqn() + ddl);
        String[] batches = {
            " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                + " TIMESTAMP '2025-01-01 12:00:00')",
            " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')"
        };
        for (String batch : batches) {
            sqlRunner.execute("INSERT INTO " + iceFqn() + batch);
            sqlRunner.execute("INSERT INTO " + pmnFqn() + batch);
        }
    }

    private void createSpatialDualTables(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        iceTable = "t_xconn_rt_ice_" + method + "_" + ts;
        pmnTable = "t_xconn_rt_pmn_" + method + "_" + ts;
        String ddl = " (id BIGINT, x DOUBLE, y DOUBLE, label VARCHAR)";
        sqlRunner.executeDdl("CREATE TABLE " + iceFqn() + ddl);
        sqlRunner.executeDdl("CREATE TABLE " + pmnFqn() + ddl);
        String[] batches = {
            " (id, x, y, label) VALUES (1, 2.0, 3.0, 'q1'), (2, 8.0, 7.0, 'q1b')",
            " (id, x, y, label) VALUES (3, 102.0, 105.0, 'q2'), (4, 108.0, 109.0, 'q2b')"
        };
        for (String batch : batches) {
            sqlRunner.execute("INSERT INTO " + iceFqn() + batch);
            sqlRunner.execute("INSERT INTO " + pmnFqn() + batch);
        }
    }

    private void createGinDualTables(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        iceTable = "t_xconn_gin_ice_" + method + "_" + ts;
        pmnTable = "t_xconn_gin_pmn_" + method + "_" + ts;
        String ddl = " (id BIGINT, tags VARCHAR, data VARCHAR)";
        sqlRunner.executeDdl("CREATE TABLE " + iceFqn() + ddl);
        sqlRunner.executeDdl("CREATE TABLE " + pmnFqn() + ddl);
        String[] batches = {
            " (id, tags, data) VALUES (1, 'alpha,beta', 'd1'), (2, 'beta,alpha', 'd2')",
            " (id, tags, data) VALUES (3, 'gamma,delta', 'd3'), (4, 'delta,gamma', 'd4')"
        };
        for (String batch : batches) {
            sqlRunner.execute("INSERT INTO " + iceFqn() + batch);
            sqlRunner.execute("INSERT INTO " + pmnFqn() + batch);
        }
    }

    /**
     * Asserts cross-connector equivalence: same query on Iceberg and Paimon
     * must return equivalent result sets (mutual containsAll).
     */
    private void assertCrossConnectorEquivalence(
            String iceIndexDdl, String pmnIndexDdl, String whereClause) {
        sqlRunner.executeDdl(iceIndexDdl);
        sqlRunner.executeDdl(pmnIndexDdl);
        List<TypedRow> iceRows = sqlRunner.execute(
                "SELECT * FROM " + iceFqn() + " WHERE " + whereClause);
        List<TypedRow> pmnRows = sqlRunner.execute(
                "SELECT * FROM " + pmnFqn() + " WHERE " + whereClause);
        assertThat(iceRows).containsAll(pmnRows);
        assertThat(pmnRows).containsAll(iceRows);
    }

    // -----------------------------------------------------------------------
    // Equality-based indexes: BLOOM_FILTER, HASH, BITMAP
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Equality-Based Cross-Connector")
    class EqualityBased {

        @BeforeEach
        void setup(TestInfo info) { createGenericDualTables(info); }

        @Test
        @DisplayName("BLOOM_FILTER: Iceberg and Paimon return equivalent results")
        void bloomFilterCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_bf ON " + iceFqn() + "(user_id) USING BLOOM_FILTER",
                    "CREATE INDEX idx_bf ON " + pmnFqn() + "(user_id) USING BLOOM_FILTER",
                    "user_id = 1");
        }

        @Test
        @DisplayName("HASH: Iceberg and Paimon return equivalent results")
        void hashCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_h ON " + iceFqn() + "(session_id) USING HASH",
                    "CREATE INDEX idx_h ON " + pmnFqn() + "(session_id) USING HASH",
                    "session_id = 'sess-001'");
        }

        @Test
        @DisplayName("BITMAP: Iceberg and Paimon return equivalent results")
        void bitmapCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_bm ON " + iceFqn() + "(status) USING BITMAP",
                    "CREATE INDEX idx_bm ON " + pmnFqn() + "(status) USING BITMAP",
                    "status = 'ACTIVE'");
        }
    }

    // -----------------------------------------------------------------------
    // Range-based indexes: BTREE, SPARSE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Range-Based Cross-Connector")
    class RangeBased {

        @BeforeEach
        void setup(TestInfo info) { createGenericDualTables(info); }

        @Test
        @DisplayName("BTREE: Iceberg and Paimon return equivalent results")
        void btreeCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_bt ON " + iceFqn() + "(amount) USING BTREE",
                    "CREATE INDEX idx_bt ON " + pmnFqn() + "(amount) USING BTREE",
                    "amount BETWEEN 100.0 AND 200.0");
        }

        @Test
        @DisplayName("SPARSE: Iceberg and Paimon return equivalent results")
        void sparseCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_sp ON " + iceFqn() + "(user_id) USING SPARSE",
                    "CREATE INDEX idx_sp ON " + pmnFqn() + "(user_id) USING SPARSE",
                    "user_id BETWEEN 1 AND 20");
        }
    }

    // -----------------------------------------------------------------------
    // Text-based indexes: PREFIX, NGRAM_BLOOM_FILTER
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Text-Based Cross-Connector")
    class TextBased {

        @BeforeEach
        void setup(TestInfo info) { createGenericDualTables(info); }

        @Test
        @DisplayName("PREFIX: Iceberg and Paimon return equivalent results")
        void prefixCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_pfx ON " + iceFqn() + "(name) USING PREFIX",
                    "CREATE INDEX idx_pfx ON " + pmnFqn() + "(name) USING PREFIX",
                    "name LIKE 'Ali%'");
        }

        @Test
        @DisplayName("NGRAM_BLOOM_FILTER: Iceberg and Paimon return equivalent results")
        void ngramCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_ng ON " + iceFqn() + "(name) USING NGRAM_BLOOM_FILTER",
                    "CREATE INDEX idx_ng ON " + pmnFqn() + "(name) USING NGRAM_BLOOM_FILTER",
                    "name LIKE '%lic%'");
        }
    }

    // -----------------------------------------------------------------------
    // Spatial: R_TREE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Spatial Cross-Connector")
    class Spatial {

        @BeforeEach
        void setup(TestInfo info) { createSpatialDualTables(info); }

        @Test
        @DisplayName("R_TREE: Iceberg and Paimon return equivalent results")
        void rtreeCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_rt ON " + iceFqn() + "(x, y) USING R_TREE",
                    "CREATE INDEX idx_rt ON " + pmnFqn() + "(x, y) USING R_TREE",
                    "(x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)");
        }
    }

    // -----------------------------------------------------------------------
    // Containment: GIN
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Containment Cross-Connector")
    class Containment {

        @BeforeEach
        void setup(TestInfo info) { createGinDualTables(info); }

        @Test
        @DisplayName("GIN: Iceberg and Paimon return equivalent results")
        void ginCrossConnector() {
            assertCrossConnectorEquivalence(
                    "CREATE INDEX idx_gin ON " + iceFqn() + "(tags) USING GIN",
                    "CREATE INDEX idx_gin ON " + pmnFqn() + "(tags) USING GIN",
                    "tags LIKE '%alpha%'");
        }
    }
}
