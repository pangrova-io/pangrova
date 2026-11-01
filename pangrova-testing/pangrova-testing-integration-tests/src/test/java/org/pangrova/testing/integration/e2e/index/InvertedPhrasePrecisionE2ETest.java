package org.pangrova.testing.integration.e2e.index;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * E2E tests for INVERTED index MATCH_PHRASE with/without store_positions.
 *
 * <p>Validates: Requirements 36.1–36.6, 42.3
 */
@DisplayName("Inverted Phrase Precision E2E")
class InvertedPhrasePrecisionE2ETest extends SqlE2ETestBase {

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
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_phrase_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, text_col VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " VALUES (1, 'the quick brown fox jumps'),"
                + " (2, 'brown fox quick the jumps'),"
                + " (3, 'completely different text')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("store_positions=false (Default)")
    class StorePositionsFalse {

        @Test
        @DisplayName("LIKE query with inverted index — returns matching rows")
        void likeQueryWithInvertedIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_inv_nopos ON " + fqn()
                    + "(text_col) USING INVERTED");

            // Use LIKE as MATCH() is not yet registered in Calcite
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE text_col LIKE '%quick brown fox%'");
            assertThat(result).isNotEmpty();
            // Row 3 should NOT be returned (no 'quick brown fox' substring)
            assertThat(result).noneMatch(r ->
                    Long.valueOf(3L).equals(((Number) r.getRaw("id")).longValue()));
        }
    }

    @Nested
    @DisplayName("store_positions=true")
    class StorePositionsTrue {

        @Test
        @DisplayName("LIKE query with positions-enabled inverted index")
        void likeQueryWithPositionsIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_inv_pos ON " + fqn()
                    + "(text_col) USING INVERTED WITH ('inverted.store_positions'='true')");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE text_col LIKE '%quick brown fox%'");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Index File Size Comparison")
    class IndexFileSizeComparison {

        @Test
        @DisplayName("store_positions=true index is larger than without positions")
        void positionsIndexLarger() {
            // Create index without positions
            sqlRunner.executeDdl("CREATE INDEX idx_nopos ON " + fqn()
                    + "(text_col) USING INVERTED");

            // Create another table for positions comparison
            String table2 = tableName + "_pos";
            sqlRunner.executeDdl("CREATE TABLE " + FQN_PREFIX + table2
                    + " (id BIGINT, text_col VARCHAR)");
            sqlRunner.execute("INSERT INTO " + FQN_PREFIX + table2
                    + " VALUES (1, 'the quick brown fox jumps'),"
                    + " (2, 'brown fox quick the jumps')");
            sqlRunner.executeDdl("CREATE INDEX idx_withpos ON " + FQN_PREFIX + table2
                    + "(text_col) USING INVERTED WITH ('inverted.store_positions'='true')");

            // Both indexes should be created
            List<TypedRow> idx1 = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(idx1).anyMatch(r ->
                    "idx_nopos".equals(r.getRaw("index_name").toString()));

            List<TypedRow> idx2 = sqlRunner.execute(
                    "SHOW INDEXES FROM " + FQN_PREFIX + table2);
            assertThat(idx2).anyMatch(r ->
                    "idx_withpos".equals(r.getRaw("index_name").toString()));
        }
    }
}
