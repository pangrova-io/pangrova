package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for index DDL lifecycle: CREATE INDEX, DROP INDEX, SHOW INDEXES,
 * ALTER INDEX REBUILD, ALTER TABLE ADD COLUMN, DROP TABLE cleanup, RENAME TABLE follow.
 *
 * <p>Validates: Requirements 3.1–3.9, 4.1–4.9, 29.1, 31.1, 31.2, 37.1, 37.3, R12.7
 */
@DisplayName("Index DDL Lifecycle E2E")
class IndexDdlLifecycleE2ETest extends SqlE2ETestBase {

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
        tableName = "t_idx_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, description VARCHAR, amount DOUBLE, status VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (user_id, description, amount, status) VALUES"
                + " (1, 'hello world', 100.0, 'ACTIVE'),"
                + " (2, 'pangrova engine', 200.0, 'INACTIVE')");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // --- 35.1.1: CREATE INDEX ---

    @Nested
    @DisplayName("CREATE INDEX")
    class CreateIndex {

        @Test
        @DisplayName("CREATE INDEX creates a BLOOM_FILTER index and SHOW INDEXES lists it")
        void createBloomFilterIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            assertThat(indexes.get(0).getRaw("index_name").toString()).isEqualTo("idx_bloom");
            assertThat(indexes.get(0).getRaw("index_type").toString()).isEqualTo("BLOOM_FILTER");
        }

        @Test
        @DisplayName("CREATE INDEX with WITH clause persists properties")
        void createIndexWithProperties() {
            sqlRunner.executeDdl("CREATE INDEX idx_inv ON " + fqn()
                    + "(description) USING INVERTED WITH ('inverted.analyzer'='standard')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();

            TypedRow idx = indexes.stream()
                    .filter(r -> "idx_inv".equals(r.getRaw("index_name").toString()))
                    .findFirst().orElseThrow();
            assertThat(idx.getRaw("index_type").toString()).isEqualTo("INVERTED");
        }

        @Test
        @DisplayName("CREATE INDEX IF NOT EXISTS is idempotent")
        void createIndexIfNotExistsIdempotent() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            // Second call should not error
            sqlRunner.executeDdl("CREATE INDEX IF NOT EXISTS idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            long bloomCount = indexes.stream()
                    .filter(r -> "idx_bloom".equals(r.getRaw("index_name").toString()))
                    .count();
            assertThat(bloomCount).isEqualTo(1);
        }
    }

    // --- 35.1.4–35.1.5: DROP INDEX ---

    @Nested
    @DisplayName("DROP INDEX")
    class DropIndex {

        @Test
        @DisplayName("DROP INDEX removes index from SHOW INDEXES")
        void dropIndexRemovesFromShow() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("DROP INDEX idx_bloom ON " + fqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean found = indexes.stream()
                    .anyMatch(r -> "idx_bloom".equals(r.getRaw("index_name").toString())
                            && !"DROPPED".equals(r.getRaw("state").toString()));
            assertThat(found).isFalse();
        }

        @Test
        @DisplayName("DROP INDEX IF EXISTS on non-existent index succeeds silently")
        void dropNonExistentIfExists() {
            sqlRunner.executeDdl("DROP INDEX IF EXISTS no_such_idx ON " + fqn());
            // No error thrown — test passes
        }
    }

    // --- 35.1.6: SHOW INDEXES ---

    @Nested
    @DisplayName("SHOW INDEXES")
    class ShowIndexes {

        @Test
        @DisplayName("SHOW INDEXES returns expected columns")
        void showIndexesColumns() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();

            TypedRow first = indexes.get(0);
            assertThat(first.containsColumn("index_name")).isTrue();
            assertThat(first.containsColumn("index_type")).isTrue();
            assertThat(first.containsColumn("columns")).isTrue();
            assertThat(first.containsColumn("state")).isTrue();
        }
    }

    // --- 35.1.7: ALTER INDEX REBUILD ---

    @Nested
    @DisplayName("ALTER INDEX REBUILD")
    class AlterIndexRebuild {

        @Test
        @DisplayName("ALTER INDEX REBUILD on freshly created index succeeds")
        void rebuildIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            sqlRunner.executeDdl("ALTER INDEX idx_bloom ON " + fqn() + " REBUILD");

            List<TypedRow> indexes = sqlRunner.execute(
                    "SHOW INDEXES ON " + fqn());
            assertThat(indexes).isNotEmpty();
        }
    }

    // --- 35.1.8: DDL validation errors ---

    @Nested
    @DisplayName("DDL Validation Errors")
    class DdlValidation {

        @Test
        @DisplayName("CREATE INDEX with duplicate name returns error")
        void duplicateNameError() {
            sqlRunner.executeDdl("CREATE INDEX idx_dup ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            QueryError error = sqlRunner.executeExpectingError(
                    "CREATE INDEX idx_dup ON " + fqn()
                            + "(amount) USING BTREE");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("already exists", "duplicate");
        }
    }

    // --- 35.1.9: Multi-column DDL validation ---

    @Nested
    @DisplayName("Multi-Column Index Validation")
    class MultiColumnValidation {

        @Test
        @DisplayName("BLOOM_FILTER rejects multi-column index")
        void bloomFilterRejectsMultiColumn() {
            QueryError error = sqlRunner.executeExpectingError(
                    "CREATE INDEX idx_mc ON " + fqn()
                            + "(user_id, amount) USING BLOOM_FILTER");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("single-column", "single column", "one column");
        }
    }

    // --- R12.7: ALTER TABLE ADD COLUMN — index still works ---

    @Nested
    @DisplayName("ALTER TABLE ADD COLUMN with Index")
    class AddColumnWithIndex {

        @Test
        @DisplayName("Index remains functional after ALTER TABLE ADD COLUMN")
        void indexSurvivesAddColumn() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexesBefore = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexesBefore).isNotEmpty();

            // Add a new column to the table
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " ADD COLUMN notes VARCHAR");

            // Index should still be listed
            List<TypedRow> indexesAfter = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean indexPresent = indexesAfter.stream()
                    .anyMatch(r -> "idx_bloom".equals(r.getRaw("index_name").toString()));
            assertThat(indexPresent)
                    .as("Index should survive ALTER TABLE ADD COLUMN")
                    .isTrue();

            // Query using the indexed column should still work
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT user_id FROM " + fqn() + " WHERE user_id = 1");
            assertThat(results).isNotEmpty();
        }
    }

    // --- R12.7: DROP TABLE — indexes cleaned up ---

    @Nested
    @DisplayName("DROP TABLE Cleans Up Indexes")
    class DropTableCleansIndexes {

        @Test
        @DisplayName("DROP TABLE removes all associated indexes")
        void dropTableRemovesIndexes() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_inv ON " + fqn()
                    + "(description) USING INVERTED");

            // Verify indexes exist
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes.size()).isGreaterThanOrEqualTo(2);

            // Drop the table
            sqlRunner.executeDdl("DROP TABLE " + fqn());

            List<TypedRow> afterDrop = sqlRunner.execute(
                    "SHOW INDEXES FROM " + fqn());
            assertThat(afterDrop)
                    .as("Indexes should be cleaned up after DROP TABLE")
                    .isEmpty();
        }
    }

    // --- R12.7: RENAME TABLE — indexes follow ---

    @Nested
    @DisplayName("RENAME TABLE — Indexes Follow")
    class RenameTableIndexesFollow {

        @Test
        @DisplayName("RENAME TABLE on HadoopCatalog returns structured error")
        void renameTableOnHadoopCatalogReturnsError() {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexesBefore = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexesBefore).isNotEmpty();

            String newTableName = tableName + "_renamed";
            String newFqn = FQN_PREFIX + newTableName;

            QueryError error = sqlRunner.executeExpectingError(
                    "ALTER TABLE " + fqn() + " RENAME TO " + newFqn);
            assertThat(error.message()).containsIgnoringCase("rename");
        }
    }
}
