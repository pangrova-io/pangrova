package org.pangrova.testing.integration.e2e.index;

import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.pangrova.types.value.TypedRow;
import org.pangrova.indexing.api.data.BTreeIndexData;
import org.pangrova.indexing.api.data.CompositeIndexKey;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
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

    private void awaitReadyIndex(String indexName) {
        org.awaitility.Awaitility.await("index READY: " + indexName)
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(sqlRunner.execute("SHOW INDEXES FROM " + fqn()).stream()
                        .filter(row -> indexName.equals(row.getRaw("index_name").toString()))
                        .findFirst().orElseThrow().getRaw("state").toString()).isEqualTo("READY"));
    }

    private void awaitReadyGeneration(String indexName, int generation) {
        org.awaitility.Awaitility.await("index generation " + generation + ": " + indexName)
                .atMost(Duration.ofSeconds(30))
                .until(() -> {
                    try (var connection = stateStoreDataSource().getConnection();
                            var statement = connection.prepareStatement(
                                    "SELECT generation, state FROM index_descriptors "
                                            + "WHERE table_name=? AND index_name=?")) {
                        statement.setString(1, tableName);
                        statement.setString(2, indexName);
                        try (var rows = statement.executeQuery()) {
                            return rows.next() && rows.getInt("generation") == generation
                                    && "READY".equals(rows.getString("state"));
                        }
                    }
                });
    }

    // --- 35.1.1: CREATE INDEX ---

    @Nested
    @DisplayName("CREATE INDEX")
    class CreateIndex {

        @Test
        @DisplayName("CREATE INDEX creates a BLOOM_FILTER index and SHOW INDEXES lists it")
        void createBloomFilterIndex() throws Exception {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            assertThat(indexes.get(0).getRaw("index_name").toString()).isEqualTo("idx_bloom");
            assertThat(indexes.get(0).getRaw("index_type").toString()).isEqualTo("BLOOM_FILTER");
            awaitReadyIndex("idx_bloom");
            try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));
                long sourceSnapshot = table.currentSnapshot().snapshotId();
                assertThat(table.refs().entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith("pangrova_index_"))
                        .map(entry -> entry.getValue().snapshotId()))
                        .containsExactly(sourceSnapshot);
            }
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
            assertThat(first.columnNames()).containsExactlyInAnyOrder(
                    "index_id", "index_name", "index_type", "columns", "state", "stale_reason",
                    "generation", "build_snapshot_id", "last_indexed_snapshot_id", "created_at", "updated_at",
                    "algorithm", "metric");
        }
    }

    // --- 35.1.7: ALTER INDEX REBUILD ---

    @Nested
    @DisplayName("ALTER INDEX REBUILD")
    class AlterIndexRebuild {

        @Test
        @DisplayName("ALTER INDEX REBUILD on freshly created index succeeds")
        void rebuildIndex() throws Exception {
            sqlRunner.executeDdl("CREATE INDEX idx_bloom ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            awaitReadyIndex("idx_bloom");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (user_id, description, amount, status) VALUES"
                    + " (3, 'new generation', 300.0, 'ACTIVE')");

            sqlRunner.executeDdl("ALTER INDEX idx_bloom ON " + fqn() + " REBUILD");

            awaitReadyGeneration("idx_bloom", 1);
            try (var connection = stateStoreDataSource().getConnection();
                    var statement = connection.prepareStatement("""
                            SELECT old.index_file_path, rebuilt.index_file_path
                            FROM index_descriptors AS descriptor
                            JOIN index_file_mappings AS old
                              ON old.index_id = descriptor.index_id AND old.generation = 0
                            JOIN index_file_mappings AS rebuilt
                              ON rebuilt.index_id = descriptor.index_id AND rebuilt.generation = 1
                              AND rebuilt.data_file_path = old.data_file_path
                            WHERE descriptor.table_name = ? AND descriptor.index_name = ?
                            """)) {
                statement.setString(1, tableName);
                statement.setString(2, "idx_bloom");
                int sharedSources = 0;
                try (var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        String oldPath = rows.getString(1);
                        String rebuiltPath = rows.getString(2);
                        assertThat(rebuiltPath).isNotEqualTo(oldPath).contains("/g1/");
                        var oldFile = new org.apache.hadoop.fs.Path(oldPath);
                        var rebuiltFile = new org.apache.hadoop.fs.Path(rebuiltPath);
                        var configuration = new Configuration();
                        assertThat(oldFile.getFileSystem(configuration).exists(oldFile)).isTrue();
                        assertThat(rebuiltFile.getFileSystem(configuration).exists(rebuiltFile)).isTrue();
                        sharedSources++;
                    }
                }
                assertThat(sharedSources).isPositive();
            }
            try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));
                long rebuiltSnapshot = table.currentSnapshot().snapshotId();
                assertThat(table.refs().entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith("pangrova_index_"))
                        .map(entry -> entry.getValue().snapshotId()))
                        .contains(rebuiltSnapshot);
            }

            List<TypedRow> indexes = sqlRunner.execute(
                    "SHOW INDEXES ON " + fqn());
            assertThat(indexes).isNotEmpty();
            assertThat(sqlRunner.execute("SELECT user_id FROM " + fqn() + " WHERE user_id = 3"))
                    .hasSize(1);
        }
    }

    @Nested
    @DisplayName("DROP and recreate index")
    class DropAndRecreate {

        @Test
        void keepsDroppedGenerationAddressableUntilGc() throws IOException {
            sqlRunner.executeDdl("CREATE INDEX idx_reused ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            awaitReadyIndex("idx_reused");
            sqlRunner.executeDdl("DROP INDEX idx_reused ON " + fqn());

            sqlRunner.executeDdl("CREATE INDEX idx_reused ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            awaitReadyIndex("idx_reused");

            try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));
                assertThat(table.refs().keySet().stream()
                        .filter(name -> name.startsWith("pangrova_index_")))
                        .hasSize(2);
            }
            try (var connection = stateStoreDataSource().getConnection();
                    var statement = connection.prepareStatement(
                            "SELECT state FROM index_descriptors WHERE table_name=? AND index_name=?")) {
                statement.setString(1, tableName);
                statement.setString(2, "idx_reused");
                try (var rows = statement.executeQuery()) {
                    var states = new java.util.ArrayList<String>();
                    while (rows.next()) {
                        states.add(rows.getString(1));
                    }
                    assertThat(states).containsExactlyInAnyOrder("DROPPED", "READY");
                }
            } catch (java.sql.SQLException failure) {
                throw new IllegalStateException("Cannot inspect retained index identities", failure);
            }
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
        @DisplayName("BTREE publishes readable multi-column artifacts and preserves NULL query results")
        void btreePublishesCompositeArtifacts() throws Exception {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (user_id, description, amount, status) VALUES (5, 'partial null', 500.0, NULL)");
            sqlRunner.executeDdl("CREATE INDEX idx_mc_btree ON " + fqn() + "(user_id, status) USING BTREE");
            awaitReadyIndex("idx_mc_btree");
            var serializer = new IndexSerializer(TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class)));
            var keys = new java.util.ArrayList<Comparable<?>>();
            try (var connection = stateStoreDataSource().getConnection();
                    var statement = connection.prepareStatement("""
                            SELECT mapping.index_file_path
                            FROM index_file_mappings AS mapping
                            JOIN index_descriptors AS descriptor
                              ON mapping.index_id = descriptor.index_id AND mapping.generation = descriptor.generation
                            WHERE descriptor.table_name = ? AND descriptor.index_name = ? AND descriptor.state = 'READY'
                            """)) {
                statement.setString(1, tableName);
                statement.setString(2, "idx_mc_btree");
                int artifacts = 0;
                try (var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        var file = new org.apache.hadoop.fs.Path(rows.getString(1));
                        try (var input = file.getFileSystem(new Configuration()).open(file)) {
                            var data = (BTreeIndexData) serializer.deserialize(input.readAllBytes()).orElseThrow();
                            data.leafPages().forEach(page -> keys.addAll(page.keys()));
                        }
                        artifacts++;
                    }
                }
                assertThat(artifacts).isPositive();
            }
            assertThat(keys).containsExactlyInAnyOrder(new CompositeIndexKey(List.of(1L, "ACTIVE")),
                    new CompositeIndexKey(List.of(2L, "INACTIVE")));
            // Native currently prunes this envelope only for Bloom. These checks
            // prove query correctness after publication, not BTree pruning.
            assertThat(sqlRunner.execute("SELECT user_id FROM " + fqn() + " WHERE status IS NULL"))
                    .extracting(row -> ((Number) row.getRaw("user_id")).longValue()).containsExactly(5L);
            assertThat(sqlRunner.execute("SELECT user_id FROM " + fqn() + " WHERE user_id = 5"))
                    .extracting(row -> ((Number) row.getRaw("user_id")).longValue()).containsExactly(5L);
        }

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
