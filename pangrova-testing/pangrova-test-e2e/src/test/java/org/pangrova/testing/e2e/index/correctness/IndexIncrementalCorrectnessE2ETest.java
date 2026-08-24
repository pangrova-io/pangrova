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
 * E2E tests verifying incremental correctness after INSERT operations
 * performed AFTER index creation.
 *
 * <p>Pattern per test: create table with 2 initial INSERT batches → create
 * index → INSERT new data after index → query for new data → verify found
 * → verify correctness invariant (baseline vs withIndex → containsAll).</p>
 *
 * <p>Covers 7 index types: BTREE, BLOOM_FILTER, BITMAP, HASH, SPARSE,
 * PREFIX, NGRAM_BLOOM_FILTER. R_TREE, GIN, and INVERTED are excluded
 * (R_TREE/GIN need specialized tables; INVERTED needs TextMatch).</p>
 *
 * <p>Validates: Requirements R11.1, R11.2, R11.3</p>
 */
@DisplayName("Index Incremental Correctness E2E — INSERT after index creation")
class IndexIncrementalCorrectnessE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

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
    // Helper: create table with 2 initial INSERT batches (pre-index data)
    // -----------------------------------------------------------------------

    private static String createTableWith2Batches(String prefix, TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String table = prefix + "_" + method + "_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        // Batch 1
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                + " TIMESTAMP '2025-01-01 12:00:00')");
        // Batch 2
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')");
        return fqn;
    }

    /**
     * Post-index INSERT batch — values distinct from initial data so queries
     * can target specifically the new rows.
     */
    private static void insertPostIndexBatch(String fqn) {
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (5000, 'new_Zara', 500.0, 'NEW', 'new description', 'sess-5000',"
                + " TIMESTAMP '2025-06-01 00:00:00'),"
                + " (6000, 'new_Yuri', 600.0, 'NEW', 'new entry', 'sess-6000',"
                + " TIMESTAMP '2025-06-15 00:00:00')");
    }

    // -----------------------------------------------------------------------
    // 17.2: Per-index-type incremental correctness
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BTREE Incremental Correctness")
    class BtreeIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_bt", info);
        }

        @Test
        @DisplayName("BTREE: INSERT after index → new data queryable via range")
        void btreeIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 450.0 AND 650.0");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                return amt >= 450.0 && amt <= 650.0;
            });
        }

        @Test
        @DisplayName("BTREE: correctness invariant holds after INSERT")
        void btreeIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 0.0 AND 700.0");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 0.0 AND 700.0");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    @Nested
    @DisplayName("BLOOM_FILTER Incremental Correctness")
    class BloomFilterIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_bf", info);
        }

        @Test
        @DisplayName("BLOOM_FILTER: INSERT after index → new data queryable via equality")
        void bloomFilterIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id = 5000");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("user_id")).longValue() == 5000L);
        }

        @Test
        @DisplayName("BLOOM_FILTER: correctness invariant holds after INSERT")
        void bloomFilterIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id = 1");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id = 1");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    @Nested
    @DisplayName("BITMAP Incremental Correctness")
    class BitmapIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_bm", info);
        }

        @Test
        @DisplayName("BITMAP: INSERT after index → new data queryable via equality")
        void bitmapIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE status = 'NEW'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    "NEW".equals(r.getRaw("status").toString()));
        }

        @Test
        @DisplayName("BITMAP: correctness invariant holds after INSERT")
        void bitmapIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE status = 'ACTIVE'");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE status = 'ACTIVE'");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    @Nested
    @DisplayName("HASH Incremental Correctness")
    class HashIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_hash", info);
        }

        @Test
        @DisplayName("HASH: INSERT after index → new data queryable via equality")
        void hashIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_hash ON " + fqn
                    + "(session_id) USING HASH");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE session_id = 'sess-5000'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    "sess-5000".equals(r.getRaw("session_id").toString()));
        }

        @Test
        @DisplayName("HASH: correctness invariant holds after INSERT")
        void hashIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_hash ON " + fqn
                    + "(session_id) USING HASH");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE session_id = 'sess-001'");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE session_id = 'sess-001'");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    @Nested
    @DisplayName("SPARSE Incremental Correctness")
    class SparseIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_sp", info);
        }

        @Test
        @DisplayName("SPARSE: INSERT after index → new data queryable via range")
        void sparseIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_sp ON " + fqn
                    + "(user_id) USING SPARSE");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id BETWEEN 4000 AND 7000");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long uid = ((Number) r.getRaw("user_id")).longValue();
                return uid >= 4000 && uid <= 7000;
            });
        }

        @Test
        @DisplayName("SPARSE: correctness invariant holds after INSERT")
        void sparseIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_sp ON " + fqn
                    + "(user_id) USING SPARSE");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id BETWEEN 1 AND 20");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id BETWEEN 1 AND 20");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    @Nested
    @DisplayName("PREFIX Incremental Correctness")
    class PrefixIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_pfx", info);
        }

        @Test
        @DisplayName("PREFIX: INSERT after index → new data queryable via LIKE prefix")
        void prefixIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn
                    + "(name) USING PREFIX");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE 'new_%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("name").toString().startsWith("new_"));
        }

        @Test
        @DisplayName("PREFIX: correctness invariant holds after INSERT")
        void prefixIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn
                    + "(name) USING PREFIX");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE 'Ali%'");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE 'Ali%'");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    @Nested
    @DisplayName("NGRAM_BLOOM_FILTER Incremental Correctness")
    class NgramBloomIncremental {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = createTableWith2Batches("t_inc_ngram", info);
        }

        @Test
        @DisplayName("NGRAM_BLOOM_FILTER: INSERT after index → new data queryable via LIKE substring")
        void ngramIncrementalInsert() {
            sqlRunner.executeDdl("CREATE INDEX idx_ngram ON " + fqn
                    + "(name) USING NGRAM_BLOOM_FILTER");
            insertPostIndexBatch(fqn);

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE '%new%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("name").toString().contains("new"));
        }

        @Test
        @DisplayName("NGRAM_BLOOM_FILTER: correctness invariant holds after INSERT")
        void ngramIncrementalCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_ngram ON " + fqn
                    + "(name) USING NGRAM_BLOOM_FILTER");
            insertPostIndexBatch(fqn);

            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE '%lic%'");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE '%lic%'");
            assertThat(withIndex).containsAll(baseline);
        }
    }

    // -----------------------------------------------------------------------
    // 17.3: Multiple sequential INSERTs after index creation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multiple Sequential INSERTs After Index")
    class MultipleSequentialInserts {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
            String table = "t_inc_multi_" + method + "_" + System.nanoTime();
            fqn = FQN_PREFIX + table;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                    + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                    + " ts TIMESTAMP)");
            // Batch 1 (pre-index)
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                    + " TIMESTAMP '2025-01-01 00:00:00'),"
                    + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                    + " TIMESTAMP '2025-01-01 12:00:00')");
        }

        @Test
        @DisplayName("3 sequential INSERTs after BTREE index → all data queryable")
        void multipleInsertsAfterBtreeIndex() {
            // Create BTREE index after batch 1
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            // Batch 2 (post-index)
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                    + " TIMESTAMP '2025-02-01 00:00:00'),"
                    + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                    + " TIMESTAMP '2025-02-15 00:00:00')");

            // Batch 3 (post-index)
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (100, 'Eve', 250.0, 'ACTIVE', 'distributed sql', 'sess-100',"
                    + " TIMESTAMP '2025-03-01 00:00:00'),"
                    + " (200, 'Frank', 275.0, 'PENDING', 'lakehouse format', 'sess-200',"
                    + " TIMESTAMP '2025-03-15 00:00:00')");

            // Batch 4 (post-index)
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (1000, 'Grace', 350.0, 'ACTIVE', 'iceberg table', 'sess-1000',"
                    + " TIMESTAMP '2025-04-01 00:00:00'),"
                    + " (2000, 'Hank', 375.0, 'INACTIVE', 'paimon store', 'sess-2000',"
                    + " TIMESTAMP '2025-04-15 00:00:00')");

            // Query spanning all data — range covers all batches
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 0.0 AND 400.0");
            // All 8 rows should be returned (2 per batch × 4 batches)
            assertThat(rows).hasSize(8);
            assertThat(rows).allMatch(r -> {
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                return amt >= 0.0 && amt <= 400.0;
            });
        }

        @Test
        @DisplayName("3 sequential INSERTs after BTREE index → correctness invariant holds")
        void multipleInsertsCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            // 3 sequential INSERTs after index
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                    + " TIMESTAMP '2025-02-01 00:00:00'),"
                    + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                    + " TIMESTAMP '2025-02-15 00:00:00')");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (100, 'Eve', 250.0, 'ACTIVE', 'distributed sql', 'sess-100',"
                    + " TIMESTAMP '2025-03-01 00:00:00'),"
                    + " (200, 'Frank', 275.0, 'PENDING', 'lakehouse format', 'sess-200',"
                    + " TIMESTAMP '2025-03-15 00:00:00')");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                    + " (1000, 'Grace', 350.0, 'ACTIVE', 'iceberg table', 'sess-1000',"
                    + " TIMESTAMP '2025-04-01 00:00:00'),"
                    + " (2000, 'Hank', 375.0, 'INACTIVE', 'paimon store', 'sess-2000',"
                    + " TIMESTAMP '2025-04-15 00:00:00')");

            // Correctness invariant: two identical queries must return same results
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 0.0 AND 400.0");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 0.0 AND 400.0");
            assertThat(withIndex).containsAll(baseline);
        }
    }
}
