package org.pangrova.testing.integration.e2e.index;

import org.pangrova.types.value.TypedRow;
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
 * E2E tests verifying each index type's data skipping and result correctness.
 *
 * <p>Merged from {@code IndexDataSkippingE2ETest} (allMatch assertions proving
 * query results satisfy the predicate) and {@code IndexCorrectnessInvariantE2ETest}
 * (containsAll assertions proving indexes never drop correct results).</p>
 *
 * <p>Each nested class targets one index type and contains two tests:
 * <ul>
 *   <li><b>dataSkipping</b> — creates index, queries, asserts all returned rows
 *       match the predicate (allMatch)</li>
 *   <li><b>correctnessInvariant</b> — queries without index, creates index,
 *       queries with index, asserts with-index result containsAll baseline</li>
 * </ul>
 *
 * <p>Test data: 4 separate INSERT batches via {@link IndexTestFixture} to force
 * 4 data files with distinct, non-overlapping value ranges.</p>
 *
 * <p>Validates: Requirements 9.1, 10.1–10.5, 18.1–18.10, R3.1, R3.2, R3.3, R3.4</p>
 */
@DisplayName("Index Type Verification E2E")
class IndexTypeVerificationE2ETest extends SqlE2ETestBase {

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
    // BLOOM_FILTER
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BLOOM_FILTER")
    class BloomFilter {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_bf", info);
        }

        @Test
        @DisplayName("data skipping: equality predicate returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn + "(user_id) USING BLOOM_FILTER");
            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + fqn + " WHERE user_id = 1");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    Long.valueOf(1L).equals(((Number) r.getRaw("user_id")).longValue()));
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "user_id = 1",
                    "CREATE INDEX idx_bf_corr ON " + fqn + "(user_id) USING BLOOM_FILTER");
        }
    }

    // -----------------------------------------------------------------------
    // BTREE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BTREE")
    class BTree {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_bt", info);
        }

        @Test
        @DisplayName("data skipping: range predicate returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE FORCE");
            IndexReadiness.awaitReady(sqlRunner, fqn, "idx_bt");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 100.0 AND 200.0");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                return amt >= 100.0 && amt <= 200.0;
            });
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 100.0 AND 200.0");
            sqlRunner.executeDdl("CREATE INDEX idx_bt_corr ON " + fqn + "(amount) USING BTREE FORCE");
            IndexReadiness.awaitReady(sqlRunner, fqn, "idx_bt_corr");
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 100.0 AND 200.0");
            assertThat(withIndex).containsAll(baseline);
        }

        @Test
        @DisplayName("Native B-tree skips files that connector ranges cannot exclude")
        void nativeSplitPruningUsesBTreeWithOverlappingConnectorRanges(TestInfo info) {
            String table = "t_bt_native_" + info.getTestMethod().map(m -> m.getName()).orElse("unknown")
                    + "_" + System.nanoTime();
            String nativeFqn = CATALOG + "." + SCHEMA + "." + table;
            sqlRunner.executeDdl("CREATE TABLE " + nativeFqn + " (user_id BIGINT)");
            for (int batch = 0; batch < 4; batch++) {
                int low = batch == 0 ? 1 : 0;
                int high = (batch + 1) * 100;
                sqlRunner.execute("INSERT INTO " + nativeFqn + " VALUES (" + low + "), (" + high + ")");
            }
            List<TypedRow> baseline = sqlRunner.execute(
                    "SELECT * FROM " + nativeFqn + " WHERE user_id = 1");
            assertThat(baseline).hasSize(1);

            String indexName = "idx_bt_native";
            sqlRunner.executeDdl("CREATE INDEX " + indexName + " ON " + nativeFqn
                    + "(user_id) USING BTREE FORCE");
            IndexReadiness.awaitReady(sqlRunner, nativeFqn, indexName);

            var before = nativeIndexSplitTotals();
            List<TypedRow> withIndex = sqlRunner.execute(
                    "SELECT * FROM " + nativeFqn + " WHERE user_id = 1");
            var after = nativeIndexSplitTotals();

            assertThat(withIndex).containsExactlyElementsOf(baseline);
            assertThat(after.evaluated() - before.evaluated())
                    .as("Native pre-scan B-tree evaluations").isGreaterThanOrEqualTo(4);
            assertThat(after.skipped() - before.skipped())
                    .as("Native B-tree skips with overlapping connector ranges").isGreaterThanOrEqualTo(3);
        }
    }

    // -----------------------------------------------------------------------
    // INVERTED
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("INVERTED")
    class Inverted {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_inv", info);
        }

        @Test
        @DisplayName("data skipping: text match predicate returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_inv ON " + fqn + "(description) USING INVERTED");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE description LIKE '%pangrova%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("description").toString().contains("pangrova"));
        }
    }

    // -----------------------------------------------------------------------
    // PREFIX
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("PREFIX")
    class Prefix {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_pfx", info);
        }

        @Test
        @DisplayName("data skipping: LIKE prefix predicate returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn + "(name) USING PREFIX");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE 'Ali%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("name").toString().startsWith("Ali"));
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "name LIKE 'Ali%'",
                    "CREATE INDEX idx_pfx_corr ON " + fqn + "(name) USING PREFIX");
        }
    }

    // -----------------------------------------------------------------------
    // NGRAM_BLOOM_FILTER
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("NGRAM_BLOOM_FILTER")
    class NGramBloom {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_ngram", info);
        }

        @Test
        @DisplayName("data skipping: substring predicate returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_ngram ON " + fqn
                    + "(name) USING NGRAM_BLOOM_FILTER");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE name LIKE '%lic%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("name").toString().contains("lic"));
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "name LIKE '%lic%'",
                    "CREATE INDEX idx_ngram_corr ON " + fqn + "(name) USING NGRAM_BLOOM_FILTER");
        }
    }

    // -----------------------------------------------------------------------
    // BITMAP
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BITMAP")
    class Bitmap {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_bm", info);
        }

        @Test
        @DisplayName("data skipping: low-cardinality equality returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn + "(status) USING BITMAP");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE status = 'ACTIVE'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    "ACTIVE".equals(r.getRaw("status").toString()));
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "status = 'ACTIVE'",
                    "CREATE INDEX idx_bm_corr ON " + fqn + "(status) USING BITMAP");
        }
    }

    // -----------------------------------------------------------------------
    // HASH
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("HASH")
    class Hash {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_hash", info);
        }

        @Test
        @DisplayName("data skipping: O(1) equality lookup returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_hash ON " + fqn + "(session_id) USING HASH");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE session_id = 'sess-001'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    "sess-001".equals(r.getRaw("session_id").toString()));
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "session_id = 'sess-001'",
                    "CREATE INDEX idx_hash_corr ON " + fqn + "(session_id) USING HASH");
        }
    }

    // -----------------------------------------------------------------------
    // SPARSE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("SPARSE")
    class Sparse {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_sparse", info);
        }

        @Test
        @DisplayName("data skipping: range on sorted column returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_sparse ON " + fqn + "(user_id) USING SPARSE FORCE");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id BETWEEN 1 AND 20");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long uid = ((Number) r.getRaw("user_id")).longValue();
                return uid >= 1 && uid <= 20;
            });
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "user_id BETWEEN 1 AND 20",
                    "CREATE INDEX idx_sparse_corr ON " + fqn + "(user_id) USING SPARSE FORCE");
        }
    }

    // -----------------------------------------------------------------------
    // R_TREE (spatial)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R_TREE")
    class RTree {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createSpatialTable(sqlRunner, CATALOG, SCHEMA, "t_rt", info);
        }

        @Test
        @DisplayName("data skipping: spatial range returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_rt ON " + fqn + "(x, y) USING R_TREE");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE (x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                double x = ((Number) r.getRaw("x")).doubleValue();
                double y = ((Number) r.getRaw("y")).doubleValue();
                return x >= 0.0 && x <= 10.0 && y >= 0.0 && y <= 10.0;
            });
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "(x BETWEEN 0.0 AND 10.0) AND (y BETWEEN 0.0 AND 10.0)",
                    "CREATE INDEX idx_rt_corr ON " + fqn + "(x, y) USING R_TREE");
        }
    }

    // -----------------------------------------------------------------------
    // GIN (containment)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("GIN")
    class Gin {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGinTable(sqlRunner, CATALOG, SCHEMA, "t_gin", info);
        }

        @Test
        @DisplayName("data skipping: containment predicate returns only matching rows")
        void dataSkipping() {
            sqlRunner.executeDdl("CREATE INDEX idx_gin ON " + fqn + "(tags) USING GIN");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE tags LIKE '%alpha%'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    r.getRaw("tags").toString().contains("alpha"));
        }

        @Test
        @DisplayName("correctness invariant: withIndex containsAll baseline")
        void correctnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "tags LIKE '%alpha%'",
                    "CREATE INDEX idx_gin_corr ON " + fqn + "(tags) USING GIN");
        }
    }
}
