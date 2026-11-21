package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E tests for Paimon file-level indexes: bloom filter, bitmap, and BSI.
 *
 * <p>Validates that:
 * 1. Tables persist bloom-filter.columns and return correct point queries
 * 2. Tables persist bitmap.columns and return correct low-cardinality filters
 * 3. Tables persist bsi.columns and return correct numeric range filters
 * 4. Unsupported rewrite_file_index requests fail without changing the table
 *
 * <p>Requirements: C10 (File Index Bloom/Bitmap/BSI)
 */
@DisplayName("Paimon File Index (Bloom/Bitmap/BSI) - E2E Tests")
class PaimonFileIndexTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private Map<String, String> persistedOptions() {
        return sqlRunner.execute("SELECT * FROM " + fqn() + "$options").stream()
                .collect(Collectors.toMap(row -> row.getString("key"),
                        row -> row.getString("value")));
    }

    private void rejectUnsupportedRewrite() {
        long snapshotBefore = sqlRunner.execute("SELECT snapshot_id FROM " + fqn()
                + "$snapshots ORDER BY snapshot_id DESC LIMIT 1").get(0).getLong("snapshot_id");

        assertThatThrownBy(() -> sqlRunner.execute("CALL " + CATALOG
                + ".rewrite_file_index('" + SCHEMA + "." + tableName + "')"))
                .hasMessageContaining("rewrite_file_index")
                .hasMessageContaining("has no table-control maintenance policy");

        assertThat(sqlRunner.execute("SELECT snapshot_id FROM " + fqn()
                + "$snapshots ORDER BY snapshot_id DESC LIMIT 1").get(0).getLong("snapshot_id"))
                .isEqualTo(snapshotBefore);
    }

    // --- 2.12.4: bloom-filter.columns point query filtering ---

    @Nested
    @DisplayName("2.12.4 - Bloom Filter Index Point Query")
    class BloomFilterIndex {

        @Test
        @DisplayName("Table with bloom-filter.columns supports point query filtering")
        void bloomFilterPointQuery() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bloom-filter.columns' = 'name')");

            for (int i = 0; i < 20; i++) {
                sqlRunner.execute("INSERT INTO " + fqn()
                        + " (id, name) VALUES (" + i + ", 'user_" + i + "')");
            }

            // Point query on bloom filter column — should leverage index
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn()
                            + " WHERE name = 'user_7'");
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("user_7");
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(7L);
        }

        @Test
        @DisplayName("Bloom filter index property persists in table metadata")
        void bloomFilterPropertyInOptions() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, email VARCHAR, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bloom-filter.columns' = 'email')");

            assertThat(persistedOptions()).containsEntry("bloom-filter.columns", "email");
        }
    }

    // --- 2.12.5: bitmap.columns low-cardinality column filtering ---

    @Nested
    @DisplayName("2.12.5 - Bitmap Index Low-Cardinality Filtering")
    class BitmapIndex {

        @Test
        @DisplayName("Table with bitmap.columns supports low-cardinality filtering")
        void bitmapLowCardinalityFilter() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, status VARCHAR, amount INT,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bitmap.columns' = 'status')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (1, 'active', 100)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (2, 'inactive', 200)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (3, 'active', 300)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (4, 'pending', 400)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (5, 'active', 500)");

            // Filter on bitmap-indexed low-cardinality column
            List<TypedRow> activeRows = sqlRunner.execute(
                    "SELECT id, status, amount FROM " + fqn()
                            + " WHERE status = 'active' ORDER BY id");
            assertThat(activeRows).hasSize(3);
            for (TypedRow row : activeRows) {
                assertThat(row.getRaw("status")).isEqualTo("active");
            }
        }

        @Test
        @DisplayName("Bitmap index property persists in table metadata")
        void bitmapPropertyInOptions() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bitmap.columns' = 'category')");

            assertThat(persistedOptions()).containsEntry("bitmap.columns", "category");
        }
    }

    // --- 2.12.6: bsi.columns numeric range filtering ---

    @Nested
    @DisplayName("2.12.6 - BSI Index Numeric Range Filtering")
    class BsiIndex {

        @Test
        @DisplayName("Table with bsi.columns supports numeric range filtering")
        void bsiNumericRangeFilter() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, amount BIGINT, label VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bsi.columns' = 'amount')");

            for (int i = 1; i <= 20; i++) {
                sqlRunner.execute("INSERT INTO " + fqn()
                        + " (id, amount, label) VALUES ("
                        + i + ", " + (i * 100) + ", 'item_" + i + "')");
            }

            // Range query on BSI-indexed numeric column
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, amount, label FROM " + fqn()
                            + " WHERE amount >= 500 AND amount <= 1000 ORDER BY id");
            assertThat(rows).hasSize(6);  // ids 5-10 → amounts 500-1000
            for (TypedRow row : rows) {
                long amount = ((Number) row.getRaw("amount")).longValue();
                assertThat(amount).isBetween(500L, 1000L);
            }
        }

        @Test
        @DisplayName("BSI index property persists in table metadata")
        void bsiPropertyInOptions() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, score INT, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bsi.columns' = 'score')");

            assertThat(persistedOptions()).containsEntry("bsi.columns", "score");
        }
    }

    // --- 2.12.7: rewrite_file_index procedure ---

    @Nested
    @DisplayName("2.12.7 - rewrite_file_index Procedure")
    class RewriteFileIndex {

        @Test
        @DisplayName("Unsupported rewrite_file_index is rejected and table remains queryable")
        void rewriteFileIndexIsRejected() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, score INT,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bloom-filter.columns' = 'name',"
                    + " 'bsi.columns' = 'score')");

            for (int i = 1; i <= 10; i++) {
                sqlRunner.execute("INSERT INTO " + fqn()
                        + " (id, name, score) VALUES ("
                        + i + ", 'user_" + i + "', " + (i * 10) + ")");
            }

            rejectUnsupportedRewrite();

            // The rejected procedure must leave the original table queryable.
            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(allRows).hasSize(10);

            // Preserve the original point-query result as well.
            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn()
                            + " WHERE name = 'user_5'");
            assertThat(filtered).hasSize(1);
            assertThat(((Number) filtered.get(0).getRaw("id")).longValue()).isEqualTo(5L);
        }

        @Test
        @DisplayName("Rejected rewrite_file_index preserves data integrity")
        void rejectedRewriteFileIndexPreservesData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, value INT, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bitmap.columns' = 'value')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value) VALUES (1, 10), (2, 20), (3, 30)");

            long sumBefore = sqlRunner.execute(
                    "SELECT id, value FROM " + fqn())
                    .stream()
                    .mapToLong(r -> ((Number) r.getRaw("value")).longValue())
                    .sum();

            rejectUnsupportedRewrite();

            long sumAfter = sqlRunner.execute(
                    "SELECT id, value FROM " + fqn())
                    .stream()
                    .mapToLong(r -> ((Number) r.getRaw("value")).longValue())
                    .sum();

            assertThat(sumAfter).isEqualTo(sumBefore);
        }
    }
}
