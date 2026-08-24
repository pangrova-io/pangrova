package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for Paimon file-level indexes: bloom filter, bitmap, and BSI.
 *
 * <p>Validates that:
 * 1. Tables with bloom-filter.columns enable point query filtering
 * 2. Tables with bitmap.columns enable low-cardinality column filtering
 * 3. Tables with bsi.columns enable numeric range filtering
 * 4. rewrite_file_index procedure rebuilds indexes on existing tables
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
        @DisplayName("Bloom filter index property visible in $options")
        void bloomFilterPropertyInOptions() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, email VARCHAR, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bloom-filter.columns' = 'email')");

            List<TypedRow> allOptions = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$options");
            List<TypedRow> options = allOptions.stream()
                    .filter(r -> "bloom-filter.columns".equals(
                            String.valueOf(r.getRaw("key"))))
                    .collect(Collectors.toList());
            assertThat(options).hasSize(1);
            assertThat(options.get(0).getRaw("value")).isEqualTo("email");
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
        @DisplayName("Bitmap index property visible in $options")
        void bitmapPropertyInOptions() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bitmap.columns' = 'category')");

            List<TypedRow> allOptions = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$options");
            List<TypedRow> options = allOptions.stream()
                    .filter(r -> "bitmap.columns".equals(
                            String.valueOf(r.getRaw("key"))))
                    .collect(Collectors.toList());
            assertThat(options).hasSize(1);
            assertThat(options.get(0).getRaw("value")).isEqualTo("category");
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
        @DisplayName("BSI index property visible in $options")
        void bsiPropertyInOptions() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, score INT, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bsi.columns' = 'score')");

            List<TypedRow> allOptions = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$options");
            List<TypedRow> options = allOptions.stream()
                    .filter(r -> "bsi.columns".equals(
                            String.valueOf(r.getRaw("key"))))
                    .collect(Collectors.toList());
            assertThat(options).hasSize(1);
            assertThat(options.get(0).getRaw("value")).isEqualTo("score");
        }
    }

    // --- 2.12.7: rewrite_file_index procedure ---

    @Nested
    @DisplayName("2.12.7 - rewrite_file_index Procedure")
    class RewriteFileIndex {

        @Test
        @DisplayName("CALL rewrite_file_index rebuilds indexes and table remains queryable")
        void rewriteFileIndexProcedure() {
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

            // Rebuild file indexes
            sqlRunner.execute("CALL " + CATALOG
                    + ".rewrite_file_index('" + tableName + "')");

            // Verify table is still queryable after index rebuild
            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(allRows).hasSize(10);

            // Verify point query still works after rebuild
            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn()
                            + " WHERE name = 'user_5'");
            assertThat(filtered).hasSize(1);
            assertThat(((Number) filtered.get(0).getRaw("id")).longValue()).isEqualTo(5L);
        }

        @Test
        @DisplayName("rewrite_file_index preserves data integrity")
        void rewriteFileIndexPreservesData() {
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

            sqlRunner.execute("CALL " + CATALOG
                    + ".rewrite_file_index('" + tableName + "')");

            long sumAfter = sqlRunner.execute(
                    "SELECT id, value FROM " + fqn())
                    .stream()
                    .mapToLong(r -> ((Number) r.getRaw("value")).longValue())
                    .sum();

            assertThat(sumAfter).isEqualTo(sumBefore);
        }
    }
}
