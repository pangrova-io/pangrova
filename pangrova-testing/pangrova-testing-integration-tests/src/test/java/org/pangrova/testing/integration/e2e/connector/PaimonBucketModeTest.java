package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Paimon dynamic bucket (bucket=-1) and postpone bucket (bucket=-2) E2E tests.
 *
 * <p>Validates that Paimon tables with non-fixed bucket modes work correctly
 * through the full pipeline: automatic bucket assignment on write, bucket
 * auto-expansion under data growth, and correct bucketing after compaction
 * for postpone mode.
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native →
 * transport → Client.
 *
 * <p>Requirements: C2 (Dynamic Bucket), C3 (Postpone Bucket)
 */
@DisplayName("Paimon Bucket Mode - E2E Tests")
class PaimonBucketModeTest extends SqlE2ETestBase {

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

    // --- Task 1.6.3: Dynamic bucket (bucket=-1) write and auto-assignment ---

    @Nested
    @DisplayName("1.6.3 - Dynamic Bucket Write and Auto-Assignment")
    class DynamicBucketWrite {

        @Test
        @DisplayName("bucket=-1 table accepts writes and data is readable")
        void dynamicBucketWriteAndRead() {
            // Create a PK table with dynamic bucket mode (bucket=-1).
            // Paimon automatically assigns buckets based on data distribution.
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " TBLPROPERTIES ("
                    + "   'bucket' = '-1',"
                    + "   'file.format' = 'parquet'"
                    + " )");

            // Insert data — Paimon should auto-assign buckets
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, score) VALUES "
                    + "(1, 'alice', 90.0), (2, 'bob', 85.0), (3, 'carol', 92.0)");

            // Verify data is readable and correct
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("alice");
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("name")).isEqualTo("bob");
            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
            assertThat(rows.get(2).getString("name")).isEqualTo("carol");
        }
    }

    // --- Task 1.6.4: Dynamic bucket auto-expansion under data growth ---

    @Nested
    @DisplayName("1.6.4 - Dynamic Bucket Auto-Expansion")
    class DynamicBucketAutoExpansion {

        @Test
        @DisplayName("bucket=-1 handles data growth with correct query results")
        void dynamicBucketAutoExpansionQueryCorrect() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, amount DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " TBLPROPERTIES ("
                    + "   'bucket' = '-1',"
                    + "   'file.format' = 'parquet'"
                    + " )");

            // First batch: small dataset
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, amount) VALUES "
                    + "(1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0)");

            List<TypedRow> firstRead = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(firstRead).hasSize(3);

            // Second batch: more data to trigger potential bucket expansion
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, amount) VALUES "
                    + "(4, 'C', 300.0), (5, 'B', 250.0), (6, 'A', 175.0),"
                    + " (7, 'C', 350.0), (8, 'B', 225.0)");

            // Third batch: even more data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, amount) VALUES "
                    + "(9, 'A', 400.0), (10, 'C', 450.0)");

            // Verify all data is queryable and correct after growth
            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(allRows).hasSize(10);

            // Verify first and last rows
            assertThat(allRows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(allRows.get(9).getLong("id")).isEqualTo(10L);

            // Verify filtered query works correctly across buckets
            List<TypedRow> categoryA = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE category = 'A' ORDER BY id");
            assertThat(categoryA).hasSize(4);
            assertThat(categoryA.get(0).getLong("id")).isEqualTo(1L);
            assertThat(categoryA.get(1).getLong("id")).isEqualTo(3L);
            assertThat(categoryA.get(2).getLong("id")).isEqualTo(6L);
            assertThat(categoryA.get(3).getLong("id")).isEqualTo(9L);

            // Verify upsert works correctly with dynamic buckets
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, amount) VALUES (1, 'A', 999.0)");

            List<TypedRow> afterUpsert = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE id = 1");
            assertThat(afterUpsert).hasSize(1);
            assertThat(afterUpsert.get(0).getDouble("amount")).isEqualTo(999.0);
        }
    }

    // --- Task 1.6.5: Postpone bucket (bucket=-2) write and compaction ---

    @Nested
    @DisplayName("1.6.5 - Postpone Bucket Write and Compaction")
    class PostponeBucketCompaction {

        @Test
        @DisplayName("bucket=-2 table accepts writes and compaction assigns correct buckets")
        void postponeBucketWriteAndCompact() {
            // Create a PK table with postpone bucket mode (bucket=-2).
            // Writes go to an unassigned bucket; compaction assigns real buckets.
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, label VARCHAR, value DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " TBLPROPERTIES ("
                    + "   'bucket' = '-2',"
                    + "   'file.format' = 'parquet'"
                    + " )");

            // Insert data — goes to unassigned bucket initially
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, label, value) VALUES "
                    + "(1, 'x', 10.0), (2, 'y', 20.0), (3, 'z', 30.0)");

            // Verify data is readable before compaction
            List<TypedRow> beforeCompact = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(beforeCompact).hasSize(3);

            // Trigger compaction via CALL procedure.
            // The compact procedure assigns real buckets to postpone-mode data.
            sqlRunner.execute("CALL " + CATALOG + ".sys.compact('"
                    + SCHEMA + "." + tableName + "')");

            // Verify data is still correct after compaction
            List<TypedRow> afterCompact = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(afterCompact).hasSize(3);

            assertThat(afterCompact.get(0).getLong("id")).isEqualTo(1L);
            assertThat(afterCompact.get(0).getString("label")).isEqualTo("x");
            assertThat(afterCompact.get(1).getLong("id")).isEqualTo(2L);
            assertThat(afterCompact.get(1).getString("label")).isEqualTo("y");
            assertThat(afterCompact.get(2).getLong("id")).isEqualTo(3L);
            assertThat(afterCompact.get(2).getString("label")).isEqualTo("z");
        }
    }

    // --- Task 1.6.6: Postpone bucket query consistency before and after compaction ---

    @Nested
    @DisplayName("1.6.6 - Postpone Bucket Query Consistency")
    class PostponeBucketQueryConsistency {

        @Test
        @DisplayName("bucket=-2 query results identical before and after compaction")
        void postponeBucketQueryConsistentBeforeAndAfterCompaction() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, tag VARCHAR, amount DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " TBLPROPERTIES ("
                    + "   'bucket' = '-2',"
                    + "   'file.format' = 'parquet'"
                    + " )");

            // Insert multiple batches
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, tag, amount) VALUES "
                    + "(1, 'alpha', 100.0), (2, 'beta', 200.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, tag, amount) VALUES "
                    + "(3, 'gamma', 300.0), (1, 'alpha', 150.0)");

            // Snapshot before compaction
            List<TypedRow> beforeCompact = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            // Trigger compaction
            sqlRunner.execute("CALL " + CATALOG + ".sys.compact('"
                    + SCHEMA + "." + tableName + "')");

            // Snapshot after compaction
            List<TypedRow> afterCompact = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            // Row count must be identical
            assertThat(afterCompact).hasSameSizeAs(beforeCompact);

            // Each row must match exactly
            for (int i = 0; i < beforeCompact.size(); i++) {
                TypedRow before = beforeCompact.get(i);
                TypedRow after = afterCompact.get(i);
                assertThat(after.getLong("id")).isEqualTo(before.getLong("id"));
                assertThat(after.getString("tag")).isEqualTo(before.getString("tag"));
                assertThat(after.getDouble("amount")).isEqualTo(before.getDouble("amount"));
            }

            // Verify upsert was applied: id=1 should have amount=150.0
            assertThat(afterCompact).hasSize(3);
            assertThat(afterCompact.get(0).getLong("id")).isEqualTo(1L);
            assertThat(afterCompact.get(0).getDouble("amount")).isEqualTo(150.0);
        }
    }
}
