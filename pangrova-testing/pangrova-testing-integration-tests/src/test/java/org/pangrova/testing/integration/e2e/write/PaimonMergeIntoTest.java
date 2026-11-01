package org.pangrova.testing.integration.e2e.write;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for MERGE INTO on Paimon primary-key tables.
 *
 * <p>Covers Requirements 108.1-108.4. All operations execute through the real
 * pipeline: HTTP → Coordinator → Worker → Native → Paimon Storage → transport.
 */
@DisplayName("Paimon Merge Into - E2E Tests")
class PaimonMergeIntoTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_merge";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String targetTable;
    private String sourceTable;

    @BeforeEach
    void assignUniqueTableNames(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long suffix = System.nanoTime();
        targetTable = "t_target_" + method + "_" + suffix;
        sourceTable = "t_source_" + method + "_" + suffix;
    }

    private String targetFqn() {
        return FQN_PREFIX + targetTable;
    }

    private String sourceFqn() {
        return FQN_PREFIX + sourceTable;
    }

    /** Create target table with PK and 3 rows: (1,'alice',100), (2,'bob',200), (3,'carol',300). */
    private void seedTarget() {
        sqlRunner.executeDdl("CREATE TABLE " + targetFqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE,"
                + " PRIMARY KEY (id) NOT ENFORCED)"
                + " WITH ('merge-engine' = 'deduplicate', 'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + targetFqn() + " (id, name, amount) VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");
    }

    /** Create source table with given column definitions and insert values. */
    private void seedSource(String columnDefs, String values) {
        sqlRunner.executeDdl("CREATE TABLE " + sourceFqn()
                + " (" + columnDefs + ", PRIMARY KEY (id) NOT ENFORCED)"
                + " WITH ('merge-engine' = 'deduplicate', 'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + sourceFqn() + " VALUES " + values);
    }

    // --- 108.1: MERGE INTO UPDATE ---

    @Nested
    @DisplayName("108.1 - MERGE INTO UPDATE on Paimon PK table")
    class MergeIntoUpdate {

        @Test
        @DisplayName("MERGE INTO updates matched rows on Paimon primary-key table")
        void mergeUpdateMatchedRows() {
            seedTarget();
            seedSource("id BIGINT, name VARCHAR, amount DOUBLE",
                    "(2, 'bob_updated', 999.0)");

            sqlRunner.execute("MERGE INTO " + targetFqn() + " t "
                    + "USING " + sourceFqn() + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + targetFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRowValues(rows.get(0), 1L, "alice", 100.0);
            assertRowValues(rows.get(1), 2L, "bob_updated", 999.0);
            assertRowValues(rows.get(2), 3L, "carol", 300.0);
        }
    }

    // --- 108.2: MERGE INTO INSERT ---

    @Nested
    @DisplayName("108.2 - MERGE INTO INSERT on Paimon PK table")
    class MergeIntoInsert {

        @Test
        @DisplayName("MERGE INTO inserts unmatched rows on Paimon primary-key table")
        void mergeInsertUnmatchedRows() {
            seedTarget();
            seedSource("id BIGINT, name VARCHAR, amount DOUBLE",
                    "(4, 'dave', 400.0), (5, 'eve', 500.0)");

            sqlRunner.execute("MERGE INTO " + targetFqn() + " t "
                    + "USING " + sourceFqn() + " s ON t.id = s.id "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, amount) "
                    + "VALUES (s.id, s.name, s.amount)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + targetFqn() + " ORDER BY id");

            assertThat(rows).hasSize(5);
            assertRowValues(rows.get(0), 1L, "alice", 100.0);
            assertRowValues(rows.get(1), 2L, "bob", 200.0);
            assertRowValues(rows.get(2), 3L, "carol", 300.0);
            assertRowValues(rows.get(3), 4L, "dave", 400.0);
            assertRowValues(rows.get(4), 5L, "eve", 500.0);
        }
    }

    // --- 108.3: MERGE INTO DELETE ---

    @Nested
    @DisplayName("108.3 - MERGE INTO DELETE on Paimon PK table")
    class MergeIntoDelete {

        @Test
        @DisplayName("MERGE INTO deletes matched rows on Paimon primary-key table")
        void mergeDeleteMatchedRows() {
            seedTarget();
            seedSource("id BIGINT, name VARCHAR, amount DOUBLE",
                    "(2, 'bob', 200.0), (3, 'carol', 300.0)");

            sqlRunner.execute("MERGE INTO " + targetFqn() + " t "
                    + "USING " + sourceFqn() + " s ON t.id = s.id "
                    + "WHEN MATCHED AND t.amount >= 200.0 THEN DELETE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + targetFqn() + " ORDER BY id");

            assertThat(rows).hasSize(1);
            assertRowValues(rows.get(0), 1L, "alice", 100.0);
        }
    }

    // --- 108.4: MERGE INTO complete (UPDATE + INSERT + DELETE) ---

    @Nested
    @DisplayName("108.4 - MERGE INTO Complete on Paimon PK table")
    class MergeIntoComplete {

        @Test
        @DisplayName("MERGE with multiple actions fails before changing the table")
        void mergeCompleteStatement() {
            seedTarget();
            seedSource("id BIGINT, name VARCHAR, amount DOUBLE",
                    "(2, 'bob_v2', 250.0), (3, 'carol_del', 999.0), (4, 'dave', 400.0)");

            var error = sqlRunner.executeExpectingError("MERGE INTO " + targetFqn() + " t "
                    + "USING " + sourceFqn() + " s ON t.id = s.id "
                    + "WHEN MATCHED AND s.amount > 500.0 THEN DELETE "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, amount) "
                    + "VALUES (s.id, s.name, s.amount)");
            assertThat(error.message()).contains("one atomic native write");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + targetFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRowValues(rows.get(0), 1L, "alice", 100.0);
            assertRowValues(rows.get(1), 2L, "bob", 200.0);
            assertRowValues(rows.get(2), 3L, "carol", 300.0);
        }
    }

    private static void assertRowValues(TypedRow row,
                                        long expectedId, String expectedName,
                                        double expectedAmount) {
        assertThat(row.getLong("id")).isEqualTo(expectedId);
        assertThat(row.getString("name")).isEqualTo(expectedName);
        assertThat(row.getDouble("amount")).isEqualTo(expectedAmount);
    }
}
