package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * Iceberg v3 format-version E2E tests: table creation, full DML operations,
 * and mixed v2/v3 JOIN queries.
 *
 * <p>Covers D1 acceptance criteria: v3 table creation, full operations,
 * and v2/v3 mixed queries. All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 *
 * <p>Requirements: D1
 */
@DisplayName("Iceberg v3 Format Version E2E Tests")
class IcebergV3FormatTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
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

    private String fqn(String name) {
        return FQN_PREFIX + name;
    }

    // --- 1.1.3: v3 table creation with INSERT and SELECT ---

    @Nested
    @DisplayName("1.1.3 - v3 Table CREATE, INSERT, SELECT")
    class V3CreateInsertSelect {

        @Test
        @DisplayName("Create v3 table, insert rows, and select them back")
        void createV3TableInsertAndSelect() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(0).getRaw("amount")).doubleValue()).isEqualTo(100.0);
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(3L);
            assertThat(rows.get(2).getRaw("name")).isEqualTo("carol");
        }

        @Test
        @DisplayName("Multiple inserts into v3 table accumulate correctly")
        void multipleInsertsAccumulate() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (1, 'first')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (2, 'second')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("val")).isEqualTo("first");
            assertThat(rows.get(1).getRaw("val")).isEqualTo("second");
        }

        @Test
        @DisplayName("Select with WHERE predicate on v3 table")
        void selectWithPredicateOnV3Table() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, price DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, price) VALUES "
                    + "(1, 'electronics', 999.99), "
                    + "(2, 'books', 19.99), "
                    + "(3, 'electronics', 499.99)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE category = 'electronics' ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows).allSatisfy(row ->
                    assertThat(row.getRaw("category")).isEqualTo("electronics"));
        }
    }

    // --- 1.1.4: Full DML operations on v3 table ---

    @Nested
    @DisplayName("1.1.4 - v3 Table Full DML Operations")
    class V3FullDmlOperations {

        @Test
        @DisplayName("INSERT INTO v3 table")
        void insertIntoV3Table() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, score) VALUES "
                    + "(1, 'alice', 85.0), (2, 'bob', 92.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(1).getRaw("score")).doubleValue()).isEqualTo(92.0);
        }

        @Test
        @DisplayName("DELETE FROM v3 table")
        void deleteFromV3Table() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 2");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        }

        @Test
        @DisplayName("UPDATE on v3 table")
        void updateV3Table() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0)");

            sqlRunner.execute("UPDATE " + fqn()
                    + " SET amount = 150.0 WHERE id = 1");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("amount")).doubleValue()).isEqualTo(150.0);
            assertThat(((Number) rows.get(1).getRaw("amount")).doubleValue()).isEqualTo(200.0);
        }

        @Test
        @DisplayName("MERGE INTO on v3 table")
        void mergeIntoV3Table() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0)");

            String srcTable = tableName + "_src";
            String srcFqn = fqn(srcTable);
            sqlRunner.executeDdl("CREATE TABLE " + srcFqn
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");
            sqlRunner.execute("INSERT INTO " + srcFqn
                    + " VALUES (2, 'bob_v2', 250.0), (3, 'carol', 300.0)");

            sqlRunner.execute("MERGE INTO " + fqn() + " t "
                    + "USING " + srcFqn + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, amount) "
                    + "VALUES (s.id, s.name, s.amount)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRow(rows.get(0), 1L, "alice", 100.0);
            assertRow(rows.get(1), 2L, "bob_v2", 250.0);
            assertRow(rows.get(2), 3L, "carol", 300.0);
        }

        @Test
        @DisplayName("SELECT with aggregation on v3 table")
        void selectWithAggregationOnV3Table() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, amount DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, amount) VALUES "
                    + "(1, 'A', 10.0), (2, 'B', 20.0), "
                    + "(3, 'A', 30.0), (4, 'B', 40.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT category, SUM(amount) AS total FROM " + fqn()
                    + " GROUP BY category ORDER BY category");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("category")).isEqualTo("A");
            assertThat(((Number) rows.get(0).getRaw("total")).doubleValue()).isEqualTo(40.0);
            assertThat(rows.get(1).getRaw("category")).isEqualTo("B");
            assertThat(((Number) rows.get(1).getRaw("total")).doubleValue()).isEqualTo(60.0);
        }
    }

    // --- 1.1.5: Mixed v2/v3 table JOIN ---

    @Nested
    @DisplayName("1.1.5 - v2 and v3 Mixed JOIN Queries")
    class V2V3MixedJoin {

        @Test
        @DisplayName("JOIN between v2 and v3 tables returns correct results")
        void joinV2AndV3Tables() {
            String v2Table = tableName + "_v2";
            String v3Table = tableName + "_v3";

            // v2 table (default format-version=2)
            sqlRunner.executeDdl("CREATE TABLE " + fqn(v2Table)
                    + " (id BIGINT, name VARCHAR)");

            // v3 table
            sqlRunner.executeDdl("CREATE TABLE " + fqn(v3Table)
                    + " (id BIGINT, department VARCHAR, salary DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn(v2Table)
                    + " (id, name) VALUES "
                    + "(1, 'alice'), (2, 'bob'), (3, 'carol')");

            sqlRunner.execute("INSERT INTO " + fqn(v3Table)
                    + " (id, department, salary) VALUES "
                    + "(1, 'engineering', 120000.0), "
                    + "(2, 'marketing', 95000.0), "
                    + "(4, 'sales', 85000.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT v2.name, v3.department, v3.salary "
                    + "FROM " + fqn(v2Table) + " v2 "
                    + "INNER JOIN " + fqn(v3Table) + " v3 ON v2.id = v3.id "
                    + "ORDER BY v2.name");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(rows.get(0).getRaw("department")).isEqualTo("engineering");
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(rows.get(1).getRaw("department")).isEqualTo("marketing");
        }

        @Test
        @DisplayName("LEFT JOIN between v2 and v3 tables preserves unmatched rows")
        void leftJoinV2AndV3Tables() {
            String v2Table = tableName + "_v2";
            String v3Table = tableName + "_v3";

            sqlRunner.executeDdl("CREATE TABLE " + fqn(v2Table)
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.executeDdl("CREATE TABLE " + fqn(v3Table)
                    + " (id BIGINT, score DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn(v2Table)
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");
            sqlRunner.execute("INSERT INTO " + fqn(v3Table)
                    + " (id, score) VALUES (1, 95.0), (3, 88.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT v2.id, v2.name, v3.score "
                    + "FROM " + fqn(v2Table) + " v2 "
                    + "LEFT JOIN " + fqn(v3Table) + " v3 ON v2.id = v3.id "
                    + "ORDER BY v2.id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(0).getRaw("score")).doubleValue()).isEqualTo(95.0);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(rows.get(1).getRaw("score")).isNull();
            assertThat(rows.get(2).getRaw("name")).isEqualTo("carol");
            assertThat(((Number) rows.get(2).getRaw("score")).doubleValue()).isEqualTo(88.0);
        }

        @Test
        @DisplayName("Aggregation over JOIN of v2 and v3 tables")
        void aggregationOverV2V3Join() {
            String v2Table = tableName + "_v2";
            String v3Table = tableName + "_v3";

            sqlRunner.executeDdl("CREATE TABLE " + fqn(v2Table)
                    + " (id BIGINT, region VARCHAR)");
            sqlRunner.executeDdl("CREATE TABLE " + fqn(v3Table)
                    + " (id BIGINT, revenue DOUBLE)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + fqn(v2Table)
                    + " (id, region) VALUES "
                    + "(1, 'east'), (2, 'west'), (3, 'east'), (4, 'west')");
            sqlRunner.execute("INSERT INTO " + fqn(v3Table)
                    + " (id, revenue) VALUES "
                    + "(1, 100.0), (2, 200.0), (3, 300.0), (4, 400.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT v2.region, SUM(v3.revenue) AS total_revenue "
                    + "FROM " + fqn(v2Table) + " v2 "
                    + "INNER JOIN " + fqn(v3Table) + " v3 ON v2.id = v3.id "
                    + "GROUP BY v2.region "
                    + "ORDER BY v2.region");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("region")).isEqualTo("east");
            assertThat(((Number) rows.get(0).getRaw("total_revenue")).doubleValue())
                    .isEqualTo(400.0);
            assertThat(rows.get(1).getRaw("region")).isEqualTo("west");
            assertThat(((Number) rows.get(1).getRaw("total_revenue")).doubleValue())
                    .isEqualTo(600.0);
        }
    }

    // --- Assertion helper ---

    private static void assertRow(TypedRow row,
                                  long expectedId, String expectedName,
                                  double expectedAmount) {
        assertThat(((Number) row.getRaw("id")).longValue()).isEqualTo(expectedId);
        assertThat(row.getRaw("name")).isEqualTo(expectedName);
        assertThat(((Number) row.getRaw("amount")).doubleValue()).isEqualTo(expectedAmount);
    }
}
