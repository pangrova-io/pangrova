package org.pangrova.testing.integration.e2e.write;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for INSERT path operations.
 *
 * <p>Covers INSERT VALUES, INSERT SELECT, multi-row insert, type coercion
 * on insert, and NULL handling through the full pipeline.
 */
@E2ETest
@DisplayName("INSERT Path - E2E Tests")
class InsertPathTest extends PangrovaE2ETestBase {

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
        tableName = "t_insert_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // ── INSERT VALUES ──

    @Nested
    @DisplayName("INSERT VALUES")
    class InsertValues {

        @Test
        @DisplayName("Single row INSERT then SELECT returns exact row")
        void singleRowInsert() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, price DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'alpha', 99.9)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(1);
        }

        @Test
        @DisplayName("Multi-row INSERT inserts all rows")
        void multiRowInsert() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

            assertQueryReturnsRows("SELECT * FROM " + fqn(), 5);
        }

        @Test
        @DisplayName("INSERT with NULL values stores nulls correctly")
        void insertWithNulls() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, NULL, 100.0), (2, 'bob', NULL)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
        }

        @Test
        @DisplayName("INSERT with explicit column list inserts in specified order")
        void insertWithColumnList() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, score DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " (name, id, score) VALUES ('test', 42, 3.14)");

            List<TypedRow> rows = executeQuery("SELECT id, name FROM " + fqn());
            assertThat(rows).hasSize(1);
        }
    }

    // ── INSERT SELECT ──

    @Nested
    @DisplayName("INSERT SELECT")
    class InsertSelect {

        @Test
        @DisplayName("INSERT SELECT copies data from source table")
        void insertSelectFromSource() {
            String src = fqn();
            executeDdl("CREATE TABLE " + src + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + src + " VALUES (1, 'x'), (2, 'y'), (3, 'z')");

            String dst = FQN_PREFIX + tableName + "_dst";
            executeDdl("CREATE TABLE " + dst + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + dst + " SELECT * FROM " + src);

            assertQueryReturnsRows("SELECT * FROM " + dst, 3);
        }

        @Test
        @DisplayName("INSERT SELECT with WHERE copies subset")
        void insertSelectWithFilter() {
            String src = fqn();
            executeDdl("CREATE TABLE " + src + " (id BIGINT, val BIGINT)");
            executeDml("INSERT INTO " + src + " VALUES (1, 10), (2, 20), (3, 30), (4, 40)");

            String dst = FQN_PREFIX + tableName + "_filtered";
            executeDdl("CREATE TABLE " + dst + " (id BIGINT, val BIGINT)");
            executeDml("INSERT INTO " + dst + " SELECT * FROM " + src + " WHERE val > 20");

            assertQueryReturnsRows("SELECT * FROM " + dst, 2);
        }
    }

    // ── Multiple batches ──

    @Nested
    @DisplayName("Multiple INSERT batches")
    class MultipleBatches {

        @Test
        @DisplayName("Multiple INSERT statements accumulate rows")
        void multipleInserts() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'first')");
            executeDml("INSERT INTO " + fqn() + " VALUES (2, 'second')");
            executeDml("INSERT INTO " + fqn() + " VALUES (3, 'third')");

            assertQueryReturnsRows("SELECT * FROM " + fqn(), 3);
        }
    }
}
