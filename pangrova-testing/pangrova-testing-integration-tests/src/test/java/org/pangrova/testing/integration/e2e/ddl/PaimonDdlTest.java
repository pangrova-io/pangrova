package org.pangrova.testing.integration.e2e.ddl;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for DDL operations on Paimon tables.
 *
 * <p>Covers Requirements 109.1-109.5. All DDL operations execute through the real
 * pipeline: HTTP → Coordinator → Worker → Native → Paimon Storage → transport.
 */
@DisplayName("Paimon DDL - E2E Tests")
class PaimonDdlTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_ddl";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA, paimonWarehouse().toString())));
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

    // --- 109.1: CREATE TABLE with PK + merge-engine=deduplicate ---

    @Nested
    @DisplayName("109.1 - CREATE TABLE with PK and deduplicate merge engine")
    class CreateTableDeduplicate {

        @Test
        @DisplayName("CREATE TABLE with PK and deduplicate is queryable after INSERT")
        void createDeduplicateTableAndQuery() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('merge-engine' = 'deduplicate')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES (1, 'alpha', 10.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn());

            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        }
    }

    // --- 109.2: CREATE TABLE with PK + merge-engine=partial-update ---

    @Nested
    @DisplayName("109.2 - CREATE TABLE with PK and partial-update merge engine")
    class CreateTablePartialUpdate {

        @Test
        @DisplayName("CREATE TABLE with partial-update engine is queryable")
        void createPartialUpdateTableAndQuery() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('merge-engine' = 'partial-update')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn());

            assertThat(rows).isEmpty();
        }
    }

    // --- 109.3: ALTER TABLE ADD COLUMN ---

    @Nested
    @DisplayName("109.3 - ALTER TABLE ADD COLUMN on Paimon table")
    class AlterTableAddColumn {

        @Test
        @DisplayName("ADD COLUMN makes new column visible via SELECT")
        void addColumnVisible() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " ADD COLUMN extra VARCHAR");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, extra) VALUES (1, 'alpha', 'bonus')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("extra")).isEqualTo("bonus");
        }
    }

    // --- 109.4: DROP TABLE ---

    @Nested
    @DisplayName("109.4 - DROP TABLE on Paimon table")
    class DropTable {

        @Test
        @DisplayName("DROP TABLE makes subsequent SELECT return error")
        void dropTableThenQueryFails() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alpha')");

            sqlRunner.executeDdl("DROP TABLE " + fqn());

            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + fqn());

            assertThat(error.errorType()).isEqualTo("SEMANTIC");
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("not found", "not exist", "does not exist");
        }
    }

    // --- 109.5: DESCRIBE TABLE ---

    @Nested
    @DisplayName("109.5 - DESCRIBE TABLE on Paimon table")
    class DescribeTable {

        @Test
        @DisplayName("DESCRIBE TABLE returns correct column definitions")
        void describeReturnsCorrectColumns() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('merge-engine' = 'deduplicate')");

            List<TypedRow> description = sqlRunner.execute(
                    "DESCRIBE " + fqn());

            assertThat(description).isNotEmpty();

            List<String> columnNames = description.stream()
                    .map(row -> row.getRaw("column_name").toString().toLowerCase())
                    .toList();
            assertThat(columnNames).contains("id", "name", "amount");
        }
    }
}
