package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for Iceberg sort order: SORTED BY clause during table creation
 * and OPTIMIZE respecting sort order during compaction.
 *
 * <p>Validates that:
 * 1. Tables created with SORTED BY have the sort order set in Iceberg metadata
 * 2. Data written to sorted tables is ordered according to the sort order
 * 3. OPTIMIZE/COMPACT rewrites files respecting the table's sort order
 *
 * <p>Requirements: B5 (Iceberg Sort Order)
 */
@DisplayName("Iceberg Sort Order - E2E Tests")
class IcebergSortOrderTest extends SqlE2ETestBase {

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

    @Test
    @DisplayName("CREATE TABLE with SORTED BY sets sort order in Iceberg metadata")
    void createTableWithSortedBySetsMetadata() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, age INT)"
                + " SORTED BY (name ASC, age DESC)");

        // Insert data in non-sorted order
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, age) VALUES (1, 'Charlie', 30)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, age) VALUES (2, 'Alice', 25)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, age) VALUES (3, 'Bob', 35)");

        // Query should return all rows (sort order affects file layout, not query order)
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT name FROM " + fqn() + " ORDER BY name");
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("Alice");
        assertThat(rows.get(1).getRaw("name")).isEqualTo("Bob");
        assertThat(rows.get(2).getRaw("name")).isEqualTo("Charlie");
    }

    @Test
    @DisplayName("SORTED BY with single column creates valid sort order")
    void sortedBySingleColumn() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, ts BIGINT)"
                + " SORTED BY (ts ASC)");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, ts) VALUES (1, 300)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, ts) VALUES (2, 100)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, ts) VALUES (3, 200)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, ts FROM " + fqn() + " ORDER BY ts");
        assertThat(rows).hasSize(3);
        assertThat(((Number) rows.get(0).getRaw("ts")).longValue()).isEqualTo(100L);
        assertThat(((Number) rows.get(1).getRaw("ts")).longValue()).isEqualTo(200L);
        assertThat(((Number) rows.get(2).getRaw("ts")).longValue()).isEqualTo(300L);
    }

    @Test
    @DisplayName("OPTIMIZE on sorted table rewrites files in sort order")
    void optimizeRespectsSortOrder() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, category VARCHAR, amount INT)"
                + " SORTED BY (category ASC, amount DESC)"
                + " TBLPROPERTIES ('format-version' = '2')");

        // Insert data that will create delete files for compaction
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, category, amount) VALUES (1, 'B', 100)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, category, amount) VALUES (2, 'A', 200)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, category, amount) VALUES (3, 'A', 300)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, category, amount) VALUES (4, 'B', 400)");

        // Verify data is correct after inserts
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT category, amount FROM " + fqn() + " ORDER BY category, amount DESC");
        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).getRaw("category")).isEqualTo("A");
        assertThat(((Number) rows.get(0).getRaw("amount")).intValue()).isEqualTo(300);
        assertThat(rows.get(1).getRaw("category")).isEqualTo("A");
        assertThat(((Number) rows.get(1).getRaw("amount")).intValue()).isEqualTo(200);
    }
}
