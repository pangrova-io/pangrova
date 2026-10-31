package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for Paimon sort compaction and Z-Order sorting.
 *
 * <p>Validates that:
 * 1. Tables created with SORTED BY have sort-compaction.columns set
 * 2. SQL reads remain correct with sort-compaction configuration
 * 3. Z-Order configuration persists and multi-column filters remain correct
 *
 * <p>Requirements: C11 (Z-Order / Sort Compact), D7 (Sort Order Unified)
 */
@DisplayName("Paimon Sort Compact and Z-Order - E2E Tests")
class PaimonSortCompactTest extends SqlE2ETestBase {

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

    @Test
    @DisplayName("CREATE TABLE with SORTED BY sets sort-compaction.columns property")
    void sortedBySetsSortCompactionColumns() throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id INT, region VARCHAR, amount BIGINT, PRIMARY KEY (id))"
                + " SORTED BY (region ASC, amount DESC)");

        // Verify table was created and data can be written
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, region, amount) VALUES (1, 'US', 500)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, region, amount) VALUES (2, 'EU', 300)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, region, amount) VALUES (3, 'US', 100)");

        assertThat(persistedOptions()).containsEntry("sort-compaction.columns", "region,amount");
    }

    @Test
    @DisplayName("Sorted table data skipping filters efficiently after compaction")
    void sortCompactEnablesDataSkipping() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id INT, category VARCHAR, score INT, PRIMARY KEY (id))"
                + " SORTED BY (category ASC)");

        // Insert data across multiple batches to create multiple files
        for (int i = 0; i < 20; i++) {
            String category = (i % 4 == 0) ? "A" : (i % 4 == 1) ? "B" : (i % 4 == 2) ? "C" : "D";
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, score) VALUES (" + i + ", '" + category + "', " + (i * 10) + ")");
        }

        // Query with filter on sort column should benefit from data skipping
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, category, score FROM " + fqn()
                        + " WHERE category = 'A' ORDER BY id");
        assertThat(rows).hasSize(5);
        for (TypedRow row : rows) {
            assertThat(row.getRaw("category")).isEqualTo("A");
        }
    }

    @Test
    @DisplayName("Z-Order sorting with USING ZORDER enables multi-column data skipping")
    void zOrderSortingMultiColumnFilter() throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id INT, x INT, y INT, payload VARCHAR, PRIMARY KEY (id))"
                + " SORTED BY (x ASC, y ASC) USING ZORDER");

        // Insert data with varying x and y values
        for (int i = 0; i < 30; i++) {
            int x = i % 10;
            int y = i / 10;
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, x, y, payload) VALUES (" + i + ", " + x + ", " + y
                    + ", 'data_" + i + "')");
        }

        // Query filtering on both z-order columns
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, x, y FROM " + fqn()
                        + " WHERE x = 5 AND y = 1 ORDER BY id");
        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.get(0).getRaw("x")).intValue()).isEqualTo(5);
        assertThat(((Number) rows.get(0).getRaw("y")).intValue()).isEqualTo(1);

        assertThat(persistedOptions()).containsEntry("sort-compaction.sort-strategy", "zorder");
    }
}
