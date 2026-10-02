package org.pangrova.testing.e2e.dynamictable;

import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Dynamic Table CDC-mode incremental refresh.
 *
 * <p>Uses Paimon's native changelog capability as the CDC source since
 * Kafka TestContainers are not available in the E2E dependency set.
 * Verifies INSERT, UPDATE, and DELETE propagation through CDC refresh.</p>
 *
 * <p>Validates: Requirement R4</p>
 */
@DisplayName("Dynamic Table CDC E2E")
class DynamicTableCdcE2ETest extends SqlE2ETestBase {

    private static final String PAIMON_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String SOURCE_TABLE = PAIMON_CATALOG + "." + SCHEMA + ".dt_cdc_source";
    private static final String DT_FULL = PAIMON_CATALOG + "." + SCHEMA + ".dt_cdc_target";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(PAIMON_CATALOG, SCHEMA,
                        paimonWarehouse().toString()));
    }

    @BeforeAll
    static void prepareSourceTable() {
        isDockerReady();
        cdcAvailableFlag = true;
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(PAIMON_CATALOG, SCHEMA,
                        paimonWarehouse().toString())));
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE); } catch (Exception ignored) { }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, name VARCHAR(100), PRIMARY KEY (id))");
    }

    @BeforeEach
    void cleanupBeforeEachTest() {
        ensureDynamicTableDropped(DT_FULL);
        try {
            sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
        } catch (Exception ignored) {
        }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, name VARCHAR(100), PRIMARY KEY (id))");
    }

    @Test
    @DisplayName("CDC INSERT propagates new rows to Dynamic Table")
    void cdcInsertPropagation() {
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");

        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                + " TARGET_LAG = 'PT5M' REFRESH_MODE = CDC "
                + "AS SELECT * FROM " + SOURCE_TABLE);

        // Insert via CDC
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'Bob')");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + DT_FULL);
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getRaw("name"))
                .containsExactlyInAnyOrder("Alice", "Bob");
    }

    @Test
    @DisplayName("CDC UPDATE propagates modified rows to Dynamic Table")
    void cdcUpdatePropagation() {
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");

        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                + " TARGET_LAG = 'PT5M' REFRESH_MODE = CDC "
                + "AS SELECT * FROM " + SOURCE_TABLE);

        // Update via CDC
        sqlRunner.execute("UPDATE " + SOURCE_TABLE + " SET name = 'Alice_v2' WHERE id = 1");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + DT_FULL + " WHERE id = 1");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("Alice_v2");
    }

    @Test
    @DisplayName("CDC DELETE removes rows from Dynamic Table")
    void cdcDeletePropagation() {
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'Bob')");

        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                + " TARGET_LAG = 'PT5M' REFRESH_MODE = CDC "
                + "AS SELECT * FROM " + SOURCE_TABLE);

        // Delete via CDC
        sqlRunner.execute("DELETE FROM " + SOURCE_TABLE + " WHERE id = 1");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + DT_FULL);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("Bob");
    }
}
