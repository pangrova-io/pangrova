package org.pangrova.testing.e2e.dynamictable;

import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Dynamic Table aggregate incremental refresh.
 *
 * <p>Verifies that GROUP BY aggregates are correctly maintained through
 * incremental refresh cycles, including rollback on DELETE operations.</p>
 *
 * <p>Validates: Requirement R11</p>
 */
@DisplayName("Dynamic Table Aggregate E2E")
class DynamicTableAggregateE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String SOURCE_TABLE = CATALOG + "." + SCHEMA + ".dt_agg_source";
    private static final String DT_NAME = "dt_agg_target";
    private static final String DT_FULL = CATALOG + "." + SCHEMA + "." + DT_NAME;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void prepareSourceTable() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE); } catch (Exception ignored) { }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (category VARCHAR(50), amount DOUBLE)");
    }

    @Test
    @DisplayName("GROUP BY aggregate reflects INSERT and DELETE through incremental refresh")
    void aggregateIncrementalRefreshWithDeleteRollback() {
        // Initial data: two categories
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES ('A', 10.0)");
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES ('A', 20.0)");
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES ('B', 30.0)");

        // Create DT with GROUP BY aggregate
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                + " TARGET_LAG = 'PT5M' AS "
                + "SELECT category, SUM(amount) AS total_amount "
                + "FROM " + SOURCE_TABLE + " GROUP BY category");

        // Verify initial aggregates
        List<TypedRow> initial = sqlRunner.execute(
                "SELECT * FROM " + DT_FULL + " ORDER BY category");
        assertThat(initial).hasSize(2);
        assertThat(initial.get(0).getRaw("category")).isEqualTo("A");
        assertThat(((Number) initial.get(0).getRaw("total_amount")).doubleValue())
                .isEqualTo(30.0);
        assertThat(initial.get(1).getRaw("category")).isEqualTo("B");
        assertThat(((Number) initial.get(1).getRaw("total_amount")).doubleValue())
                .isEqualTo(30.0);

        // DELETE one row from category A, then refresh
        sqlRunner.execute("DELETE FROM " + SOURCE_TABLE
                + " WHERE category = 'A' AND amount = 10.0");

        // After refresh, aggregate for A should roll back to 20.0
        List<TypedRow> afterDelete = sqlRunner.execute(
                "SELECT * FROM " + DT_FULL + " WHERE category = 'A'");
        assertThat(afterDelete).hasSize(1);
        assertThat(((Number) afterDelete.get(0).getRaw("total_amount")).doubleValue())
                .isEqualTo(20.0);
    }
}
