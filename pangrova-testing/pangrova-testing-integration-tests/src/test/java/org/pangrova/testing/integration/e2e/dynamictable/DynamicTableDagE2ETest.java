package org.pangrova.testing.integration.e2e.dynamictable;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for Dynamic Table multi-level DAG cascade refresh.
 *
 * <p>Verifies that a three-level dependency chain (source → DT_A → DT_B → DT_C)
 * correctly propagates data through cascade refresh.</p>
 *
 * <p>Validates: Requirement R6</p>
 */
@DisplayName("Dynamic Table DAG Cascade E2E")
class DynamicTableDagE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String SOURCE_TABLE = CATALOG + "." + SCHEMA + ".dt_dag_source";
    private static final String DT_A = CATALOG + "." + SCHEMA + ".dt_dag_a";
    private static final String DT_B = CATALOG + "." + SCHEMA + ".dt_dag_b";
    private static final String DT_C = CATALOG + "." + SCHEMA + ".dt_dag_c";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void prepareSourceTable() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE); } catch (Exception ignored) { }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, value VARCHAR(100))");
    }

    @BeforeEach
    void cleanupBeforeEachTest() {
        ensureDynamicTableDropped(DT_C);
        ensureDynamicTableDropped(DT_B);
        ensureDynamicTableDropped(DT_A);
        try {
            sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
        } catch (Exception ignored) {
        }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, value VARCHAR(100))");
    }

    @Test
    @DisplayName("Three-level DAG cascade: source → DT_A → DT_B → DT_C propagates data")
    void threeLevelCascadeRefresh() {
        // Seed source
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'root')");

        // Build three-level DAG
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_A
                + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_B
                + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + DT_A);
        sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_C
                + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + DT_B);

        // Verify data propagated to all three levels
        List<TypedRow> levelA = sqlRunner.execute("SELECT * FROM " + DT_A);
        assertThat(levelA).hasSize(1);
        assertThat(levelA.get(0).getRaw("value")).isEqualTo("root");

        List<TypedRow> levelB = sqlRunner.execute("SELECT * FROM " + DT_B);
        assertThat(levelB).hasSize(1);
        assertThat(levelB.get(0).getRaw("value")).isEqualTo("root");

        List<TypedRow> levelC = sqlRunner.execute("SELECT * FROM " + DT_C);
        assertThat(levelC).hasSize(1);
        assertThat(levelC.get(0).getRaw("value")).isEqualTo("root");

        // Insert new data at source and refresh each level in dependency order
        sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'cascade')");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_A + " REFRESH");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_B + " REFRESH");
        sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_C + " REFRESH");

        List<TypedRow> levelCAfter = sqlRunner.execute("SELECT * FROM " + DT_C);
        assertThat(levelCAfter).hasSize(2);
        assertThat(levelCAfter).extracting(r -> r.getRaw("value"))
                .containsExactlyInAnyOrder("root", "cascade");
    }
}
