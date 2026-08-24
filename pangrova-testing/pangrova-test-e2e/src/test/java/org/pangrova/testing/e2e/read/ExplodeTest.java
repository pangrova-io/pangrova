package org.pangrova.testing.e2e.read;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for EXPLODE / POSEXPLODE (LATERAL TABLE UNNEST) SQL features.
 *
 * <p>Pipeline: SQL → Parser → Planner (ExtensionSingleRel) → Native (Velox UnnestNode) → Result.
 * Validates that EXPLODE produces the expected number of unnested rows.</p>
 *
 * <p>Requirements: 5.2, 5.3, 5.7, 8.2</p>
 */
@DisplayName("EXPLODE / POSEXPLODE - E2E Tests")
class ExplodeTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String TABLE = "explode_test_table";
    private static final String FQN = CATALOG + "." + SCHEMA + "." + TABLE;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void createTestData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.createTestTable(CATALOG, SCHEMA, TABLE,
                "id BIGINT, tags ARRAY(VARCHAR)");
        sqlRunner.executeDdl("INSERT INTO " + FQN
                + " (id, tags) VALUES (1, ARRAY['a', 'b', 'c'])");
        sqlRunner.executeDdl("INSERT INTO " + FQN
                + " (id, tags) VALUES (2, ARRAY['x'])");
        sqlRunner.executeDdl("INSERT INTO " + FQN
                + " (id, tags) VALUES (3, ARRAY[])");
    }

    @Nested
    @DisplayName("EXPLODE basic")
    class ExplodeBasic {

        @Test
        @DisplayName("EXPLODE expands array into multiple rows")
        void explodeExpandsArrayIntoMultipleRows() {
            // id=1 has 3 tags, id=2 has 1 tag, id=3 has 0 tags (skipped) → 4 rows total
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, t.val FROM " + FQN
                    + ", LATERAL TABLE(EXPLODE(tags)) AS t(val)");

            assertThat(rows).hasSize(4);
        }

        @Test
        @DisplayName("EXPLODE preserves column association with WHERE filter")
        void explodePreservesColumnAssociation() {
            // id=1 has tags [a, b, c] → 3 rows after WHERE id = 1
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, t.val FROM " + FQN
                    + ", LATERAL TABLE(EXPLODE(tags)) AS t(val) "
                    + "WHERE id = 1 ORDER BY t.val");

            assertThat(rows).hasSize(3);
        }
    }
}
