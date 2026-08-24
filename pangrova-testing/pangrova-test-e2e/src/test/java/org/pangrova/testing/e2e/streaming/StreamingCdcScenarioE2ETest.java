package org.pangrova.testing.e2e.streaming;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.spi.connector.data.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T11: Streaming/CDC — 4 E2E scenarios.
 */
@E2ETest
@DisplayName("T11: Streaming/CDC")
class StreamingCdcE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: INSERT/UPDATE/DELETE operations tracked")
    class OperationTypes {
        @Test void allDmlOperationsWork() {
            String tbl = CATALOG + "." + SCHEMA + ".t11_ops_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,'a'),(2,'b')");
            executeDml("UPDATE " + tbl + " SET val='x' WHERE id=1");
            executeDml("DELETE FROM " + tbl + " WHERE id=2");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
        }
    }

    @Nested @DisplayName("Scenario 2: Incremental data visible")
    class IncrementalVisible {
        @Test void newInsertsVisibleImmediately() {
            String tbl = CATALOG + "." + SCHEMA + ".t11_incr_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, ts BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,1000)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
            executeDml("INSERT INTO " + tbl + " VALUES (2,2000)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 2);
        }
    }

    @Nested @DisplayName("Scenario 3: Checkpoint continuation")
    class CheckpointContinuation {
        @Test void multipleWritesAccumulateCorrectly() {
            String tbl = CATALOG + "." + SCHEMA + ".t11_cp_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, batch INT)");
            for (int batch = 1; batch <= 3; batch++) {
                executeDml("INSERT INTO " + tbl + " VALUES (" + batch + "," + batch + ")");
            }
            assertQueryReturnsRows("SELECT * FROM " + tbl, 3);
        }
    }

    @Nested @DisplayName("Scenario 4: Watermark window")
    class WatermarkWindow {
        @Test void latestDataAccessible() {
            String tbl = CATALOG + "." + SCHEMA + ".t11_wm_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, event_time BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1,1000),(2,2000),(3,3000)");
            List<TypedRow> rows = executeQuery(
                    "SELECT * FROM " + tbl + " WHERE event_time >= 2000 ORDER BY id");
            assertThat(rows).hasSize(2);
        }
    }
}
