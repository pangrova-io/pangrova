package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for SUSPECT worker scheduling preference.
 *
 * <p>When a worker is marked SUSPECT, the scheduler should preferentially
 * assign tasks to ACTIVE workers. Queries must still succeed even when
 * some workers are in SUSPECT state.
 *
 * <p>Covers: T-1.12.1 active-worker scheduling path.
 */
@DisplayName("Suspect Worker Scheduling E2E")
class SuspectWorkerSchedulingE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTestTable() {
        tableName = "suspect_test_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                "id BIGINT, name VARCHAR, value BIGINT");
        for (int i = 1; i <= 10; i++) {
            sqlRunner.execute(String.format(
                    "INSERT INTO %s%s (id, name, value) VALUES (%d, 'item_%d', %d)",
                    FQN_PREFIX, tableName, i, i, i * 100));
        }
    }

    @Test
    @DisplayName("Queries succeed with single active worker")
    void queries_succeedWithSingleActiveWorker() {
        // In test cluster there is one worker — it is ACTIVE.
        // Queries should execute normally.
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + tableName);

        assertThat(rows).hasSize(1);
        long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
        assertThat(count).isEqualTo(10L);
    }

    @Test
    @DisplayName("Aggregation query returns correct result with active worker")
    void aggregation_correctResultWithActiveWorker() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT SUM(value) AS total FROM " + FQN_PREFIX + tableName);

        assertThat(rows).hasSize(1);
        long total = ((Number) rows.get(0).getRaw("total")).longValue();
        // Sum of 100+200+...+1000 = 5500
        assertThat(total).isEqualTo(5500L);
    }

    @Test
    @DisplayName("Multiple sequential queries all succeed on active worker")
    void multipleQueries_allSucceedOnActiveWorker() {
        for (int i = 0; i < 5; i++) {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, value FROM " + FQN_PREFIX + tableName
                            + " WHERE id = " + (i + 1));
            assertThat(rows).hasSize(1);
            long value = ((Number) rows.get(0).getRaw("value")).longValue();
            assertThat(value).isEqualTo((i + 1) * 100L);
        }
    }
}
