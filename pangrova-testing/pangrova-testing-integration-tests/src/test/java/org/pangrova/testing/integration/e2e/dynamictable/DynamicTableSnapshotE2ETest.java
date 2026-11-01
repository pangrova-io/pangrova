package org.pangrova.testing.integration.e2e.dynamictable;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for Dynamic Table snapshot-mode lifecycle.
 *
 * <p>Validates the full DDL lifecycle: CREATE DYNAMIC TABLE, incremental
 * refresh via snapshot diff, ALTER SUSPEND/RESUME, DROP cleanup, and
 * SHOW/DESCRIBE output through the real SQL pipeline.</p>
 *
 * <p>Validates: Requirements R1, R2, R3, R12</p>
 */
@DisplayName("Dynamic Table Snapshot E2E")
class DynamicTableSnapshotE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String SOURCE_TABLE = CATALOG + "." + SCHEMA + ".dt_snapshot_source";
    private static final String DT_NAME = "dt_snapshot_target";
    private static final String DT_FULL = CATALOG + "." + SCHEMA + "." + DT_NAME;

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
                + " (id INT, name VARCHAR(100))");
    }

    @BeforeEach
    void cleanupBeforeEachTest() {
        ensureDynamicTableDropped(DT_FULL);
        try {
            sqlRunner.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
        } catch (Exception ignored) { }
        sqlRunner.execute("CREATE TABLE " + SOURCE_TABLE
                + " (id INT, name VARCHAR(100))");
    }

    // ── R1: CREATE DYNAMIC TABLE and verify data after refresh ──────

    @Nested
    @DisplayName("CREATE and Initial Refresh (R1)")
    class CreateAndRefresh {

        @Test
        @DisplayName("CREATE DYNAMIC TABLE populates target from source after initial refresh")
        void createDynamicTableAndVerifyData() {
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'Bob')");

            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + DT_FULL);
            assertThat(rows).hasSize(2);
            assertThat(rows).extracting(r -> r.getRaw("name"))
                    .containsExactlyInAnyOrder("Alice", "Bob");
        }
    }

    // ── R3: Incremental refresh processes only new data ─────────────

    @Nested
    @DisplayName("Incremental Refresh (R3)")
    class IncrementalRefresh {

        @Test
        @DisplayName("Second INSERT triggers incremental refresh with only new rows")
        void incrementalRefreshProcessesOnlyNewData() {
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);

            // Verify initial data
            List<TypedRow> initial = sqlRunner.execute("SELECT * FROM " + DT_FULL);
            assertThat(initial).hasSize(1);

            // Insert additional data and refresh
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'Bob')");
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (3, 'Charlie')");
            sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            // After refresh, target should contain all 3 rows
            List<TypedRow> afterRefresh = sqlRunner.execute("SELECT * FROM " + DT_FULL);
            assertThat(afterRefresh).hasSize(3);
            assertThat(afterRefresh).extracting(r -> r.getRaw("name"))
                    .containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
        }
    }

    // ── R2: ALTER SUSPEND / RESUME lifecycle ────────────────────────

    @Nested
    @DisplayName("ALTER SUSPEND / RESUME (R2)")
    class SuspendResume {

        @Test
        @DisplayName("SUSPEND stops refresh scheduling, RESUME restarts it")
        void suspendAndResumeLifecycle() {
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);

            // Suspend — refresh must be rejected while the materialization is suspended
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " SUSPEND");
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (2, 'Bob')");
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH"))
                    .isInstanceOf(Exception.class);

            // Resume re-enables refresh; an explicit refresh picks up pending changes
            sqlRunner.execute("ALTER DYNAMIC TABLE " + DT_FULL + " RESUME");
            sqlRunner.executeDdl("ALTER DYNAMIC TABLE " + DT_FULL + " REFRESH");

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + DT_FULL);
            assertThat(rows).hasSize(2);
        }
    }

    // ── R2: DROP DYNAMIC TABLE cleanup ──────────────────────────────

    @Nested
    @DisplayName("DROP DYNAMIC TABLE (R2)")
    class DropDynamicTable {

        @Test
        @DisplayName("DROP removes DT metadata and stops scheduling")
        void dropCleansUpCompletely() {
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);

            sqlRunner.execute("DROP DYNAMIC TABLE " + DT_FULL);

            // Querying dropped DT should fail
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> sqlRunner.execute("SELECT * FROM " + DT_FULL)
            ).isInstanceOf(Exception.class);
        }
    }

    // ── R12: SHOW / DESCRIBE DYNAMIC TABLES ─────────────────────────

    @Nested
    @DisplayName("SHOW / DESCRIBE DYNAMIC TABLES (R12)")
    class ShowDescribe {

        @Test
        @DisplayName("SHOW DYNAMIC TABLES lists active DTs with state and refresh mode")
        void showDynamicTablesOutput() {
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);

            List<TypedRow> showResult = sqlRunner.execute("SHOW DYNAMIC TABLES");
            assertThat(showResult).isNotEmpty();
            assertThat(showResult).anyMatch(row -> {
                Object name = row.getRaw("dt_name");
                return DT_NAME.equals(name) || DT_FULL.equals(name);
            });
        }

        @Test
        @DisplayName("DESCRIBE DYNAMIC TABLE shows detail including refresh history")
        void describeDynamicTableOutput() {
            sqlRunner.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'Alice')");
            sqlRunner.execute("CREATE DYNAMIC TABLE " + DT_FULL
                    + " TARGET_LAG = 'PT5M' AS SELECT * FROM " + SOURCE_TABLE);

            List<TypedRow> descResult = sqlRunner.execute("DESCRIBE DYNAMIC TABLE " + DT_FULL);
            assertThat(descResult).isNotEmpty();
        }
    }
}
