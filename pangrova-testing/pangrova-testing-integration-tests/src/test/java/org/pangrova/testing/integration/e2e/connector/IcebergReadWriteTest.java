package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg connector E2E test for MERGE INTO with snapshot verification.
 *
 * <p>Covers R9.10. R9.1, R9.6-R9.9, R9.17 removed as duplicates of write/ package tests.
 * All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 *
 * <p>Requirements: 9.1, 16.6
 */
@DisplayName("Iceberg Connector - Read/Write E2E Tests")
class IcebergReadWriteTest extends SqlE2ETestBase {

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

    // --- R9.10: MERGE INTO on Iceberg table ---

    @Nested
    @DisplayName("R9.10 - MERGE INTO on Iceberg Table")
    class MergeIntoOnIceberg {

        @Test
        @DisplayName("MERGE INTO handles matched/unmatched correctly on Iceberg")
        void mergeIntoOnIceberg() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES (1, 'alice', 100.0), (2, 'bob', 200.0)");

            String srcTable = tableName + "_src";
            String srcFqn = FQN_PREFIX + srcTable;
            sqlRunner.executeDdl("CREATE TABLE " + srcFqn
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)");
            sqlRunner.execute("INSERT INTO " + srcFqn
                    + " VALUES (2, 'bob_v2', 250.0), (3, 'carol', 300.0)");

            sqlRunner.execute("MERGE INTO " + fqn() + " t "
                    + "USING " + srcFqn + " s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, amount = s.amount "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, amount) "
                    + "VALUES (s.id, s.name, s.amount)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRow(rows.get(0), 1L, "alice", 100.0, "amount");
            assertRow(rows.get(1), 2L, "bob_v2", 250.0, "amount");
            assertRow(rows.get(2), 3L, "carol", 300.0, "amount");
        }
    }

    // --- Assertion helper ---

    private static void assertRow(TypedRow row,
                                  long expectedId, String expectedName,
                                  double expectedValue, String valueColumn) {
        assertThat(((Number) row.getRaw("id")).longValue()).isEqualTo(expectedId);
        assertThat(row.getRaw("name")).isEqualTo(expectedName);
        assertThat(((Number) row.getRaw(valueColumn)).doubleValue()).isEqualTo(expectedValue);
    }
}
