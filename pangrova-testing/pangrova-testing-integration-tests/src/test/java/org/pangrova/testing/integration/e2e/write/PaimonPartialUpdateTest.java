package org.pangrova.testing.integration.e2e.write;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for Paimon partial-update merge engine.
 *
 * <p>Covers Requirements 111.1-111.2. All operations execute through the real
 * pipeline: HTTP → Coordinator → Worker → Native → Paimon Storage → transport.
 */
@DisplayName("Paimon Partial Update - E2E Tests")
class PaimonPartialUpdateTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_partial";
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

    // --- 111.1 + 111.2: Partial field merge ---

    @Nested
    @DisplayName("111.1/111.2 - Partial field merge on partial-update engine")
    class PartialFieldMerge {

        @Test
        @DisplayName("Partial inserts for same key merge into complete row")
        void partialFieldsMergeCorrectly() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('merge-engine' = 'partial-update', 'bucket' = '1',"
                    + " 'file.format' = 'parquet')");

            // First insert: only id + name (amount is NULL)
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES (1, 'alice', CAST(NULL AS DOUBLE))");

            // Second insert: only id + amount (name is NULL)
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES (1, CAST(NULL AS VARCHAR), 99.5)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE id = 1");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("alice");
            assertThat(rows.get(0).getDouble("amount")).isEqualTo(99.5);
        }
    }
}
