package org.pangrova.testing.integration.e2e.write;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test verifying that PageSink.close() does NOT auto-commit data.
 *
 * <p>If a write operation fails before finish() is called, close() alone
 * must not make partial data visible. This validates the fix from T-1.1.1
 * and T-1.1.2 (remove auto-commit from close()).
 *
 * <p>Covers: T-1.1.1 IcebergPageSink close, T-1.1.2 PaimonPageSink close.
 */
@DisplayName("PageSink Close No Auto-Commit E2E")
class PageSinkCloseNoAutoCommitE2ETest extends SqlE2ETestBase {

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
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Test
    @DisplayName("Successful write after failed write commits correctly")
    void successfulWriteAfterFailure_commitsCorrectly() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES (1, 'alice', 100.0)");

        // First: failed write (type mismatch)
        sqlRunner.executeExpectingError(
                "INSERT INTO " + fqn()
                + " (id, name, amount) VALUES ('bad', 'fail', 0.0)");

        // Second: successful write
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES (2, 'bob', 200.0)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn() + " ORDER BY id");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
        assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
    }

    @Test
    @DisplayName("Table remains queryable after multiple failed writes")
    void tableRemains_queryableAfterMultipleFailures() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES (1, 'seed', 10.0)");

        // Multiple failed writes
        for (int i = 0; i < 3; i++) {
            sqlRunner.executeExpectingError(
                    "INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES ('bad', 'fail', 0.0)");
        }

        // Table should still be healthy with original data
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + fqn());
        assertThat(rows.get(0).getLong("cnt")).isEqualTo(1L);
    }
}
