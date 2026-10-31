package org.pangrova.testing.integration.e2e.index.interaction;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying index behavior with boundary values (zero, empty string,
 * match-all LIKE pattern).
 *
 * <p>Validates: Requirements R14.1, R14.2, R14.3, R14.4</p>
 */
@DisplayName("Index Boundary Value E2E — edge case values")
class IndexBoundaryValueE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    private String fqn;

    @BeforeEach
    void createTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String table = "t_bndval_" + method + "_" + System.nanoTime();
        fqn = FQN_PREFIX + table;
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        // Batch 1: includes zero amount and empty string name
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (0, '', 0.0, 'ACTIVE', 'zero row', 'sess-000',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 12:00:00')");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')");
    }

    @Test
    @DisplayName("BTREE: zero value equality query returns correct results")
    void btreeZeroValue() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE amount = 0.0");
        sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE amount = 0.0");
        assertThat(withIndex).containsAll(baseline);
    }

    @Test
    @DisplayName("BLOOM_FILTER: empty string equality query returns correct results")
    void bloomFilterEmptyString() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE name = ''");
        sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                + "(name) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE name = ''");
        assertThat(withIndex).containsAll(baseline);
    }

    @Test
    @DisplayName("PREFIX: match-all LIKE '%' returns correct results")
    void prefixMatchAllLike() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE name LIKE '%'");
        sqlRunner.executeDdl("CREATE INDEX idx_pfx ON " + fqn + "(name) USING PREFIX");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE name LIKE '%'");
        assertThat(withIndex).containsAll(baseline);
    }
}
