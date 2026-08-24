package org.pangrova.testing.e2e.index.interaction;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying aggregation queries interact correctly with indexes.
 *
 * <p>COUNT, SUM, GROUP BY results must be identical with and without index.
 * Indexes reduce I/O but must not alter aggregation semantics.</p>
 *
 * <p>Validates: Requirements R15.1, R15.2, R15.3</p>
 */
@DisplayName("Index Aggregation E2E — COUNT/SUM/GROUP BY interaction")
class IndexAggregationE2ETest extends SqlE2ETestBase {

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
        String table = "t_agg_" + method + "_" + System.nanoTime();
        fqn = FQN_PREFIX + table;
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                + " TIMESTAMP '2025-01-01 12:00:00')");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (100, 'Eve', 250.0, 'ACTIVE', 'distributed sql', 'sess-100',"
                + " TIMESTAMP '2025-03-01 00:00:00'),"
                + " (200, 'Frank', 275.0, 'PENDING', 'lakehouse format', 'sess-200',"
                + " TIMESTAMP '2025-03-15 00:00:00')");
        sqlRunner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1000, 'Grace', 350.0, 'ACTIVE', 'iceberg table', 'sess-1000',"
                + " TIMESTAMP '2025-04-01 00:00:00'),"
                + " (2000, 'Hank', 375.0, 'INACTIVE', 'paimon store', 'sess-2000',"
                + " TIMESTAMP '2025-04-15 00:00:00')");
    }

    @Test
    @DisplayName("BTREE: COUNT with range predicate identical with and without index")
    void btreeCountWithRange() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + fqn
                        + " WHERE amount BETWEEN 100.0 AND 300.0");
        sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + fqn
                        + " WHERE amount BETWEEN 100.0 AND 300.0");
        assertThat(withIndex).containsAll(baseline);
    }

    @Test
    @DisplayName("BLOOM_FILTER: SUM + GROUP BY identical with and without index")
    void bloomFilterSumGroupBy() {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT status, SUM(amount) AS total FROM " + fqn
                        + " WHERE user_id = 1 GROUP BY status");
        sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                + "(user_id) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT status, SUM(amount) AS total FROM " + fqn
                        + " WHERE user_id = 1 GROUP BY status");
        assertThat(withIndex).containsAll(baseline);
    }
}
