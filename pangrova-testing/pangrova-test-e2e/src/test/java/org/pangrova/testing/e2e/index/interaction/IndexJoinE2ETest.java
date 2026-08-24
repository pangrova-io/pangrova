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
 * E2E tests verifying JOIN queries interact correctly with indexes.
 *
 * <p>Indexes on join columns or filter columns in JOIN queries must not
 * alter result correctness.</p>
 *
 * <p>Validates: Requirements R18.1, R18.2, R18.3</p>
 */
@DisplayName("Index JOIN E2E — join interaction")
class IndexJoinE2ETest extends SqlE2ETestBase {

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

    private String leftFqn;
    private String rightFqn;

    @BeforeEach
    void createTables(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        String leftTable = "t_join_l_" + method + "_" + ts;
        String rightTable = "t_join_r_" + method + "_" + ts;
        leftFqn = FQN_PREFIX + leftTable;
        rightFqn = FQN_PREFIX + rightTable;

        sqlRunner.executeDdl("CREATE TABLE " + leftFqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        sqlRunner.execute("INSERT INTO " + leftFqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                + " TIMESTAMP '2025-01-01 12:00:00')");
        sqlRunner.execute("INSERT INTO " + leftFqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')");

        sqlRunner.executeDdl("CREATE TABLE " + rightFqn
                + " (ref_id BIGINT, category VARCHAR)");
        sqlRunner.execute("INSERT INTO " + rightFqn
                + " (ref_id, category) VALUES (1, 'cat_a'), (10, 'cat_b')");
    }

    @Test
    @DisplayName("BLOOM_FILTER on join column: JOIN returns correct results")
    void bloomFilterOnJoinColumn() {
        String query = "SELECT a.name, b.category FROM " + leftFqn + " a"
                + " JOIN " + rightFqn + " b ON a.user_id = b.ref_id";
        List<TypedRow> baseline = sqlRunner.execute(query);
        sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + leftFqn
                + "(user_id) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(query);
        assertThat(withIndex).containsAll(baseline);
    }

    @Test
    @DisplayName("BTREE on filter column + JOIN: correct results with containsAll")
    void btreeFilterWithJoin() {
        String query = "SELECT a.name, b.category FROM " + leftFqn + " a"
                + " JOIN " + rightFqn + " b ON a.user_id = b.ref_id"
                + " WHERE a.amount BETWEEN 0.0 AND 100.0";
        List<TypedRow> baseline = sqlRunner.execute(query);
        sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + leftFqn
                + "(amount) USING BTREE");
        List<TypedRow> withIndex = sqlRunner.execute(query);
        assertThat(withIndex).containsAll(baseline);
    }
}
