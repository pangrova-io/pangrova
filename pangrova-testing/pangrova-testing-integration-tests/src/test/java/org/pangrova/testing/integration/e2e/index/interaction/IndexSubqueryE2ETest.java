package org.pangrova.testing.integration.e2e.index.interaction;

import org.pangrova.types.value.TypedRow;
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
 * E2E tests verifying subquery-driven predicates interact correctly with indexes.
 *
 * <p>A subquery producing values for an IN clause must work correctly when
 * the outer table has an index on the filtered column.</p>
 *
 * <p>Validates: Requirements R17.1, R17.2</p>
 */
@DisplayName("Index Subquery E2E — subquery-driven predicate interaction")
class IndexSubqueryE2ETest extends SqlE2ETestBase {

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

    private String mainFqn;
    private String lookupFqn;

    @BeforeEach
    void createTables(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        String mainTable = "t_subq_main_" + method + "_" + ts;
        String lookupTable = "t_subq_lkp_" + method + "_" + ts;
        mainFqn = FQN_PREFIX + mainTable;
        lookupFqn = FQN_PREFIX + lookupTable;

        sqlRunner.executeDdl("CREATE TABLE " + mainFqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        sqlRunner.execute("INSERT INTO " + mainFqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                + " TIMESTAMP '2025-01-01 12:00:00')");
        sqlRunner.execute("INSERT INTO " + mainFqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')");

        // Lookup table: contains user_ids that the subquery will select
        sqlRunner.executeDdl("CREATE TABLE " + lookupFqn
                + " (ref_id BIGINT, label VARCHAR)");
        sqlRunner.execute("INSERT INTO " + lookupFqn
                + " (ref_id, label) VALUES (1, 'target'), (10, 'target')");
    }

    @Test
    @DisplayName("BLOOM_FILTER: IN subquery returns correct results with index")
    void bloomFilterInSubquery() {
        String query = "SELECT * FROM " + mainFqn
                + " WHERE user_id IN (SELECT ref_id FROM " + lookupFqn + ")";
        List<TypedRow> baseline = sqlRunner.execute(query);
        sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + mainFqn
                + "(user_id) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(query);
        assertThat(withIndex).containsAll(baseline);
    }
}
