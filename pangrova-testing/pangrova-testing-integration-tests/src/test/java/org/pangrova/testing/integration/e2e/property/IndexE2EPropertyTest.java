package org.pangrova.testing.integration.e2e.property;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based E2E tests for scalar index correctness invariants (BLOOM_FILTER).
 *
 * <p>Validates: Requirements 9.1, 10.3, 10.4
 */
class IndexE2EPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    /**
     * Property E2E-1: Index Evaluation Safety (No False Negatives).
     * Query result WITH index must be a superset of (or equal to) query result WITHOUT index.
     */
    @Property(tries = 5)
    void indexEvaluationSafety(@ForAll @IntRange(min = 1, max = 1000) int userId) {
        String table = "t_prop_safety_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;

        sqlRunner.executeDdl("CREATE TABLE " + fqn + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (" + userId + ", 'a'), (999, 'b')");

        List<TypedRow> withoutIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = " + userId);

        sqlRunner.executeDdl("CREATE INDEX idx_prop ON " + fqn
                + "(user_id) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = " + userId);

        assertThat(withIndex.size()).isGreaterThanOrEqualTo(withoutIndex.size());

        sqlRunner.executeDdl("DROP INDEX idx_prop ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }

    /**
     * Property E2E-2: DDL Idempotency.
     * CREATE INDEX IF NOT EXISTS executed N times all succeed.
     */
    @Property(tries = 3)
    void ddlIdempotency(@ForAll @IntRange(min = 2, max = 5) int repetitions) {
        String table = "t_prop_idemp_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;

        sqlRunner.executeDdl("CREATE TABLE " + fqn + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (1, 'a')");

        for (int i = 0; i < repetitions; i++) {
            sqlRunner.executeDdl("CREATE INDEX IF NOT EXISTS idx_idemp ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");
        }

        List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn);
        long count = indexes.stream()
                .filter(r -> "idx_idemp".equals(r.getRaw("index_name").toString()))
                .count();
        assertThat(count).isEqualTo(1);

        sqlRunner.executeDdl("DROP INDEX idx_idemp ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }

    /**
     * Property E2E-3: Split Filter Conservatism.
     * Number of result rows with index >= number without index (index only reduces I/O).
     */
    @Property(tries = 5)
    void splitFilterConservatism(@ForAll @IntRange(min = 1, max = 500) int searchId) {
        String table = "t_prop_conserv_" + System.nanoTime();
        String fqn = FQN_PREFIX + table;

        sqlRunner.executeDdl("CREATE TABLE " + fqn + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn
                + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (" + searchId + ", 'target')");

        List<TypedRow> withoutIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = " + searchId);

        sqlRunner.executeDdl("CREATE INDEX idx_conserv ON " + fqn
                + "(user_id) USING BLOOM_FILTER");
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = " + searchId);

        assertThat(withIndex.size()).isGreaterThanOrEqualTo(withoutIndex.size());

        sqlRunner.executeDdl("DROP INDEX idx_conserv ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }
}
