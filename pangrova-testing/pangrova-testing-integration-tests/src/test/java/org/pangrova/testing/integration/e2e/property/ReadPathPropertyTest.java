package org.pangrova.testing.integration.e2e.property;

import net.jqwik.api.*;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for read path correctness.
 *
 * <p>Validates invariants of SELECT queries through the full pipeline:
 * - Filter predicates reduce or maintain row count (monotonicity)
 * - ORDER BY produces sorted results
 * - LIMIT constrains result size
 * - DISTINCT removes duplicates</p>
 *
 * <p>Requirements validated: 3.1, 9.3</p>
 */
@DisplayName("Read Path - Property Tests")
class ReadPathPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN = CATALOG + "." + SCHEMA + ".prop_read";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void seedData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        var runner = new org.pangrova.testing.integration.e2e.runner.SqlTestRunner(framework);
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN
                + " (id BIGINT, name VARCHAR, value DOUBLE, category VARCHAR)");
        runner.execute("INSERT INTO " + FQN + " VALUES "
                + "(1, 'alpha', 10.0, 'A'), (2, 'beta', 20.0, 'B'), "
                + "(3, 'gamma', 30.0, 'A'), (4, 'delta', 40.0, 'B'), "
                + "(5, 'epsilon', 50.0, 'A'), (6, 'zeta', 60.0, 'C')");
    }

    @Property(tries = 1)
    @Tag("Property: filter monotonicity")
    void filterReducesOrMaintainsRowCount(@ForAll("thresholds") int threshold) {
        List<TypedRow> all = sqlRunner.execute("SELECT * FROM " + FQN);
        List<TypedRow> filtered = sqlRunner.execute(
                "SELECT * FROM " + FQN + " WHERE id > " + threshold);
        assertThat(filtered.size()).isLessThanOrEqualTo(all.size());
    }

    @Property(tries = 1)
    @Tag("Property: LIMIT constrains size")
    void limitConstrainsResultSize(@ForAll("limits") int limit) {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + FQN + " LIMIT " + limit);
        assertThat(rows.size()).isLessThanOrEqualTo(limit);
    }

    @Property(tries = 1)
    @Tag("Property: DISTINCT eliminates duplicates")
    void distinctEliminatesDuplicates() {
        List<TypedRow> distinct = sqlRunner.execute(
                "SELECT DISTINCT category FROM " + FQN);
        long uniqueCount = distinct.stream()
                .map(r -> r.getRaw("category"))
                .distinct()
                .count();
        assertThat(uniqueCount).isEqualTo(distinct.size());
    }

    @Property(tries = 1)
    @Tag("Property: ORDER BY produces sorted output")
    void orderByProducesSortedOutput() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id FROM " + FQN + " ORDER BY id ASC");
        for (int i = 1; i < rows.size(); i++) {
            long prev = ((Number) rows.get(i - 1).getRaw("id")).longValue();
            long curr = ((Number) rows.get(i).getRaw("id")).longValue();
            assertThat(curr).isGreaterThanOrEqualTo(prev);
        }
    }

    @Provide
    Arbitrary<Integer> thresholds() {
        return Arbitraries.of(0, 2, 4, 6, 10);
    }

    @Provide
    Arbitrary<Integer> limits() {
        return Arbitraries.of(1, 3, 5, 10);
    }
}
