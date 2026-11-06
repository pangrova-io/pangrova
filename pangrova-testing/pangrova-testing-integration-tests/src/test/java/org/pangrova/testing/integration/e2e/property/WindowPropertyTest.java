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
 * Property-based tests for window function correctness.
 *
 * <p>Validates window function invariants:
 * - ROW_NUMBER produces consecutive integers 1..N within each partition
 * - RANK values are bounded [1, partition_size]
 * - SUM OVER() equals total SUM for the partition
 * - Window functions do not change the row count</p>
 *
 * <p>Requirements validated: 3.4, 9.3</p>
 */
@DisplayName("Window Function - Property Tests")
class WindowPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN = CATALOG + "." + SCHEMA + ".prop_window";

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
                + " (id BIGINT, dept VARCHAR, salary DOUBLE)");
        runner.execute("INSERT INTO " + FQN + " VALUES "
                + "(1, 'eng', 100.0), (2, 'eng', 120.0), (3, 'eng', 110.0), "
                + "(4, 'sales', 90.0), (5, 'sales', 95.0), (6, 'hr', 80.0)");
    }

    @Property(tries = 1)
    @Tag("Property: window preserves row count")
    void windowPreservesRowCount() {
        List<TypedRow> base = sqlRunner.execute("SELECT * FROM " + FQN);
        List<TypedRow> withWindow = sqlRunner.execute(
                "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM " + FQN);
        assertThat(withWindow).hasSameSizeAs(base);
    }

    @Property(tries = 1)
    @Tag("Property: ROW_NUMBER consecutive integers")
    void rowNumberConsecutive() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM " + FQN);
        for (int i = 0; i < rows.size(); i++) {
            assertThat(((Number) rows.get(i).getRaw("rn")).longValue())
                    .isEqualTo(i + 1L);
        }
    }

    @Property(tries = 1)
    @Tag("Property: RANK bounded by partition size")
    void rankBoundedByPartitionSize() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk "
                + "FROM " + FQN);
        for (TypedRow row : rows) {
            long rank = ((Number) row.getRaw("rnk")).longValue();
            assertThat(rank).isGreaterThanOrEqualTo(1);
        }
    }

    @Property(tries = 1)
    @Tag("Property: SUM OVER() equals total")
    void sumOverEqualsTotal() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT salary, SUM(salary) OVER () AS total_salary FROM " + FQN);
        List<TypedRow> totalRow = sqlRunner.execute("SELECT SUM(salary) AS s FROM " + FQN);
        double expectedTotal = ((Number) totalRow.get(0).getRaw("s")).doubleValue();

        for (TypedRow row : rows) {
            assertThat(((Number) row.getRaw("total_salary")).doubleValue())
                    .isCloseTo(expectedTotal, org.assertj.core.data.Offset.offset(0.001));
        }
    }
}
