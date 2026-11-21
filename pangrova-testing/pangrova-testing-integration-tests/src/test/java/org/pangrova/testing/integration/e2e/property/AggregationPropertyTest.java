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
 * Property-based tests for aggregation correctness.
 *
 * <p>Validates aggregation invariants through the full pipeline:
 * - SUM of parts equals SUM of whole (additivity)
 * - COUNT(*) equals actual row count
 * - MIN/MAX are bounded by data range
 * - GROUP BY partitions are exhaustive and mutually exclusive</p>
 *
 * <p>Requirements validated: 3.2, 9.3</p>
 */
@DisplayName("Aggregation - Property Tests")
class AggregationPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN = CATALOG + "." + SCHEMA + ".prop_agg";

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
                + " (id BIGINT, amount DOUBLE, category VARCHAR)");
        runner.execute("INSERT INTO " + FQN + " VALUES "
                + "(1, 100.0, 'A'), (2, 200.0, 'B'), (3, 150.0, 'A'), "
                + "(4, 300.0, 'B'), (5, 250.0, 'C'), (6, 50.0, 'A')");
    }

    @Property(tries = 1)
    @Tag("Property: SUM additivity")
    void sumAdditivity() {
        List<TypedRow> totalRow = sqlRunner.execute("SELECT SUM(amount) AS total FROM " + FQN);
        List<TypedRow> groupRows = sqlRunner.execute(
                "SELECT SUM(amount) AS grp_total FROM " + FQN + " GROUP BY category");

        double total = ((Number) totalRow.get(0).getRaw("total")).doubleValue();
        double sumOfGroups = groupRows.stream()
                .mapToDouble(r -> ((Number) r.getRaw("grp_total")).doubleValue())
                .sum();
        assertThat(sumOfGroups).isCloseTo(total, org.assertj.core.data.Offset.offset(0.001));
    }

    @Property(tries = 1)
    @Tag("Property: COUNT consistency")
    void countConsistency() {
        List<TypedRow> countAll = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + FQN);
        List<TypedRow> allRows = sqlRunner.execute("SELECT id FROM " + FQN);
        assertThat(((Number) countAll.get(0).getRaw("cnt")).longValue())
                .isEqualTo(allRows.size());
    }

    @Property(tries = 1)
    @Tag("Property: MIN/MAX bounds")
    void minMaxBounds() {
        List<TypedRow> bounds = sqlRunner.execute(
                "SELECT MIN(amount) AS mn, MAX(amount) AS mx FROM " + FQN);
        List<TypedRow> allRows = sqlRunner.execute("SELECT amount FROM " + FQN);

        double min = ((Number) bounds.get(0).getRaw("mn")).doubleValue();
        double max = ((Number) bounds.get(0).getRaw("mx")).doubleValue();

        for (TypedRow row : allRows) {
            double val = ((Number) row.getRaw("amount")).doubleValue();
            assertThat(val).isGreaterThanOrEqualTo(min);
            assertThat(val).isLessThanOrEqualTo(max);
        }
    }

    @Property(tries = 1)
    @Tag("Property: GROUP BY partition exhaustiveness")
    void groupByPartitionExhaustiveness() {
        List<TypedRow> groupCounts = sqlRunner.execute(
                "SELECT category, COUNT(*) AS cnt FROM " + FQN + " GROUP BY category");
        List<TypedRow> totalCount = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + FQN);

        long sumGroups = groupCounts.stream()
                .mapToLong(r -> ((Number) r.getRaw("cnt")).longValue())
                .sum();
        assertThat(sumGroups).isEqualTo(((Number) totalCount.get(0).getRaw("cnt")).longValue());
    }
}
