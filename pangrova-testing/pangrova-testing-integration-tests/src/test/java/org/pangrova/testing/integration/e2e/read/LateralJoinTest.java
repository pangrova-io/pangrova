package org.pangrova.testing.integration.e2e.read;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for LATERAL JOIN SQL feature.
 *
 * <p>Validates end-to-end correctness through the full pipeline:
 * SQL → Parser → Planner (Calcite decorrelation) → Substrait → Native → Result.
 * LATERAL JOIN allows the right-side subquery to reference columns from
 * the left-side table.</p>
 *
 * <p>Requirements: 7.2, 7.3, 8.3</p>
 */
@DisplayName("LATERAL JOIN - E2E Tests")
class LateralJoinTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHDataViaConnector(
                org.pangrova.testing.integration.e2e.setup.TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                SCALE_FACTOR);
    }

    @Nested
    @DisplayName("LATERAL derived table")
    class LateralDerivedTable {

        @Test
        @DisplayName("LATERAL subquery references left-side columns")
        void lateralSubqueryReferencesLeftSideColumns() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT o.o_orderkey, o.o_totalprice, sub.max_qty "
                    + "FROM orders o, LATERAL ("
                    + "  SELECT MAX(l.l_quantity) AS max_qty "
                    + "  FROM lineitem l "
                    + "  WHERE l.l_orderkey = o.o_orderkey"
                    + ") AS sub "
                    + "ORDER BY o.o_orderkey "
                    + "LIMIT 10");

            assertThat(rows).hasSize(10);
            rows.forEach(r -> {
                assertThat(r.getRaw("o_orderkey")).isNotNull();
                // max_qty can be NULL for orders without matching lineitems
                // (scalar aggregate on empty input returns NULL for MAX).
            });
        }
    }

    @Nested
    @DisplayName("LATERAL equivalence")
    class LateralEquivalence {

        @Test
        @DisplayName("LATERAL JOIN produces same results as correlated subquery")
        void lateralJoinEquivalentToCorrelatedSubquery() {
            List<TypedRow> lateralRows = sqlRunner.execute(
                    "SELECT o.o_orderkey, sub.item_count "
                    + "FROM orders o, LATERAL ("
                    + "  SELECT COUNT(*) AS item_count "
                    + "  FROM lineitem l "
                    + "  WHERE l.l_orderkey = o.o_orderkey"
                    + ") AS sub "
                    + "ORDER BY o.o_orderkey LIMIT 5");

            List<TypedRow> scalarRows = sqlRunner.execute(
                    "SELECT o.o_orderkey, "
                    + "  (SELECT COUNT(*) FROM lineitem l WHERE l.l_orderkey = o.o_orderkey) AS item_count "
                    + "FROM orders o "
                    + "ORDER BY o.o_orderkey LIMIT 5");

            assertThat(lateralRows).hasSameSizeAs(scalarRows);
            for (int i = 0; i < lateralRows.size(); i++) {
                long lateralKey = ((Number) lateralRows.get(i).getRaw("o_orderkey")).longValue();
                long scalarKey = ((Number) scalarRows.get(i).getRaw("o_orderkey")).longValue();
                assertThat(lateralKey).isEqualTo(scalarKey);

                Object lateralCountObj = lateralRows.get(i).getRaw("item_count");
                Object scalarCountObj = scalarRows.get(i).getRaw("item_count");
                assertThat(lateralCountObj).isNotNull();
                assertThat(scalarCountObj).isNotNull();
                long lateralCount = ((Number) lateralCountObj).longValue();
                long scalarCount = ((Number) scalarCountObj).longValue();
                assertThat(lateralCount).isEqualTo(scalarCount);
            }
        }

        @Test
        @DisplayName("LATERAL COUNT returns zero when no rows match")
        void lateralCountOnEmptyInputReturnsZero() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT sub.item_count FROM (SELECT CAST(-1 AS BIGINT) AS o_orderkey) o, "
                    + "LATERAL (SELECT COUNT(*) AS item_count FROM lineitem l "
                    + "WHERE l.l_orderkey = o.o_orderkey) AS sub");
            List<TypedRow> scalarRows = sqlRunner.execute(
                    "SELECT (SELECT COUNT(*) FROM lineitem l "
                    + "WHERE l.l_orderkey = o.o_orderkey) AS item_count "
                    + "FROM (SELECT CAST(-1 AS BIGINT) AS o_orderkey) o");
            List<TypedRow> computedRows = sqlRunner.execute(
                    "SELECT (SELECT COUNT(*) + 1 FROM lineitem l "
                    + "WHERE l.l_orderkey = o.o_orderkey) AS count_plus_one "
                    + "FROM (SELECT CAST(-1 AS BIGINT) AS o_orderkey) o");

            assertThat(rows).singleElement().satisfies(row ->
                    assertThat(((Number) row.getRaw("item_count")).longValue()).isZero());
            assertThat(scalarRows).singleElement().satisfies(row ->
                    assertThat(((Number) row.getRaw("item_count")).longValue()).isZero());
            assertThat(computedRows).singleElement().satisfies(row ->
                    assertThat(((Number) row.getRaw("count_plus_one")).longValue()).isEqualTo(1));
        }
    }
}
