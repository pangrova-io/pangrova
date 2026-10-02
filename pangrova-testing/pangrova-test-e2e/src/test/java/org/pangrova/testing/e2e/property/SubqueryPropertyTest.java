package org.pangrova.testing.e2e.property;

import net.jqwik.api.*;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for subquery correctness.
 *
 * <p>Validates subquery invariants:
 * - IN subquery equivalent to EXISTS subquery
 * - NOT IN excludes correct rows
 * - Scalar subquery returns single value
 * - UNION ALL row count = sum of individual counts</p>
 *
 * <p>Requirements validated: 3.5, 9.3</p>
 */
@DisplayName("Subquery - Property Tests")
class SubqueryPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String T1 = CATALOG + "." + SCHEMA + ".prop_sub_main";
    private static final String T2 = CATALOG + "." + SCHEMA + ".prop_sub_ref";

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
        var runner = new org.pangrova.testing.e2e.runner.SqlTestRunner(framework);
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + T1
                + " (id BIGINT, value DOUBLE)");
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + T2
                + " (ref_id BIGINT, label VARCHAR)");
        runner.execute("INSERT INTO " + T1 + " VALUES "
                + "(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)");
        runner.execute("INSERT INTO " + T2 + " VALUES "
                + "(1, 'x'), (3, 'y'), (5, 'z')");
    }

    @Property(tries = 1)
    @Tag("Property: IN equivalent to EXISTS")
    void inEquivalentToExists() {
        List<TypedRow> viaIn = sqlRunner.execute(
                "SELECT id FROM " + T1 + " WHERE id IN (SELECT ref_id FROM " + T2 + ")");
        List<TypedRow> viaExists = sqlRunner.execute(
                "SELECT id FROM " + T1 + " t WHERE EXISTS "
                + "(SELECT 1 FROM " + T2 + " r WHERE r.ref_id = t.id)");
        assertThat(viaIn).extracting(row -> ((Number) row.getRaw("id")).longValue())
                .containsExactlyInAnyOrder(1L, 3L, 5L);
        assertThat(viaExists).extracting(row -> ((Number) row.getRaw("id")).longValue())
                .containsExactlyInAnyOrder(1L, 3L, 5L);
    }

    @Property(tries = 1)
    @Tag("Property: NOT IN excludes correct rows")
    void notInExcludesCorrectRows() {
        List<TypedRow> included = sqlRunner.execute(
                "SELECT id FROM " + T1 + " WHERE id IN (SELECT ref_id FROM " + T2 + ")");
        List<TypedRow> excluded = sqlRunner.execute(
                "SELECT id FROM " + T1 + " WHERE id NOT IN (SELECT ref_id FROM " + T2 + ")");
        List<TypedRow> total = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + T1);
        assertThat(included.size() + excluded.size())
                .isEqualTo(((Number) total.get(0).getRaw("cnt")).intValue());
        assertThat(excluded).extracting(row -> ((Number) row.getRaw("id")).longValue())
                .containsExactlyInAnyOrder(2L, 4L);
    }

    @Property(tries = 1)
    @Tag("Property: scalar subquery single value")
    void scalarSubquerySingleValue() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, (SELECT COUNT(*) FROM " + T2 + ") AS ref_count FROM " + T1);
        assertThat(rows).hasSize(5);
        long expectedCount = 3L;
        for (TypedRow row : rows) {
            assertThat(((Number) row.getRaw("ref_count")).longValue()).isEqualTo(expectedCount);
        }
    }

    @Property(tries = 1)
    @Tag("Property: UNION ALL row count additivity")
    void unionAllRowCountAdditivity() {
        List<TypedRow> t1Count = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + T1);
        List<TypedRow> t2Count = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + T2);
        List<TypedRow> unionAll = sqlRunner.execute(
                "SELECT id FROM " + T1 + " UNION ALL SELECT ref_id FROM " + T2);

        long expected = ((Number) t1Count.get(0).getRaw("cnt")).longValue()
                + ((Number) t2Count.get(0).getRaw("cnt")).longValue();
        assertThat((long) unionAll.size()).isEqualTo(expected);
    }
}
