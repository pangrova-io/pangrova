package org.pangrova.testing.integration.e2e.property;

import net.jqwik.api.*;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for JOIN correctness.
 *
 * <p>Validates join invariants through the full pipeline:
 * - INNER JOIN result is subset of CROSS JOIN
 * - LEFT JOIN preserves all left table rows
 * - JOIN is commutative for result count (A JOIN B ≈ B JOIN A)
 * - Self-join on equality produces at least N rows (N = table size)</p>
 *
 * <p>Requirements validated: 3.3, 9.3</p>
 */
@DisplayName("Join - Property Tests")
class JoinPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String T1 = CATALOG + "." + SCHEMA + ".prop_join_left";
    private static final String T2 = CATALOG + "." + SCHEMA + ".prop_join_right";

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
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + T1
                + " (id BIGINT, name VARCHAR, fk BIGINT)");
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + T2
                + " (id BIGINT, label VARCHAR)");
        runner.execute("INSERT INTO " + T1 + " VALUES "
                + "(1, 'a', 10), (2, 'b', 20), (3, 'c', 10), (4, 'd', 30)");
        runner.execute("INSERT INTO " + T2 + " VALUES "
                + "(10, 'X'), (20, 'Y'), (40, 'Z')");
    }

    @Property(tries = 1)
    @Tag("Property: INNER JOIN subset of CROSS JOIN")
    void innerJoinSubsetOfCrossJoin() {
        List<TypedRow> inner = sqlRunner.execute(
                "SELECT l.id FROM " + T1 + " l JOIN " + T2 + " r ON l.fk = r.id");
        List<TypedRow> cross = sqlRunner.execute(
                "SELECT l.id FROM " + T1 + " l, " + T2 + " r");
        assertThat(inner.size()).isLessThanOrEqualTo(cross.size());
    }

    @Property(tries = 1)
    @Tag("Property: LEFT JOIN preserves left rows")
    void leftJoinPreservesLeftRows() {
        List<TypedRow> leftTable = sqlRunner.execute("SELECT * FROM " + T1);
        List<TypedRow> leftJoin = sqlRunner.execute(
                "SELECT l.id FROM " + T1 + " l LEFT JOIN " + T2 + " r ON l.fk = r.id");
        assertThat(leftJoin.size()).isGreaterThanOrEqualTo(leftTable.size());
    }

    @Property(tries = 1)
    @Tag("Property: JOIN commutativity for count")
    void joinCommutativity() {
        List<TypedRow> ab = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + T1 + " l JOIN " + T2 + " r ON l.fk = r.id");
        List<TypedRow> ba = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + T2 + " r JOIN " + T1 + " l ON r.id = l.fk");
        assertThat(((Number) ab.get(0).getRaw("cnt")).longValue())
                .isEqualTo(((Number) ba.get(0).getRaw("cnt")).longValue());
    }

    @Property(tries = 1)
    @Tag("Property: self-join produces at least N rows")
    void selfJoinProducesAtLeastNRows() {
        List<TypedRow> base = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + T1);
        long baseCount = ((Number) base.get(0).getRaw("cnt")).longValue();

        List<TypedRow> selfJoin = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + T1 + " a JOIN " + T1 + " b ON a.fk = b.fk");
        assertThat(((Number) selfJoin.get(0).getRaw("cnt")).longValue())
                .isGreaterThanOrEqualTo(baseCount);
    }
}
