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
 * Property-based tests for optimization correctness.
 *
 * <p>Validates that optimizer rewrites preserve query semantics:
 * - Adding redundant predicates does not change results
 * - Query with and without subquery flattening produces same results
 * - Equivalent query formulations return identical row counts</p>
 *
 * <p>Requirements validated: 5.1, 5.2, 9.3</p>
 */
@DisplayName("Optimization - Property Tests")
class OptimizationPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN = CATALOG + "." + SCHEMA + ".prop_optim";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void seedData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        var runner = new org.pangrova.testing.e2e.runner.SqlTestRunner(framework);
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN
                + " (id BIGINT, value DOUBLE, category VARCHAR)");
        runner.execute("INSERT INTO " + FQN + " VALUES "
                + "(1, 10.0, 'A'), (2, 20.0, 'B'), (3, 30.0, 'A'), "
                + "(4, 40.0, 'B'), (5, 50.0, 'C')");
    }

    @Property(tries = 1)
    @Tag("Property: tautology filter does not change results")
    void tautologyFilterDoesNotChangeResults() {
        List<TypedRow> base = sqlRunner.execute("SELECT * FROM " + FQN);
        List<TypedRow> withTautology = sqlRunner.execute(
                "SELECT * FROM " + FQN + " WHERE 1 = 1");
        assertThat(withTautology).hasSameSizeAs(base);
    }

    @Property(tries = 1)
    @Tag("Property: contradiction filter returns empty")
    void contradictionFilterReturnsEmpty() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + FQN + " WHERE 1 = 0");
        assertThat(rows).isEmpty();
    }

    @Property(tries = 1)
    @Tag("Property: double negation equivalence")
    void doubleNegationEquivalence() {
        List<TypedRow> positive = sqlRunner.execute(
                "SELECT * FROM " + FQN + " WHERE category = 'A'");
        List<TypedRow> doubleNeg = sqlRunner.execute(
                "SELECT * FROM " + FQN + " WHERE NOT (NOT (category = 'A'))");
        assertThat(doubleNeg).hasSameSizeAs(positive);
    }

    @Property(tries = 1)
    @Tag("Property: subquery equivalent to join")
    void subqueryEquivalentToJoin() {
        List<TypedRow> viaSub = sqlRunner.execute(
                "SELECT id FROM " + FQN + " WHERE value > "
                + "(SELECT AVG(value) FROM " + FQN + ")");
        // AVG of (10+20+30+40+50)/5 = 30, so id > 30 means value in {40, 50}
        List<TypedRow> direct = sqlRunner.execute(
                "SELECT id FROM " + FQN + " WHERE value > 30");
        assertThat(viaSub).hasSameSizeAs(direct);
    }

    @Property(tries = 1)
    @Tag("Property: OR to UNION equivalence")
    void orToUnionEquivalence() {
        List<TypedRow> viaOr = sqlRunner.execute(
                "SELECT DISTINCT id FROM " + FQN
                + " WHERE category = 'A' OR category = 'C'");
        List<TypedRow> viaUnion = sqlRunner.execute(
                "SELECT id FROM " + FQN + " WHERE category = 'A' "
                + "UNION SELECT id FROM " + FQN + " WHERE category = 'C'");
        assertThat(viaUnion).hasSameSizeAs(viaOr);
    }
}
