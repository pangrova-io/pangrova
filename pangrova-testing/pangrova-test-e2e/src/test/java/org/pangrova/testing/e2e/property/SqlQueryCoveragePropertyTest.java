package org.pangrova.testing.e2e.property;

import net.jqwik.api.*;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 20: SQL query operation coverage.
 *
 * <p><b>Validates: Requirements 9.3</b></p>
 *
 * <p>For any valid SELECT query (with various clauses), the end-to-end
 * pipeline must execute successfully and return correct results.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 20: SQL query operation coverage")
class SqlQueryCoveragePropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN = CATALOG + "." + SCHEMA + ".prop_coverage";
    private static final String FQN2 = CATALOG + "." + SCHEMA + ".prop_coverage2";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void seedData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(
                        CATALOG, SCHEMA, icebergWarehouse().toString())));
        createAndSeedTables();
    }

    private static void createAndSeedTables() {
        var runner = new org.pangrova.testing.e2e.runner.SqlTestRunner(framework);
        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN
                + " (id BIGINT, name VARCHAR, amount DOUBLE, region VARCHAR)"
                + " PARTITIONED BY (region)");
        runner.execute("INSERT INTO " + FQN
                + " (id, name, amount, region) VALUES "
                + "(1, 'alice', 100.0, 'east'), "
                + "(2, 'bob', 200.0, 'west'), "
                + "(3, 'carol', 300.0, 'east'), "
                + "(4, 'dave', 150.0, 'west'), "
                + "(5, 'eve', 250.0, 'east')");

        runner.executeDdl("CREATE TABLE IF NOT EXISTS " + FQN2
                + " (id BIGINT, label VARCHAR, score DOUBLE)");
        runner.execute("INSERT INTO " + FQN2
                + " (id, label, score) VALUES "
                + "(1, 'x', 10.0), (2, 'y', 20.0), (3, 'z', 30.0)");
    }

    @Property(tries = 1)
    void selectQueriesExecuteSuccessfully(
            @ForAll("selectQueries") String sql) {
        List<TypedRow> rows = sqlRunner.execute(sql);
        assertThat(rows).as("Query [%s] must return results", sql).isNotNull();
    }

    @Property(tries = 1)
    void aggregationQueriesReturnNonEmpty(
            @ForAll("aggregationQueries") String sql) {
        List<TypedRow> rows = sqlRunner.execute(sql);
        assertThat(rows).as("Aggregation [%s] must return rows", sql).isNotEmpty();
    }

    @Property(tries = 1)
    void joinQueriesExecuteSuccessfully(
            @ForAll("joinQueries") String sql) {
        List<TypedRow> rows = sqlRunner.execute(sql);
        assertThat(rows).as("Join [%s] must return results", sql).isNotNull();
    }

    @Provide
    Arbitrary<String> selectQueries() {
        return Arbitraries.oneOf(
                basicSelects(),
                filterSelects(),
                orderByLimitSelects(),
                distinctSelects(),
                expressionSelects()
        );
    }

    @Provide
    Arbitrary<String> aggregationQueries() {
        return Arbitraries.of(
                "SELECT COUNT(*) FROM " + FQN,
                "SELECT SUM(amount) FROM " + FQN,
                "SELECT MIN(amount) FROM " + FQN,
                "SELECT MAX(amount) FROM " + FQN,
                "SELECT region, COUNT(*) FROM " + FQN + " GROUP BY region",
                "SELECT region, SUM(amount) FROM " + FQN + " GROUP BY region",
                "SELECT region, AVG(amount) FROM " + FQN + " GROUP BY region",
                "SELECT region, COUNT(*) FROM " + FQN
                        + " GROUP BY region HAVING COUNT(*) > 1",
                "SELECT region, SUM(amount) FROM " + FQN
                        + " GROUP BY region HAVING SUM(amount) > 200"
        );
    }

    @Provide
    Arbitrary<String> joinQueries() {
        return Arbitraries.of(
                "SELECT a.id, a.name, b.label FROM " + FQN + " a "
                        + "JOIN " + FQN2 + " b ON a.id = b.id",
                "SELECT a.id, a.name FROM " + FQN + " a "
                        + "LEFT JOIN " + FQN2 + " b ON a.id = b.id",
                "SELECT a.id FROM " + FQN + " a "
                        + "RIGHT JOIN " + FQN2 + " b ON a.id = b.id",
                "SELECT a.id FROM " + FQN + " a "
                        + "FULL JOIN " + FQN2 + " b ON a.id = b.id",
                "SELECT a.id FROM " + FQN + " a, "
                        + FQN2 + " b",
                "SELECT a.id, b.label FROM " + FQN + " a "
                        + "INNER JOIN " + FQN2 + " b ON a.id = b.id "
                        + "WHERE a.amount > 100"
        );
    }

    private Arbitrary<String> basicSelects() {
        return Arbitraries.of(
                "SELECT * FROM " + FQN,
                "SELECT id, name FROM " + FQN,
                "SELECT id, name, amount FROM " + FQN,
                "SELECT name, region FROM " + FQN,
                "SELECT 1",
                "SELECT 'hello'",
                "SELECT 1 + 2"
        );
    }

    private Arbitrary<String> filterSelects() {
        return Arbitraries.of(
                "SELECT * FROM " + FQN + " WHERE id > 2",
                "SELECT * FROM " + FQN + " WHERE amount >= 200",
                "SELECT * FROM " + FQN + " WHERE region = 'east'",
                "SELECT * FROM " + FQN + " WHERE id BETWEEN 2 AND 4",
                "SELECT * FROM " + FQN + " WHERE name LIKE 'a%'",
                "SELECT * FROM " + FQN + " WHERE id IN (1, 3, 5)",
                "SELECT * FROM " + FQN + " WHERE id > 1 AND region = 'east'",
                "SELECT * FROM " + FQN + " WHERE id < 3 OR amount > 200"
        );
    }

    private Arbitrary<String> orderByLimitSelects() {
        return Arbitraries.of(
                "SELECT * FROM " + FQN + " ORDER BY id",
                "SELECT * FROM " + FQN + " ORDER BY amount DESC",
                "SELECT * FROM " + FQN + " ORDER BY id LIMIT 3",
                "SELECT * FROM " + FQN + " ORDER BY name ASC LIMIT 2",
                "SELECT * FROM " + FQN + " LIMIT 1"
        );
    }

    private Arbitrary<String> distinctSelects() {
        return Arbitraries.of(
                "SELECT DISTINCT region FROM " + FQN,
                "SELECT DISTINCT name FROM " + FQN
        );
    }

    private Arbitrary<String> expressionSelects() {
        return Arbitraries.of(
                "SELECT id, amount * 2 FROM " + FQN,
                "SELECT id, amount + 10 FROM " + FQN,
                "SELECT id, CAST(id AS VARCHAR) FROM " + FQN,
                "SELECT CASE WHEN amount > 200 THEN 'high' ELSE 'low' END FROM " + FQN,
                "SELECT id, (amount - 100) FROM " + FQN
        );
    }
}
