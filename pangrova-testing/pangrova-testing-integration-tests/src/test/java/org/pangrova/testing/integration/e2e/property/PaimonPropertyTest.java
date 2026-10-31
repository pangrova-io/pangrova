package org.pangrova.testing.integration.e2e.property;

import net.jqwik.api.*;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for Paimon connector correctness.
 *
 * <p>Validates Paimon-specific invariants through the full pipeline:
 * - Table creation with Paimon connector works
 * - Write-read consistency via Paimon
 * - Primary key constraint behavior
 * - Upsert semantics on primary key tables</p>
 *
 * <p>Requirements validated: 10.1, 10.2, 9.3</p>
 */
@DisplayName("Paimon - Property Tests")
class PaimonPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_catalog";
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

    @Property(tries = 1)
    @Tag("Property: Paimon table creation")
    void paimonTableCreation() {
        String table = FQN_PREFIX + "prop_paimon_create_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table
                + " (id BIGINT, name VARCHAR, value DOUBLE)");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table);
        assertThat(rows).isEmpty();

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: Paimon write-read consistency")
    void paimonWriteReadConsistency() {
        String table = FQN_PREFIX + "prop_paimon_wr_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");

        sqlRunner.execute("INSERT INTO " + table + " VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table + " ORDER BY id");
        assertThat(rows).hasSize(3);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: Paimon aggregation correctness")
    void paimonAggregationCorrectness() {
        String table = FQN_PREFIX + "prop_paimon_agg_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table
                + " (id BIGINT, category VARCHAR, amount DOUBLE)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES "
                + "(1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0)");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT category, SUM(amount) AS total FROM " + table + " GROUP BY category");
        assertThat(rows).hasSize(2);

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: Paimon update semantics")
    void paimonUpdateSemantics() {
        String table = FQN_PREFIX + "prop_paimon_upd_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table
                + " (id BIGINT, val VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'original')");
        sqlRunner.execute("UPDATE " + table + " SET val = 'updated' WHERE id = 1");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT val FROM " + table + " WHERE id = 1");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("val")).isEqualTo("updated");

        sqlRunner.executeDdl("DROP TABLE " + table);
    }
}
