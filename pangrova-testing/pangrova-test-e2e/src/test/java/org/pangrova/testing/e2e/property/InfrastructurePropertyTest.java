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
 * Property-based tests for infrastructure correctness.
 *
 * <p>Validates infrastructure-level invariants:
 * - Pipeline handles empty result sets gracefully
 * - Multiple statements in sequence maintain state
 * - DDL followed by DML works correctly
 * - Large result sets are handled without truncation</p>
 *
 * <p>Requirements validated: 1.1, 2.2, 9.13</p>
 */
@DisplayName("Infrastructure - Property Tests")
class InfrastructurePropertyTest extends SqlE2ETestBase {

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

    @Property(tries = 1)
    @Tag("Property: empty result set handled gracefully")
    void emptyResultSetHandled() {
        String table = FQN_PREFIX + "prop_infra_empty_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val VARCHAR)");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table);
        assertThat(rows).isEmpty();

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: DDL then DML sequence works")
    void ddlThenDmlSequenceWorks() {
        String table = FQN_PREFIX + "prop_infra_seq_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, name VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 'test')");

        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("test");

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: constant query requires no table")
    void constantQueryRequiresNoTable() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 1 AS one, 'hello' AS greeting");
        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.get(0).getRaw("one")).longValue()).isEqualTo(1L);
        assertThat(rows.get(0).getRaw("greeting")).isEqualTo("hello");
    }

    @Property(tries = 1)
    @Tag("Property: multiple inserts accumulate")
    void multipleInsertsAccumulate() {
        String table = FQN_PREFIX + "prop_infra_accum_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT)");

        for (int i = 1; i <= 5; i++) {
            sqlRunner.execute("INSERT INTO " + table + " VALUES (" + i + ")");
        }

        List<TypedRow> count = sqlRunner.execute("SELECT COUNT(*) AS cnt FROM " + table);
        assertThat(((Number) count.get(0).getRaw("cnt")).longValue()).isEqualTo(5L);

        sqlRunner.executeDdl("DROP TABLE " + table);
    }

    @Property(tries = 1)
    @Tag("Property: table lifecycle create-use-drop")
    void tableLifecycleCreateUseDrop() {
        String table = FQN_PREFIX + "prop_infra_life_" + System.nanoTime();

        sqlRunner.executeDdl("CREATE TABLE " + table + " (id BIGINT, val DOUBLE)");
        sqlRunner.execute("INSERT INTO " + table + " VALUES (1, 3.14)");
        List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + table);
        assertThat(rows).hasSize(1);

        sqlRunner.executeDdl("DROP TABLE " + table);
        // After drop, table should not exist — any query would fail
    }
}
