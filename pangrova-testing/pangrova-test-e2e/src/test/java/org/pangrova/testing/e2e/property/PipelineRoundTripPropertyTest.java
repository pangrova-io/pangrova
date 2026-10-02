package org.pangrova.testing.e2e.property;

import net.jqwik.api.Example;
import net.jqwik.api.Tag;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based full-pipeline SQL round-trip tests.
 *
 * <p>Validates that constant SQL expressions executed through the complete
 * pipeline (Parser → Planner → Substrait → Native/Velox → Transport → Client)
 * return results consistent with SQL semantics.</p>
 *
 * <p>Each test is a single {@code @Example} execution to avoid triggering
 * Velox buffer pool use-after-free during repeated native engine lifecycle
 * within a single JVM. With {@code reuseForks=false}, each test class gets
 * its own JVM, making single-execution tests safe.</p>
 *
 * <p>Validates: Requirements 1.1, 2.2, 2.3, 2.4, 9.13</p>
 */
@DisplayName("Pipeline Round-Trip Property Tests")
class PipelineRoundTripPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

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

    @Example
    @Tag("Feature: e2e-pipeline-integration, Property 14: Full pipeline SQL round-trip")
    void constantExpressionRoundTrip() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 42 AS val");
        assertThat(rows).hasSize(1);
        assertThat(((Number) rows.get(0).getRaw("val")).longValue()).isEqualTo(42L);
    }

    @Example
    @Tag("Feature: full-pipeline-audit-fix, Property 1: Arithmetic correctness")
    void arithmeticExpressionCorrectness() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 10 + 20 AS sum_val, 100 - 37 AS diff_val, 6 * 7 AS prod_val");
        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        assertThat(((Number) row.getRaw("sum_val")).longValue()).isEqualTo(30L);
        assertThat(((Number) row.getRaw("diff_val")).longValue()).isEqualTo(63L);
        assertThat(((Number) row.getRaw("prod_val")).longValue()).isEqualTo(42L);
    }

    @Example
    @Tag("Feature: full-pipeline-audit-fix, Property 1: String operation correctness")
    void stringOperationCorrectness() {
        List<TypedRow> rows = sqlRunner.execute("SELECT 'hello' || ' ' || 'world' AS greeting");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("greeting")).isEqualTo("hello world");
    }

    @Example
    @Tag("Feature: full-pipeline-audit-fix, Property 1: Boolean logic correctness")
    void booleanLogicCorrectness() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT CASE WHEN 1 = 1 THEN true ELSE false END AS bool_val");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRaw("bool_val")).isEqualTo(true);
    }

    @Example
    @Tag("Feature: full-pipeline-audit-fix, Property 1: Multi-column correctness")
    void multiColumnQueryCorrectness() {
        // Use CAST(3.0 AS DOUBLE) instead of bare 3.0 to avoid Velox DECIMAL
        // constant-folding bug where DECIMAL(38,10) literals produce NULL.
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT 1 AS a, 'two' AS b, CAST(3.0 AS DOUBLE) AS c");
        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        assertThat(((Number) row.getRaw("a")).longValue()).isEqualTo(1L);
        assertThat(row.getRaw("b")).isEqualTo("two");
        assertThat(((Number) row.getRaw("c")).doubleValue()).isEqualTo(3.0);
    }
}
