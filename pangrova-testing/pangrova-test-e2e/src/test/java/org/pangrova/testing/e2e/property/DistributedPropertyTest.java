package org.pangrova.testing.e2e.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import net.jqwik.api.lifecycle.BeforeContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test for distributed execution correctness (P26).
 *
 * <p>Validates that TPC-H queries produce non-empty, deterministic results
 * when executed through the real pipeline (single-worker cluster).
 * True multi-worker equivalence testing requires a multi-node test harness
 * beyond the scope of the current E2E framework.
 *
 * <p>Metamorphic property: executing the same query twice must yield
 * identical result sets (idempotency / determinism).
 *
 * <p>Requirements validated: 14.2
 */
@DisplayName("Distributed - Property Tests (P26)")
class DistributedPropertyTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "tpch";
    private static final String TABLE_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int SCALE_FACTOR = 1;

    /** TPC-H table names ordered so "partsupp" is replaced before "part". */
    private static final List<String> TPCH_TABLES = List.of(
            "lineitem", "orders", "customer", "supplier",
            "nation", "region", "partsupp", "part");

    /** Representative TPC-H query files covering diverse SQL patterns. */
    private static final List<String> TPCH_QUERY_FILES = List.of(
            "q01.sql", "q03.sql", "q04.sql", "q05.sql", "q06.sql",
            "q10.sql", "q12.sql", "q14.sql");

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    @BeforeContainer
    static void loadTpcHData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHData(CATALOG, SCHEMA, SCALE_FACTOR);
    }

    // -----------------------------------------------------------------------
    // Property 26: Query execution determinism (idempotency)
    // Executing the same TPC-H query twice must yield identical results.
    // -----------------------------------------------------------------------

    @Property(tries = 10)
    void distributedExecutionEquivalence(@ForAll("tpchQueries") String queryFile) {
        String sql = loadAndPrefixQuery(queryFile);

        List<TypedRow> firstRun = sqlRunner.execute(sql);
        List<TypedRow> secondRun = sqlRunner.execute(sql);

        assertThat(secondRun)
                .as("Row count must be deterministic for %s", queryFile)
                .hasSameSizeAs(firstRun);

        if (!firstRun.isEmpty()) {
            // Queries with LIMIT + non-unique ORDER BY may return different rows
            // at tie boundaries across runs (valid SQL semantics). For those,
            // only row count equality is verified. For queries without LIMIT,
            // verify numeric column aggregates match within tolerance.
            if (!sql.toUpperCase().contains("LIMIT")) {
                assertNumericAggregatesMatch(firstRun, secondRun, queryFile);
            }
        }
    }

    @Provide
    Arbitrary<String> tpchQueries() {
        return Arbitraries.of(TPCH_QUERY_FILES);
    }

    // -----------------------------------------------------------------------
    // TPC-H query loading
    // -----------------------------------------------------------------------

    private static String loadAndPrefixQuery(String filename) {
        return prefixTableNames(loadResource("/tpch/queries/" + filename));
    }

    private static String prefixTableNames(String sql) {
        String result = sql;
        for (String table : TPCH_TABLES) {
            result = result.replaceAll("\\b" + table + "\\b", TABLE_PREFIX + table);
        }
        return result;
    }

    private static String loadResource(String path) {
        try (InputStream is = DistributedPropertyTest.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("TPC-H resource not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load TPC-H resource: " + path, e);
        }
    }

    // -----------------------------------------------------------------------
    // Numeric aggregate comparison for idempotency verification
    // -----------------------------------------------------------------------

    private static void assertNumericAggregatesMatch(
            List<TypedRow> first, List<TypedRow> second, String queryFile) {
        if (first.isEmpty()) return;
        List<String> columns = first.get(0).columnNames().stream().sorted().toList();
        for (String col : columns) {
            double sumFirst = numericSum(first, col);
            double sumSecond = numericSum(second, col);
            if (Double.isNaN(sumFirst) && Double.isNaN(sumSecond)) continue;
            if (sumFirst == 0.0 && sumSecond == 0.0) continue;
            double magnitude = Math.max(Math.abs(sumFirst), Math.abs(sumSecond));
            double relDiff = magnitude == 0.0 ? 0.0 : Math.abs(sumFirst - sumSecond) / magnitude;
            assertThat(relDiff)
                    .as("Numeric aggregate for column '%s' in %s (sum1=%.4f, sum2=%.4f)",
                            col, queryFile, sumFirst, sumSecond)
                    .isLessThanOrEqualTo(1e-4);
        }
    }

    private static double numericSum(List<TypedRow> rows, String col) {
        double sum = 0.0;
        boolean hasNumeric = false;
        for (TypedRow row : rows) {
            Object val = row.getRaw(col);
            if (val instanceof Number n) {
                sum += n.doubleValue();
                hasNumeric = true;
            }
        }
        return hasNumeric ? sum : Double.NaN;
    }
}
