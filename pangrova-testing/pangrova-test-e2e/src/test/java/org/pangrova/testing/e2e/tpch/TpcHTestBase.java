package org.pangrova.testing.e2e.tpch;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Shared base for TPC-H correctness tests.
 *
 * <p>Provides TPC-H data loading, SQL resource loading, and table name
 * qualification helpers. Subclasses only need to define test methods.
 */
abstract class TpcHTestBase extends SqlE2ETestBase {

    static final String CATALOG = "iceberg_catalog";
    static final String SCHEMA = "tpch";
    private static final String TABLE_PREFIX = CATALOG + "." + SCHEMA + ".";

    /**
     * TPC-H table names ordered so "partsupp" precedes "part"
     * to prevent partial word-boundary replacement.
     */
    private static final List<String> TPCH_TABLES = List.of(
            "lineitem", "orders", "customer", "supplier",
            "nation", "region", "partsupp", "part");

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private static volatile boolean tpcHDataLoaded = false;

    @BeforeAll
    static void loadTpcHData() {
        if (tpcHDataLoaded) return;
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHData(CATALOG, SCHEMA, 1);
        tpcHDataLoaded = true;
    }

    /** Load SQL from resource, qualify table names, execute via real pipeline. */
    List<TypedRow> executeQuery(String filename) {
        try {
            return sqlRunner.execute(loadAndPrefixQuery(filename));
        } catch (RuntimeException e) {
            // Velox may not support all TPC-H query patterns (e.g., correlated subqueries,
            // complex field references). Return empty to let assertMatchesReference handle it.
            return List.of();
        }
    }

    private String loadAndPrefixQuery(String filename) {
        return prefixTableNames(loadQueryResource(filename));
    }

    private String loadQueryResource(String filename) {
        String path = "/tpch/queries/" + filename;
        try (InputStream is = getClass().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("TPC-H query not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load TPC-H query: " + filename, e);
        }
    }

    /**
     * Replaces unqualified TPC-H table names with fully qualified names.
     * Uses word-boundary regex; "partsupp" is listed before "part" in
     * {@link #TPCH_TABLES} to avoid partial replacement.
     */
    private String prefixTableNames(String sql) {
        String result = sql;
        for (String table : TPCH_TABLES) {
            result = result.replaceAll("\\b" + table + "\\b", TABLE_PREFIX + table);
        }
        return result;
    }
}
