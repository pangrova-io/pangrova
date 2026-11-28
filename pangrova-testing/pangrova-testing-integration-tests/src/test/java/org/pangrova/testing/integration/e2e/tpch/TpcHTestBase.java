package org.pangrova.testing.integration.e2e.tpch;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.E2ETestFramework;
import org.pangrova.testing.integration.e2e.SharedTestContainers;
import org.pangrova.testing.integration.e2e.runner.SqlTestRunner;
import org.pangrova.testing.integration.e2e.fixture.dataset.TestDatasetLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.sql.DriverManager;

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

    private static TpcHReferenceOracle reference;

    @BeforeAll
    static void loadTpcHData() throws Exception {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        var pg = SharedTestContainers.POSTGRES;
        reference = new TpcHReferenceOracle(DriverManager.getConnection(
                pg.getJdbcUrl(), pg.getUsername(), pg.getPassword()));
        for (String table : TPCH_TABLES) {
            sqlRunner.executeDdl("DROP TABLE IF EXISTS " + TABLE_PREFIX + table);
        }
        var dualRunner = new SqlTestRunner(new E2ETestFramework(testCluster, sql -> {
            List<TypedRow> rows = sqlRunner.execute(sql);
            reference.executeInput(sql.replace(TABLE_PREFIX, ""));
            return rows;
        }));
        new TestDatasetLoader(dualRunner).loadTpcHData(CATALOG, SCHEMA, 1);
    }

    @AfterAll
    static void closeReference() throws Exception {
        if (reference != null) {
            reference.close();
            reference = null;
        }
    }

    /** Load SQL from resource, qualify table names, execute via real pipeline. */
    List<TypedRow> executeQuery(String filename) {
        return sqlRunner.execute(loadAndPrefixQuery(filename));
    }

    void assertMatchesReference(List<TypedRow> actual, String filename, String... columns) {
        TpcHResultAssertions.assertMatchesReference(
                actual, reference.query(loadQueryResource(filename)), filename, columns);
    }

    List<TypedRow> executeAndVerifyVariant(
            String filename, java.util.function.UnaryOperator<String> customize, String... columns) {
        String sql = customize.apply(loadQueryResource(filename));
        List<TypedRow> rows = sqlRunner.execute(prefixTableNames(sql));
        TpcHResultAssertions.assertMatchesReference(rows, reference.query(sql), filename, columns);
        return rows;
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
