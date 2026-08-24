package org.pangrova.testing.e2e.tpcds;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.ResultValidator;
import org.pangrova.testing.e2e.runner.SqlTestRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared utilities for TPC-DS correctness tests.
 *
 * <p>Handles query resource loading, table name prefixing, expected result
 * parsing, and result comparison against precomputed JSON reference files.
 */
final class TpcDsTestSupport {

    static final String CATALOG = "iceberg_catalog";
    static final String SCHEMA = "tpcds";

    private static final String TABLE_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * TPC-DS table names ordered longest-first to avoid partial replacement
     * (e.g. "store_returns" before "store", "catalog_sales" before "catalog_page").
     */
    private static final List<String> TPCDS_TABLES = List.of(
            "store_returns", "store_sales",
            "catalog_returns", "catalog_sales",
            "web_returns", "web_sales",
            "customer_address", "customer_demographics",
            "household_demographics",
            "call_center", "catalog_page",
            "date_dim", "time_dim",
            "income_band", "ship_mode",
            "web_page", "web_site",
            "warehouse", "customer", "inventory",
            "item", "promotion", "reason", "store");

    private TpcDsTestSupport() {}

    /** Execute a TPC-DS query file through the real pipeline. */
    static List<TypedRow> executeQuery(
            SqlTestRunner runner, String queryFile) {
        String sql = loadAndPrefixQuery(queryFile);
        return runner.execute(sql);
    }

    /**
     * Validate actual results against the precomputed JSON reference.
     *
     * <p>When the reference file contains populated rows, performs full
     * row-level ordered comparison via {@link ResultValidator}. When rows
     * are empty (placeholder), falls back to non-empty + column schema
     * validation using the provided column names.
     */
    static void assertMatchesReference(
            List<TypedRow> actual,
            String queryFile,
            String... expectedColumns) {
        List<TypedRow> expected = loadExpectedRows(queryFile);
        if (expected.isEmpty()) {
            assertSchemaFallback(actual, expectedColumns);
            return;
        }
        ResultValidator.assertEqualsOrdered(expected, actual);
    }

    // --- Internal helpers ---

    private static String loadAndPrefixQuery(String filename) {
        return prefixTableNames(loadResource("/tpcds/queries/" + filename));
    }

    private static List<TypedRow> loadExpectedRows(String queryFile) {
        String jsonFile = queryFile.replace(".sql", ".json");
        String json = loadResource("/tpcds/expected/" + jsonFile);
        return parseExpectedRows(json, jsonFile);
    }

    @SuppressWarnings("unchecked")
    private static List<TypedRow> parseExpectedRows(
            String json, String filename) {
        try {
            Map<String, Object> root = MAPPER.readValue(json,
                    new TypeReference<Map<String, Object>>() {});
            Object rows = root.get("rows");
            if (rows == null) {
                return List.of();
            }
            List<Map<String, Object>> rawRows = MAPPER.convertValue(rows,
                    new TypeReference<List<Map<String, Object>>>() {});
            return rawRows.stream()
                    .map(TypedRow::fromRawMap)
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to parse expected results: " + filename, e);
        }
    }

    private static void assertSchemaFallback(
            List<TypedRow> actual, String... columns) {
        assertThat(actual)
                .as("TPC-DS query must produce non-empty results on scale-factor data "
                        + "(populate expected JSON for full correctness validation)")
                .isNotEmpty();
        if (columns.length > 0) {
            assertThat(actual.get(0).columnNames()).contains(columns);
        }
    }

    private static String prefixTableNames(String sql) {
        String result = sql;
        for (String table : TPCDS_TABLES) {
            result = result.replaceAll(
                    "\\b" + table + "\\b", TABLE_PREFIX + table);
        }
        return result;
    }

    private static String loadResource(String path) {
        try (InputStream is =
                     TpcDsTestSupport.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException(
                        "TPC-DS resource not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to load TPC-DS resource: " + path, e);
        }
    }
}
