package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for AstPrinter: verifies SQL text output and round-trip correctness.
 */
class AstPrinterTest {

    private final AstPrinter printer = new AstPrinter();
    private final SqlParser parser = new SqlParser();

    // ── Literal round-trips ──

    @Test
    void printWindowFramesPreservesRowsVersusRange() {
        assertRoundTrip("SELECT SUM(a) OVER (ORDER BY a ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");
        assertRoundTrip("SELECT SUM(a) OVER (ORDER BY a ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");
    }

    @Test
    void printSelectInteger() {
        assertRoundTrip("SELECT 1");
    }

    @Test
    void printSelectString() {
        assertRoundTrip("SELECT 'hello'");
    }

    @Test
    void printSelectStringWithQuote() {
        assertRoundTrip("SELECT 'it''s'");
    }

    @Test
    void printSelectBoolean() {
        assertRoundTrip("SELECT TRUE");
    }

    @Test
    void printSelectDecimal() {
        assertRoundTrip("SELECT 3.14");
    }

    // ── Arithmetic and comparison ──

    @Test
    void printArithmeticExpression() {
        assertRoundTrip("SELECT (1 + 2)");
    }

    @Test
    void printComparisonExpression() {
        assertRoundTrip("SELECT (x > 10)");
    }

    @Test
    void printNotExpression() {
        assertRoundTrip("SELECT NOT TRUE");
    }

    // ── Full query clauses ──

    @Test
    void printSelectFromWhere() {
        assertRoundTrip("SELECT a FROM t WHERE (a > 1)");
    }

    @Test
    void printSelectWithAlias() {
        assertRoundTrip("SELECT a AS x FROM t AS t1");
    }

    @Test
    void printSelectStar() {
        assertRoundTrip("SELECT * FROM t");
    }

    @Test
    void printGroupByHaving() {
        assertRoundTrip("SELECT a, COUNT(a) FROM t GROUP BY a HAVING (COUNT(a) > 1)");
    }

    @Test
    void printOrderByLimit() {
        assertRoundTrip("SELECT a FROM t ORDER BY a ASC LIMIT 10");
    }

    @Test
    void printFunctionCall() {
        assertRoundTrip("SELECT SUM(a, b)");
    }

    @Test
    void printArrayConstructorInVectorInsert() {
        assertRoundTrip("INSERT INTO vectors (id, embedding) VALUES (1, ARRAY[1.0, 0.0, 2.0])");
    }

    // ── Custom extensions ──

    @Test
    void printCompact() {
        assertRoundTrip("COMPACT my_table");
    }

    @Test
    void printOptimize() {
        assertRoundTrip("OPTIMIZE my_table");
    }

    @Test
    void printVacuum() {
        assertRoundTrip("VACUUM my_table");
    }

    // ── Catalog DDL ──

    @Test
    void printCreateCatalog() {
        assertRoundTrip("CREATE CATALOG my_cat WITH PROPERTIES (connector = 'iceberg')");
    }

    @Test
    void printDropCatalog() {
        assertRoundTrip("DROP CATALOG my_cat");
    }

    @Test
    void printShowCatalogs() {
        assertRoundTrip("SHOW CATALOGS");
    }

    @Test
    void printDescribeCatalog() {
        assertRoundTrip("DESCRIBE CATALOG my_cat");
    }

    @Test
    void printAlterCatalog() {
        assertRoundTrip("ALTER CATALOG my_cat SET PROPERTIES (connector = 'hive')");
    }

    @Test
    void printIndexLifecycleStatements() {
        assertRoundTrip("CREATE INDEX idx1 ON t1 (name) USING BLOOM_FILTER");
        assertRoundTrip("DROP INDEX idx1 ON t1");
    }

    // ── CALL ──

    @Test
    void printCallStatement() {
        assertRoundTrip("CALL my_proc(1, 'arg')");
    }

    // ── Time travel ──

    @Test
    void printTimeTravelSystemTime() {
        assertRoundTrip("SELECT * FROM t FOR SYSTEM_TIME AS OF 123");
    }

    @Test
    void printTimeTravelSystemVersion() {
        assertRoundTrip("SELECT * FROM t FOR SYSTEM_VERSION AS OF 5");
    }

    @Test
    void printTimeTravelBranch() {
        assertRoundTrip("SELECT * FROM t VERSION AS OF BRANCH 'feature_x'");
    }

    @Test
    void printTimeTravelTag() {
        assertRoundTrip("SELECT * FROM t VERSION AS OF TAG 'v1.0'");
    }

    // ── Helper: parse → print → parse round-trip ──

    private void assertRoundTrip(String sql) {
        Statement ast1 = parser.parse(sql);
        String printed = printer.print(ast1);
        Statement ast2 = parser.parse(printed);
        assertThat(ast2).isEqualTo(ast1);
    }
}
