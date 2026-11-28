package org.pangrova.parser.property;

import net.jqwik.api.*;
import org.pangrova.parser.format.AstPrinter;
import org.pangrova.parser.syntax.AntlrSqlParser;
import org.pangrova.parser.api.ast.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 19: AstPrinter round-trip correctness.
 *
 * <p><b>Validates: Requirements 9.6</b></p>
 *
 * <p>For any valid AST produced by {@link AntlrSqlParser#parse(String)},
 * {@code parse(print(ast))} must produce a semantically equivalent AST.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 19: AstPrinter round-trip")
class AstPrinterRoundTripPropertyTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private final AstPrinter printer = new AstPrinter();

    @Property(tries = 10)
    void parseAndPrintRoundTripProducesEquivalentAst(
            @ForAll("roundTripSql") String sql) {
        Statement ast1 = parser.parse(sql);
        String printed = printer.print(ast1);
        Statement ast2 = parser.parse(printed);
        assertThat(ast2)
                .as("Round-trip for [%s] → [%s]", sql, printed)
                .isEqualTo(ast1);
    }

    @Provide
    Arbitrary<String> roundTripSql() {
        return Arbitraries.oneOf(
                queryStatements(),
                setOperationStatements(),
                cteStatements(),
                extensionStatements(),
                catalogStatements(),
                expressionStatements(),
                ddlStatements(),
                dmlStatements()
        );
    }

    private Arbitrary<String> queryStatements() {
        return Arbitraries.of(
                "SELECT 1",
                "SELECT 'hello'",
                "SELECT TRUE",
                "SELECT 3.14",
                "SELECT NULL",
                "SELECT * FROM t",
                "SELECT a, b FROM t",
                "SELECT a AS x FROM t AS t1",
                "SELECT a FROM t WHERE (a > 1)",
                "SELECT a, COUNT(a) FROM t GROUP BY a",
                "SELECT a, COUNT(a) FROM t GROUP BY a HAVING (COUNT(a) > 1)",
                "SELECT a FROM t ORDER BY a ASC",
                "SELECT a FROM t ORDER BY a DESC LIMIT 10",
                "SELECT DISTINCT a FROM t",
                "SELECT (a + b) FROM t",
                "SELECT (a * b) FROM t",
                "SELECT (a - b) FROM t",
                "SELECT (a / b) FROM t",
                "SELECT NOT TRUE",
                "SELECT (a > 10)"
        );
    }

    private Arbitrary<String> setOperationStatements() {
        return Arbitraries.of(
                "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
                "SELECT a FROM t1 UNION SELECT a FROM t2",
                "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
                "SELECT a FROM t1 EXCEPT SELECT a FROM t2"
        );
    }

    private Arbitrary<String> cteStatements() {
        return Arbitraries.of(
                "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte",
                "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b"
        );
    }

    private Arbitrary<String> extensionStatements() {
        return Arbitraries.of(
                "COMPACT my_table",
                "OPTIMIZE my_table",
                "VACUUM my_table",
                "CALL my_proc(1, 'arg')",
                "SELECT * FROM t FOR SYSTEM_TIME AS OF 123",
                "SELECT * FROM t FOR SYSTEM_VERSION AS OF 5",
                "SELECT SUM(a) FROM t",
                "SELECT CAST(a AS BIGINT) FROM t"
        );
    }

    private Arbitrary<String> catalogStatements() {
        return Arbitraries.of(
                "CREATE CATALOG my_cat WITH PROPERTIES (connector = 'iceberg')",
                "DROP CATALOG my_cat",
                "SHOW CATALOGS",
                "DESCRIBE CATALOG my_cat",
                "ALTER CATALOG my_cat SET PROPERTIES (connector = 'hive')"
        );
    }

    /**
     * Expression types coverage: IN, NOT IN, BETWEEN, CASE, EXISTS, subquery,
     * window functions, LIKE, IS NULL, unary minus, star function.
     */
    private Arbitrary<String> expressionStatements() {
        return Arbitraries.of(
                // InExpression
                "SELECT a FROM t WHERE a IN (1, 2, 3)",
                // NotInExpression
                "SELECT a FROM t WHERE a NOT IN (1, 2, 3)",
                // InSubqueryExpression
                "SELECT a FROM t WHERE a IN (SELECT b FROM t2)",
                // NotInSubqueryExpression
                "SELECT a FROM t WHERE a NOT IN (SELECT b FROM t2)",
                // BetweenExpression
                "SELECT a FROM t WHERE a BETWEEN 1 AND 10",
                // CaseExpression (searched)
                "SELECT CASE WHEN (a > 1) THEN 'big' ELSE 'small' END FROM t",
                // CaseExpression (simple)
                "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t",
                // ExistsExpression
                "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t2)",
                // SubqueryExpression (scalar)
                "SELECT (SELECT COUNT(*) FROM t2) FROM t",
                // WindowedFunctionCall
                "SELECT SUM(a) OVER (PARTITION BY b ORDER BY c ASC) FROM t",
                // WindowedFunctionCall with frame
                "SELECT SUM(a) OVER (ORDER BY b ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
                // StarFunctionCall
                "SELECT COUNT(*) FROM t",
                // StarFunctionCall with window
                "SELECT COUNT(*) OVER (PARTITION BY a) FROM t",
                // LikeExpression
                "SELECT a FROM t WHERE a LIKE '%pattern%'",
                // IsNullExpression
                "SELECT a FROM t WHERE a IS NULL",
                // IsNullExpression (negated)
                "SELECT a FROM t WHERE a IS NOT NULL",
                // UnaryMinusExpression
                "SELECT -a FROM t",
                "SELECT -1"
        );
    }

    /**
     * DDL statements coverage.
     */
    private Arbitrary<String> ddlStatements() {
        return Arbitraries.of(
                "CREATE TABLE t (id INT, name VARCHAR)",
                "CREATE TABLE IF NOT EXISTS t (id INT)",
                "CREATE TABLE t (id INT, PRIMARY KEY (id) NOT ENFORCED)",
                "CREATE TABLE t (id INT) PARTITIONED BY (id)",
                "DROP TABLE t",
                "DROP TABLE IF EXISTS t",
                "ALTER TABLE t ADD COLUMN c INT",
                "ALTER TABLE t DROP COLUMN c",
                "ALTER TABLE t RENAME COLUMN c TO d",
                "ALTER TABLE t ALTER COLUMN c TYPE BIGINT",
                "CREATE SCHEMA s",
                "CREATE SCHEMA IF NOT EXISTS s",
                "SHOW TABLES",
                "SHOW TABLES FROM catalog.schema",
                "SHOW SCHEMAS FROM catalog",
                "DESCRIBE t"
        );
    }

    /**
     * DML statements coverage.
     */
    private Arbitrary<String> dmlStatements() {
        return Arbitraries.of(
                "INSERT INTO t VALUES (1, 'a')",
                "INSERT INTO t (id, name) VALUES (1, 'a')",
                "DELETE FROM t",
                "DELETE FROM t WHERE (id = 1)",
                "UPDATE t SET name = 'b'",
                "UPDATE t SET name = 'b' WHERE (id = 1)",
                "MERGE INTO t AS target USING s ON (target.id = s.id) WHEN MATCHED THEN UPDATE SET name = s.name",
                "MERGE INTO t USING s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)",
                "MERGE INTO t USING s ON (t.id = s.id) WHEN MATCHED THEN DELETE"
        );
    }
}
