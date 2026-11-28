package org.pangrova.parser.property;

import net.jqwik.api.*;
import org.pangrova.parser.syntax.AntlrSqlParser;
import org.pangrova.parser.api.ast.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 21: ANTLR grammar coverage.
 *
 * <p><b>Validates: Requirements 9.5</b></p>
 *
 * <p>For any valid SQL generated from the PangrovaSql.g4 grammar rules,
 * {@link AntlrSqlParser#parse(String)} must succeed without throwing.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 21: ANTLR grammar coverage")
class AntlrGrammarCoveragePropertyTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Property(tries = 10)
    void allGeneratedSqlParseSuccessfully(
            @ForAll("validSqlStatements") String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).as("Parsed statement for [%s]", sql).isNotNull();
    }

    @Provide
    Arbitrary<String> validSqlStatements() {
        return Arbitraries.oneOf(
                selectStatements(),
                setQueryStatements(),
                cteStatements(),
                ddlStatements(),
                dmlStatements(),
                catalogDdlStatements(),
                showDescribeStatements(),
                maintenanceStatements(),
                callStatements()
        );
    }

    // ── SELECT with all clause combinations ──

    private Arbitrary<String> selectStatements() {
        return Arbitraries.oneOf(
                simpleSelects(),
                selectWithJoins(),
                selectWithExpressions(),
                selectWithWindowFunctions(),
                selectWithTimeTravel()
        );
    }

    private Arbitrary<String> simpleSelects() {
        return Arbitraries.of(
                "SELECT 1",
                "SELECT 'hello'",
                "SELECT TRUE",
                "SELECT 3.14",
                "SELECT NULL",
                "SELECT DISTINCT a FROM t",
                "SELECT * FROM t",
                "SELECT a, b FROM t",
                "SELECT a AS x FROM t",
                "SELECT a FROM t WHERE a > 1",
                "SELECT a FROM t GROUP BY a",
                "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
                "SELECT a FROM t ORDER BY a ASC",
                "SELECT a FROM t ORDER BY a DESC",
                "SELECT a FROM t LIMIT 10",
                "SELECT a FROM t LIMIT 10 OFFSET 5",
                "SELECT a FROM t ORDER BY a LIMIT 5",
                "SELECT DATE '2024-01-01'",
                "SELECT TIMESTAMP '2024-01-01 12:00:00'",
                "SELECT CAST(a AS BIGINT) FROM t"
        );
    }

    private Arbitrary<String> selectWithJoins() {
        return Arbitraries.of(
                "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a INNER JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a LEFT JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a LEFT OUTER JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a RIGHT JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a RIGHT OUTER JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a FULL JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a FULL OUTER JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a CROSS JOIN t2 b",
                "SELECT * FROM t1, t2"
        );
    }

    private Arbitrary<String> selectWithExpressions() {
        return Arbitraries.of(
                "SELECT a + b FROM t",
                "SELECT a - b FROM t",
                "SELECT a * b FROM t",
                "SELECT a / b FROM t",
                "SELECT -a FROM t",
                "SELECT a IN (1, 2, 3) FROM t",
                "SELECT a NOT IN (1, 2) FROM t",
                "SELECT a IN (SELECT id FROM t2) FROM t",
                "SELECT a NOT IN (SELECT id FROM t2) FROM t",
                "SELECT a IS NULL FROM t",
                "SELECT a IS NOT NULL FROM t",
                "SELECT a BETWEEN 1 AND 10 FROM t",
                "SELECT a LIKE 'foo%' FROM t",
                "SELECT NOT a FROM t",
                "SELECT a AND b FROM t",
                "SELECT a OR b FROM t",
                "SELECT EXISTS (SELECT 1 FROM t2) FROM t",
                "SELECT (SELECT MAX(id) FROM t2) FROM t",
                "SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM t",
                "SELECT CASE WHEN a > 1 THEN 'big' ELSE 'small' END FROM t"
        );
    }

    private Arbitrary<String> selectWithWindowFunctions() {
        return Arbitraries.of(
                "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t",
                "SELECT RANK() OVER (ORDER BY id) FROM t",
                "SELECT DENSE_RANK() OVER (ORDER BY id) FROM t",
                "SELECT SUM(val) OVER (PARTITION BY cat ORDER BY id) FROM t",
                "SELECT COUNT(*) OVER (ORDER BY id) FROM t",
                "SELECT LAG(val) OVER (ORDER BY id) FROM t",
                "SELECT LEAD(val) OVER (ORDER BY id) FROM t",
                "SELECT MIN(val) OVER (ORDER BY id) FROM t",
                "SELECT MAX(val) OVER (ORDER BY id) FROM t",
                "SELECT SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
        );
    }

    private Arbitrary<String> selectWithTimeTravel() {
        return Arbitraries.of(
                "SELECT * FROM t FOR SYSTEM_TIME AS OF 123",
                "SELECT * FROM t FOR SYSTEM_VERSION AS OF 5",
                "SELECT id FROM t FOR SYSTEM_TIME AS OF 1000",
                "SELECT * FROM t FOR SYSTEM_VERSION AS OF 10 AS snap",
                "SELECT a FROM t FOR SYSTEM_TIME AS OF 999"
        );
    }

    // ── Set operations ──

    private Arbitrary<String> setQueryStatements() {
        return Arbitraries.of(
                "SELECT id FROM t1 UNION ALL SELECT id FROM t2",
                "SELECT id FROM t1 UNION SELECT id FROM t2",
                "SELECT id FROM t1 INTERSECT SELECT id FROM t2",
                "SELECT id FROM t1 EXCEPT SELECT id FROM t2",
                "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
                "SELECT id FROM t1 UNION SELECT id FROM t2 INTERSECT SELECT id FROM t3"
        );
    }

    // ── CTE ──

    private Arbitrary<String> cteStatements() {
        return Arbitraries.of(
                "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte",
                "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b",
                "WITH cte AS (SELECT id FROM t) SELECT * FROM cte WHERE id > 5",
                "WITH cte AS (SELECT id, name FROM t) SELECT name FROM cte ORDER BY id"
        );
    }

    // ── DDL ──

    private Arbitrary<String> ddlStatements() {
        return Arbitraries.of(
                "CREATE TABLE t (id BIGINT, name VARCHAR)",
                "CREATE TABLE IF NOT EXISTS t (id BIGINT)",
                "CREATE TABLE t (id BIGINT) PARTITIONED BY (region)",
                "CREATE TABLE t (id BIGINT, PRIMARY KEY (id) NOT ENFORCED)",
                "CREATE TABLE t (id BIGINT) COMMENT 'test table'",
                "CREATE TABLE t (id BIGINT) WITH ('key' = 'val')",
                "CREATE TABLE t (id BIGINT) TBLPROPERTIES ('k' = 'v')",
                "DROP TABLE t",
                "DROP TABLE IF EXISTS t",
                "ALTER TABLE t ADD COLUMN score DOUBLE",
                "ALTER TABLE t ADD score DOUBLE DEFAULT 0",
                "ALTER TABLE t DROP COLUMN old_col",
                "ALTER TABLE t RENAME COLUMN old_name TO new_name",
                "ALTER TABLE t ALTER COLUMN id TYPE BIGINT",
                "CREATE SCHEMA my_schema",
                "CREATE SCHEMA IF NOT EXISTS my_schema"
        );
    }

    // ── DML ──

    private Arbitrary<String> dmlStatements() {
        return Arbitraries.of(
                "INSERT INTO t (id, name) VALUES (1, 'alice')",
                "INSERT INTO t VALUES (1, 'bob'), (2, 'carol')",
                "INSERT OVERWRITE t VALUES (1, 'new')",
                "INSERT OVERWRITE t (id) PARTITION (region = 'east') VALUES (1)",
                "INSERT INTO t SELECT id FROM t2",
                "INSERT OVERWRITE t SELECT id FROM t2",
                "DELETE FROM t WHERE id = 1",
                "DELETE FROM t",
                "UPDATE t SET name = 'x' WHERE id = 1",
                "UPDATE t SET name = 'x', amount = 0",
                "MERGE INTO t AS target USING s ON target.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
                "MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE"
        );
    }

    // ── Catalog DDL ──

    private Arbitrary<String> catalogDdlStatements() {
        return Arbitraries.of(
                "CREATE CATALOG my_cat USING iceberg WITH (warehouse = '/tmp')",
                "CREATE CATALOG my_cat WITH PROPERTIES (connector = 'hive')",
                "DROP CATALOG my_cat",
                "ALTER CATALOG my_cat SET PROPERTIES (warehouse = '/new')"
        );
    }

    // ── SHOW / DESCRIBE ──

    private Arbitrary<String> showDescribeStatements() {
        return Arbitraries.of(
                "SHOW CATALOGS",
                "SHOW SCHEMAS",
                "SHOW SCHEMAS FROM my_cat",
                "SHOW SCHEMAS IN my_cat",
                "SHOW TABLES",
                "SHOW TABLES FROM my_cat.my_schema",
                "SHOW TABLES IN my_cat.my_schema",
                "DESCRIBE my_cat.my_schema.my_table",
                "DESCRIBE TABLE my_table",
                "DESCRIBE CATALOG my_cat"
        );
    }

    // ── Maintenance ──

    private Arbitrary<String> maintenanceStatements() {
        return Arbitraries.of(
                "COMPACT my_table",
                "OPTIMIZE my_table",
                "VACUUM my_table"
        );
    }

    // ── CALL ──

    private Arbitrary<String> callStatements() {
        return Arbitraries.of(
                "CALL my_proc()",
                "CALL my_proc(1)",
                "CALL my_proc(1, 'arg')",
                "CALL catalog.schema.proc(1, 2, 3)"
        );
    }
}
