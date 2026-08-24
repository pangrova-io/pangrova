package org.pangrova.planner.property;

import org.pangrova.planner.parse.AstToSqlNodeConverter;
import net.jqwik.api.*;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.Statement;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 35: SQL full-stack coverage.
 *
 * <p><b>Validates: Requirements 53.1, 53.4</b></p>
 *
 * <p>For each grammar rule category in PangrovaSql.g4, generates SQL strings,
 * parses them with {@link SqlParser}, converts with {@link AstToSqlNodeConverter},
 * and validates the round-trip succeeds (parser → AST → planner SqlNode).</p>
 *
 * <h3>Coverage Matrix Summary</h3>
 * <ul>
 *   <li>SELECT/JOIN/expressions/CTE/set operations: Parser ✓ | Planner ✓ | E2E ✓</li>
 *   <li>DDL (CREATE/DROP/ALTER TABLE, CREATE SCHEMA): Parser ✓ | Planner ✓ | E2E ✓</li>
 *   <li>Catalog DDL (CREATE/DROP/ALTER/DESCRIBE CATALOG): Parser ✓ | Planner ✓ | E2E ✓</li>
 *   <li>DML (INSERT/UPDATE/DELETE/MERGE): Parser ✓ | Planner ✓ | E2E ✓</li>
 *   <li>SHOW/DESCRIBE: Parser ✓ | Planner ✓ | E2E ✓</li>
 *   <li>CALL: Parser ✓ | Planner ✓ | E2E ✓</li>
 *   <li>Materialized View (CREATE/DROP/REFRESH MV): Parser ✓ | Planner ✓ | E2E ✗ (backlog)</li>
 *   <li>Maintenance (COMPACT/OPTIMIZE/VACUUM): Parser ✓ | Planner ✓ | E2E ✗ (backlog)</li>
 *   <li>Time travel (FOR SYSTEM_TIME/VERSION): Parser ✓ | Planner ✗ | E2E ✗ (backlog)</li>
 * </ul>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 35: SQL full-stack coverage")
class SqlFullStackCoveragePropertyTest {

    private final SqlParser parser = new SqlParser();
    private final AstToSqlNodeConverter converter = new AstToSqlNodeConverter();

    // ── Core property: parse → convert round-trip succeeds ──

    @Property(tries = 10)
    void allCoveredSqlParsesAndConvertsToSqlNode(
            @ForAll("coveredSqlStatements") String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).as("Parse must succeed for [%s]", sql).isNotNull();

        SqlNode node = converter.convert(stmt);
        assertThat(node).as("Convert must produce non-null SqlNode for [%s]", sql).isNotNull();
    }

    // ── Focused property: SELECT queries with all expression types ──

    @Property(tries = 10)
    void selectWithExpressionsConvertsSuccessfully(
            @ForAll("expressionSql") String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).isNotNull();

        SqlNode node = converter.convert(stmt);
        assertThat(node).as("Expression SQL [%s] must convert", sql).isNotNull();
    }

    // ── Focused property: JOIN types ──

    @Property(tries = 10)
    void allJoinTypesConvertSuccessfully(
            @ForAll("joinSql") String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).isNotNull();

        SqlNode node = converter.convert(stmt);
        assertThat(node).as("Join SQL [%s] must convert", sql).isNotNull();
    }

    // ── Focused property: DDL statements ──

    @Property(tries = 10)
    void ddlStatementsConvertSuccessfully(
            @ForAll("ddlSql") String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).isNotNull();

        SqlNode node = converter.convert(stmt);
        assertThat(node).as("DDL SQL [%s] must convert", sql).isNotNull();
    }

    // ── Focused property: DML statements ──

    @Property(tries = 10)
    void dmlStatementsConvertSuccessfully(
            @ForAll("dmlSql") String sql) {
        Statement stmt = parser.parse(sql);
        assertThat(stmt).isNotNull();

        SqlNode node = converter.convert(stmt);
        assertThat(node).as("DML SQL [%s] must convert", sql).isNotNull();
    }

    // ── Backlog gap documentation: parser-only coverage (no planner) ──

    @Example
    @Tag("backlog")
    void timeTravelParsesButPlannerNotImplemented() {
        List<String> timeTravelSql = List.of(
                "SELECT * FROM t FOR SYSTEM_TIME AS OF 123",
                "SELECT * FROM t FOR SYSTEM_VERSION AS OF 5"
        );
        for (String sql : timeTravelSql) {
            Statement stmt = parser.parse(sql);
            assertThat(stmt).as("Time travel SQL [%s] must parse", sql).isNotNull();
            // Planner conversion not yet implemented for time travel.
            // Tracked as backlog item — see INTEGRATION_STATUS.md.
        }
    }

    @Example
    @Tag("backlog")
    void maintenanceOpsParsesAndConvertsButNoE2E() {
        List<String> maintenanceSql = List.of(
                "COMPACT my_table",
                "OPTIMIZE my_table",
                "VACUUM my_table"
        );
        for (String sql : maintenanceSql) {
            Statement stmt = parser.parse(sql);
            assertThat(stmt).isNotNull();
            SqlNode node = converter.convert(stmt);
            assertThat(node).as("Maintenance SQL [%s] converts at planner level", sql).isNotNull();
            // E2E not yet covered — maintenance ops backlog.
            // Tracked as backlog item — see INTEGRATION_STATUS.md.
        }
    }

    // ── Providers ──

    @Provide
    Arbitrary<String> coveredSqlStatements() {
        return Arbitraries.oneOf(
                selectSql(),
                setQuerySql(),
                cteSql(),
                ddlSql(),
                dmlSql(),
                catalogDdlSql(),
                showDescribeSql(),
                callSql(),
                expressionSql(),
                joinSql()
        );
    }

    // ── SELECT ──

    @Provide
    Arbitrary<String> selectSql() {
        return Arbitraries.of(
                "SELECT 1",
                "SELECT 'hello'",
                "SELECT TRUE",
                "SELECT 3.14",
                "SELECT NULL",
                "SELECT * FROM t",
                "SELECT a, b FROM t",
                "SELECT a AS x FROM t",
                "SELECT DISTINCT a FROM t",
                "SELECT a FROM t WHERE a > 1",
                "SELECT a FROM t GROUP BY a",
                "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
                "SELECT a FROM t ORDER BY a ASC",
                "SELECT a FROM t ORDER BY a DESC",
                "SELECT a FROM t LIMIT 10",
                "SELECT a FROM t LIMIT 10 OFFSET 5",
                "SELECT DATE '2024-01-01'",
                "SELECT TIMESTAMP '2024-01-01 12:00:00'"
        );
    }

    // ── Set operations ──

    @Provide
    Arbitrary<String> setQuerySql() {
        return Arbitraries.of(
                "SELECT id FROM t1 UNION ALL SELECT id FROM t2",
                "SELECT id FROM t1 UNION SELECT id FROM t2",
                "SELECT id FROM t1 INTERSECT SELECT id FROM t2",
                "SELECT id FROM t1 EXCEPT SELECT id FROM t2"
        );
    }

    // ── CTE ──

    @Provide
    Arbitrary<String> cteSql() {
        return Arbitraries.of(
                "WITH cte AS (SELECT 1 AS id) SELECT * FROM cte",
                "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b",
                "WITH cte AS (SELECT id FROM t) SELECT * FROM cte WHERE id > 5"
        );
    }

    // ── Expressions ──

    @Provide
    Arbitrary<String> expressionSql() {
        return Arbitraries.of(
                // Arithmetic
                "SELECT a + b FROM t",
                "SELECT a - b FROM t",
                "SELECT a * b FROM t",
                "SELECT a / b FROM t",
                "SELECT -a FROM t",
                // Comparison
                "SELECT a = b FROM t",
                "SELECT a <> b FROM t",
                "SELECT a < b FROM t",
                "SELECT a > b FROM t",
                "SELECT a <= b FROM t",
                "SELECT a >= b FROM t",
                // Logical
                "SELECT a AND b FROM t",
                "SELECT a OR b FROM t",
                "SELECT NOT a FROM t",
                // IN
                "SELECT a IN (1, 2, 3) FROM t",
                "SELECT a NOT IN (1, 2) FROM t",
                "SELECT a IN (SELECT id FROM t2) FROM t",
                "SELECT a NOT IN (SELECT id FROM t2) FROM t",
                // NULL checks
                "SELECT a IS NULL FROM t",
                "SELECT a IS NOT NULL FROM t",
                // BETWEEN / LIKE
                "SELECT a BETWEEN 1 AND 10 FROM t",
                "SELECT a LIKE 'foo%' FROM t",
                // EXISTS / scalar subquery
                "SELECT EXISTS (SELECT 1 FROM t2) FROM t",
                "SELECT (SELECT MAX(id) FROM t2) FROM t",
                // CASE
                "SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM t",
                "SELECT CASE WHEN a > 1 THEN 'big' ELSE 'small' END FROM t",
                // CAST
                "SELECT CAST(a AS BIGINT) FROM t",
                "SELECT CAST(a AS VARCHAR) FROM t",
                // Window functions
                "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t",
                "SELECT RANK() OVER (ORDER BY id) FROM t",
                "SELECT DENSE_RANK() OVER (ORDER BY id) FROM t",
                "SELECT SUM(val) OVER (PARTITION BY cat ORDER BY id) FROM t",
                "SELECT COUNT(*) OVER (ORDER BY id) FROM t",
                // Aggregate functions
                "SELECT COUNT(*) FROM t",
                "SELECT SUM(a) FROM t",
                "SELECT AVG(a) FROM t",
                "SELECT MIN(a) FROM t",
                "SELECT MAX(a) FROM t",
                // Literals
                "SELECT 42",
                "SELECT 3.14",
                "SELECT 'text'",
                "SELECT TRUE",
                "SELECT FALSE",
                "SELECT NULL",
                "SELECT DATE '2024-01-01'",
                "SELECT TIMESTAMP '2024-01-01 12:00:00'"
        );
    }

    // ── Joins ──

    @Provide
    Arbitrary<String> joinSql() {
        return Arbitraries.of(
                "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a INNER JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a LEFT JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a RIGHT JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a FULL JOIN t2 b ON a.id = b.id",
                "SELECT a.id FROM t1 a CROSS JOIN t2 b"
        );
    }

    // ── DDL ──

    @Provide
    Arbitrary<String> ddlSql() {
        return Arbitraries.of(
                "CREATE TABLE t (id BIGINT, name VARCHAR)",
                "CREATE TABLE IF NOT EXISTS t (id BIGINT)",
                "DROP TABLE t",
                "DROP TABLE IF EXISTS t",
                "ALTER TABLE t ADD COLUMN score DOUBLE",
                "ALTER TABLE t DROP COLUMN old_col",
                "ALTER TABLE t RENAME COLUMN old_name TO new_name",
                "ALTER TABLE t ALTER COLUMN id TYPE BIGINT",
                "CREATE SCHEMA my_schema",
                "CREATE SCHEMA IF NOT EXISTS my_schema"
        );
    }

    // ── DML ──

    @Provide
    Arbitrary<String> dmlSql() {
        return Arbitraries.of(
                "INSERT INTO t (id, name) VALUES (1, 'alice')",
                "INSERT INTO t VALUES (1, 'bob'), (2, 'carol')",
                "INSERT OVERWRITE t VALUES (1, 'new')",
                "INSERT INTO t SELECT id FROM t2",
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

    @Provide
    Arbitrary<String> catalogDdlSql() {
        return Arbitraries.of(
                "CREATE CATALOG my_cat USING iceberg WITH (warehouse = '/tmp')",
                "CREATE CATALOG my_cat WITH PROPERTIES (connector = 'hive')",
                "DROP CATALOG my_cat",
                "ALTER CATALOG my_cat SET PROPERTIES (warehouse = '/new')",
                "DESCRIBE CATALOG my_cat"
        );
    }

    // ── SHOW / DESCRIBE ──

    @Provide
    Arbitrary<String> showDescribeSql() {
        return Arbitraries.of(
                "SHOW CATALOGS",
                "SHOW SCHEMAS",
                "SHOW SCHEMAS FROM my_cat",
                "SHOW TABLES",
                "SHOW TABLES FROM my_cat.my_schema",
                "DESCRIBE my_cat.my_schema.my_table",
                "DESCRIBE TABLE my_table"
        );
    }

    // ── CALL ──

    @Provide
    Arbitrary<String> callSql() {
        return Arbitraries.of(
                "CALL my_proc()",
                "CALL my_proc(1)",
                "CALL my_proc(1, 'arg')",
                "CALL catalog.schema.proc(1, 2, 3)"
        );
    }
}
