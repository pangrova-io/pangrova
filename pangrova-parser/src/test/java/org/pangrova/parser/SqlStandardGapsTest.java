package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.ParameterMarker;
import org.pangrova.parser.ast.statement.CreateTableAsSelectStatement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SQL standard gap closures: WITH RECURSIVE, JOIN USING,
 * CREATE TABLE AS SELECT (CTAS), and parameter markers (?).
 */
class SqlStandardGapsTest {

    private final SqlParser parser = new SqlParser();

    // ── WITH RECURSIVE ──

    @Test
    void parseWithRecursive() {
        Statement stmt = parser.parse(
                "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM cte");
        assertThat(stmt).isInstanceOf(CteQuery.class);
        CteQuery cte = (CteQuery) stmt;
        assertThat(cte.recursive()).isTrue();
        assertThat(cte.ctes()).hasSize(1);
        assertThat(cte.ctes().get(0).name()).isEqualTo("cte");
    }

    @Test
    void parseWithNonRecursive() {
        Statement stmt = parser.parse("WITH t AS (SELECT 1) SELECT * FROM t");
        assertThat(stmt).isInstanceOf(CteQuery.class);
        CteQuery cte = (CteQuery) stmt;
        assertThat(cte.recursive()).isFalse();
    }

    @Test
    void parseWithRecursiveMultipleCtes() {
        Statement stmt = parser.parse(
                "WITH RECURSIVE a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b");
        assertThat(stmt).isInstanceOf(CteQuery.class);
        CteQuery cte = (CteQuery) stmt;
        assertThat(cte.recursive()).isTrue();
        assertThat(cte.ctes()).hasSize(2);
        assertThat(cte.ctes().get(0).name()).isEqualTo("a");
        assertThat(cte.ctes().get(1).name()).isEqualTo("b");
    }

    // ── JOIN USING ──

    @Test
    void parseJoinUsingSingleColumn() {
        Statement stmt = parser.parse("SELECT * FROM orders JOIN customers USING (customer_id)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.joins()).hasSize(1);
        JoinClause join = q.joins().get(0);
        assertThat(join.joinType()).isEqualTo(JoinClause.JoinType.INNER);
        assertThat(join.usingColumns()).containsExactly("customer_id");
        assertThat(join.condition()).isEmpty();
    }

    @Test
    void parseJoinUsingMultipleColumns() {
        Statement stmt = parser.parse(
                "SELECT * FROM a LEFT JOIN b USING (x, y, z)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.joins()).hasSize(1);
        JoinClause join = q.joins().get(0);
        assertThat(join.joinType()).isEqualTo(JoinClause.JoinType.LEFT);
        assertThat(join.usingColumns()).containsExactly("x", "y", "z");
    }

    @Test
    void parseJoinOnStillWorks() {
        Statement stmt = parser.parse("SELECT * FROM a JOIN b ON a.id = b.id");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.joins()).hasSize(1);
        JoinClause join = q.joins().get(0);
        assertThat(join.condition()).isPresent();
        assertThat(join.usingColumns()).isEmpty();
    }

    // ── CREATE TABLE AS SELECT (CTAS) ──

    @Test
    void parseCreateTableAsSelect() {
        Statement stmt = parser.parse("CREATE TABLE new_table AS SELECT * FROM old_table");
        assertThat(stmt).isInstanceOf(CreateTableAsSelectStatement.class);
        CreateTableAsSelectStatement ctas = (CreateTableAsSelectStatement) stmt;
        assertThat(ctas.tableName().parts()).containsExactly("new_table");
        assertThat(ctas.ifNotExists()).isFalse();
        assertThat(ctas.query()).isInstanceOf(Query.class);
    }

    @Test
    void parseCreateTableAsSelectIfNotExists() {
        Statement stmt = parser.parse(
                "CREATE TABLE IF NOT EXISTS archive AS SELECT id, name FROM users WHERE active = true");
        assertThat(stmt).isInstanceOf(CreateTableAsSelectStatement.class);
        CreateTableAsSelectStatement ctas = (CreateTableAsSelectStatement) stmt;
        assertThat(ctas.tableName().parts()).containsExactly("archive");
        assertThat(ctas.ifNotExists()).isTrue();
    }

    @Test
    void parseCreateTableAsSelectWithQualifiedName() {
        Statement stmt = parser.parse(
                "CREATE TABLE catalog.schema.target AS SELECT a, b FROM source");
        assertThat(stmt).isInstanceOf(CreateTableAsSelectStatement.class);
        CreateTableAsSelectStatement ctas = (CreateTableAsSelectStatement) stmt;
        assertThat(ctas.tableName().parts()).containsExactly("catalog", "schema", "target");
    }

    // ── Parameter Markers ──

    @Test
    void parseParameterMarkerInWhere() {
        Statement stmt = parser.parse("SELECT * FROM t WHERE id = ?");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.where()).isPresent();
    }

    @Test
    void parseParameterMarkerInSelectList() {
        Statement stmt = parser.parse("SELECT ?, name FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.selectItems()).hasSize(2);
        SelectItem first = q.selectItems().get(0);
        assertThat(first).isInstanceOf(ExpressionItem.class);
        Expression expr = ((ExpressionItem) first).expression();
        assertThat(expr).isInstanceOf(ParameterMarker.class);
        assertThat(expr.type()).isEqualTo(ExpressionType.PARAMETER_MARKER);
    }

    @Test
    void parseMultipleParameterMarkers() {
        Statement stmt = parser.parse("SELECT * FROM t WHERE a = ? AND b > ?");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void parseParameterMarkerInInsert() {
        Statement stmt = parser.parse("INSERT INTO t (a, b) VALUES (?, ?)");
        assertThat(stmt).isInstanceOf(InsertStatement.class);
    }

    // ── QUALIFY clause ──

    @Test
    void parseQualifyWithRowNumber() {
        Statement stmt = parser.parse(
                "SELECT id, name FROM employees QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.qualify()).isPresent();
        assertThat(q.where()).isEmpty();
        assertThat(q.having()).isEmpty();
    }

    @Test
    void parseQualifyWithWhereAndGroupBy() {
        Statement stmt = parser.parse(
                "SELECT dept, COUNT(*) as cnt FROM employees WHERE active = true "
                + "GROUP BY dept HAVING COUNT(*) > 1 "
                + "QUALIFY ROW_NUMBER() OVER (ORDER BY cnt DESC) <= 5");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.where()).isPresent();
        assertThat(q.having()).isPresent();
        assertThat(q.qualify()).isPresent();
    }

    @Test
    void parseQualifyAsIdentifier() {
        // QUALIFY is a non-reserved keyword, so it can be used as an identifier
        Statement stmt = parser.parse("SELECT qualify FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.qualify()).isEmpty();
        assertThat(q.selectItems()).hasSize(1);
    }

    // ── TABLESAMPLE clause ──

    @Test
    void parseTableSampleBernoulli() {
        Statement stmt = parser.parse("SELECT * FROM orders TABLESAMPLE BERNOULLI(10)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.from()).hasSize(1);
        TableRef ref = q.from().get(0);
        assertThat(ref.tableSample()).isPresent();
        TableSample sample = ref.tableSample().get();
        assertThat(sample.method()).isEqualTo(TableSample.SamplingMethod.BERNOULLI);
        assertThat(sample.seed()).isEmpty();
    }

    @Test
    void parseTableSampleSystem() {
        Statement stmt = parser.parse("SELECT * FROM orders TABLESAMPLE SYSTEM(25)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.from()).hasSize(1);
        TableRef ref = q.from().get(0);
        assertThat(ref.tableSample()).isPresent();
        TableSample sample = ref.tableSample().get();
        assertThat(sample.method()).isEqualTo(TableSample.SamplingMethod.SYSTEM);
        assertThat(sample.seed()).isEmpty();
    }

    @Test
    void parseTableSampleWithRepeatable() {
        Statement stmt = parser.parse(
                "SELECT * FROM orders TABLESAMPLE BERNOULLI(10) REPEATABLE(42)");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.tableSample()).isPresent();
        TableSample sample = ref.tableSample().get();
        assertThat(sample.method()).isEqualTo(TableSample.SamplingMethod.BERNOULLI);
        assertThat(sample.seed()).isPresent();
    }

    @Test
    void parseTableSampleWithAlias() {
        Statement stmt = parser.parse(
                "SELECT * FROM orders TABLESAMPLE BERNOULLI(5) AS sampled");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        TableRef ref = q.from().get(0);
        assertThat(ref.alias()).isPresent();
        assertThat(ref.alias().get()).isEqualTo("sampled");
        assertThat(ref.tableSample()).isPresent();
    }

    @Test
    void parseTableSampleAsIdentifier() {
        // TABLESAMPLE is a non-reserved keyword, so it can be used as an identifier
        Statement stmt = parser.parse("SELECT tablesample FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        assertThat(q.selectItems()).hasSize(1);
    }
}
