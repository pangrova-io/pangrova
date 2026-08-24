package org.pangrova.planner;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.planner.parse.AstToSqlNodeConverter;
import org.apache.calcite.sql.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AstToSqlNodeConverterTest {

    private final AstToSqlNodeConverter converter = new AstToSqlNodeConverter();

    @Test
    void convertIntegerLiteral() {
        Query query = selectExpr(Literal.ofInteger(42));
        SqlNode node = converter.convert(query);
        assertThat(node).isInstanceOf(SqlSelect.class);
        assertThat(node.toString()).contains("42");
    }

    @Test
    void convertStringLiteral() {
        Query query = selectExpr(Literal.ofString("hello"));
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("'hello'");
    }

    @Test
    void convertBooleanLiteral() {
        Query query = selectExpr(Literal.ofBoolean(true));
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("TRUE");
    }

    @Test
    void convertDecimalLiteral() {
        Query query = selectExpr(Literal.ofDecimal(new java.math.BigDecimal("3.14")));
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("3.14");
    }

    @Test
    void convertBinaryAdd() {
        Expression add = new BinaryExpression(
                Literal.ofInteger(1), BinaryExpression.Operator.ADD, Literal.ofInteger(2));
        Query query = selectExpr(add);
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("1 + 2");
    }

    @Test
    void convertIdentifier() {
        Query query = selectExpr(Identifier.of("col1"));
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("col1");
    }

    @Test
    void convertQualifiedIdentifier() {
        Query query = selectExpr(Identifier.of("schema", "table", "col"));
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("schema");
    }

    @Test
    void convertFunctionCall() {
        FunctionCall fn = new FunctionCall(Identifier.of("ABS"), List.of(Literal.ofInteger(1)));
        Query query = selectExpr(fn);
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("ABS");
    }

    @Test
    void convertNotExpression() {
        NotExpression not = new NotExpression(Literal.ofBoolean(true));
        Query query = selectExpr(not);
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).containsIgnoringCase("NOT");
    }

    @Test
    void convertAllColumns() {
        Query query = new Query(false,
                List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of("t"))),
                Optional.empty(), List.of(), Optional.empty(), List.of(), Optional.empty());
        SqlNode node = converter.convert(query);
        assertThat(node.toString()).contains("*");
    }

    @Test
    void convertExpressionItemWithAlias() {
        ExpressionItem item = new ExpressionItem(Literal.ofInteger(1), Optional.of("x"));
        Query query = new Query(false,
                List.of(item), List.of(),
                Optional.empty(), List.of(), Optional.empty(), List.of(), Optional.empty());
        SqlNode node = converter.convert(query);
        String sql = node.toString();
        assertThat(sql).contains("1");
        assertThat(sql).containsIgnoringCase("AS");
    }

    @Test
    void convertCompactStatement() {
        CompactStatement stmt = new CompactStatement(Identifier.of("my_table"));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertShowCatalogs() {
        ShowCatalogsStatement stmt = new ShowCatalogsStatement();
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertComparisonOperators() {
        for (BinaryExpression.Operator op : BinaryExpression.Operator.values()) {
            Expression bin = new BinaryExpression(Literal.ofInteger(1), op, Literal.ofInteger(2));
            Query query = selectExpr(bin);
            SqlNode node = converter.convert(query);
            assertThat(node).isNotNull();
        }
    }

    @Test
    void convertInsertOverwriteStatement() {
        InsertOverwriteStatement stmt = new InsertOverwriteStatement(
                Identifier.of("db", "tbl"), Optional.empty(),
                List.of(List.of(Literal.ofInteger(1))));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertDeleteStatement() {
        DeleteStatement stmt = new DeleteStatement(
                Identifier.of("db", "tbl"), Optional.empty());
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(org.apache.calcite.sql.SqlDelete.class);
    }

    @Test
    void convertUpdateStatement() {
        UpdateStatement stmt = new UpdateStatement(
                Identifier.of("db", "tbl"),
                List.of(new UpdateStatement.Assignment("col1", Literal.ofInteger(42))),
                Optional.empty());
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(org.apache.calcite.sql.SqlUpdate.class);
    }

    @Test
    void convertMergeStatement() {
        MergeStatement stmt = new MergeStatement(
                Identifier.of("target"), Optional.empty(),
                new TableRef(Identifier.of("source")),
                new BinaryExpression(Identifier.of("a"), BinaryExpression.Operator.EQUAL, Identifier.of("b")),
                List.of());
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(org.apache.calcite.sql.SqlMerge.class);
    }

    @Test
    void convertAlterTableAddColumn() {
        AlterTableStatement.AddColumn stmt = new AlterTableStatement.AddColumn(
                Identifier.of("db", "tbl"), "new_col", DataTypeSpec.fromRawTypeString("VARCHAR"));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertCreateSchemaStatement() {
        CreateSchemaStatement stmt = new CreateSchemaStatement(
                Identifier.of("catalog", "my_schema"), false);
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertShowSchemasStatement() {
        ShowSchemasStatement stmt = new ShowSchemasStatement("my_catalog");
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertShowTablesStatement() {
        ShowTablesStatement stmt = new ShowTablesStatement(List.of("catalog", "schema"));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertDescribeTableStatement() {
        DescribeTableStatement stmt = new DescribeTableStatement(Identifier.of("db", "tbl"));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertOptimizeStatement() {
        OptimizeStatement stmt = new OptimizeStatement(Identifier.of("my_table"));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }

    @Test
    void convertVacuumStatement() {
        VacuumStatement stmt = new VacuumStatement(Identifier.of("my_table"));
        SqlNode node = converter.convert(stmt);
        assertThat(node).isInstanceOf(SqlBasicCall.class);
    }


    private Query selectExpr(Expression expr) {
        return new Query(false,
                List.of(new ExpressionItem(expr)),
                List.of(),
                Optional.empty(), List.of(), Optional.empty(), List.of(), Optional.empty());
    }
}
