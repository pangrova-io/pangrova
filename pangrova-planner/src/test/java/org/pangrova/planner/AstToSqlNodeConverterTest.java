package org.pangrova.planner;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.ArrayConstructor;
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
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.apache.calcite.sql.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
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
    void valuesDerivedTableKeepsDirectCalciteConstructorAndColumnAliases() {
        SqlSelect select = (SqlSelect) new PangrovaParserFactory().parseToSqlNode(
                "SELECT X FROM (VALUES (1, 2), (1, 2)) AS t(X, Y)");
        SqlCall alias = (SqlCall) select.getFrom();
        assertThat(alias.getKind()).isEqualTo(SqlKind.AS);
        assertThat(alias.operand(0).getKind()).isEqualTo(SqlKind.VALUES);
        assertThat(alias.getOperandList()).hasSize(4);
        assertThat(alias.operand(2).toString()).contains("X");
        assertThat(alias.operand(3).toString()).contains("Y");
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
        assertThat(node).isInstanceOf(SqlInsert.class);
        assertThat(node.toString()).containsIgnoringCase("VALUES").contains("1");
    }

    @Test
    void convertInsertOverwriteWithStaticPartitionProjectsThePartitionValue() {
        InsertOverwriteStatement stmt = new InsertOverwriteStatement(
                Identifier.of("db", "tbl"), Optional.of(List.of("id")),
                Optional.of(Map.of("region", Literal.ofString("eu"))),
                List.of(List.of(Literal.ofInteger(10))));

        SqlInsert insert = (SqlInsert) converter.convert(stmt);

        assertThat(insert.getTargetColumnList()).hasSize(2);
        assertThat(((SqlIdentifier) insert.getTargetColumnList().get(0)).getSimple())
                .isEqualTo("id");
        assertThat(((SqlIdentifier) insert.getTargetColumnList().get(1)).getSimple())
                .isEqualTo("region");
        SqlCall row = ((SqlCall) insert.getSource()).operand(0);
        assertThat(row.operandCount()).isEqualTo(2);
        assertThat(row.operand(0).toString()).isEqualTo("10");
        assertThat(row.operand(1).toString()).isEqualTo("'eu'");
    }

    @Test
    void convertInsertOverwriteWithStaticPartitionRequiresExplicitColumns() {
        InsertOverwriteStatement stmt = new InsertOverwriteStatement(
                Identifier.of("db", "tbl"), Optional.empty(),
                Optional.of(Map.of("region", Literal.ofString("eu"))),
                List.of(List.of(Literal.ofInteger(10))));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> converter.convert(stmt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicit column list");
    }

    @Test
    void convertInsertValuesPreservesBatchAndColumns() {
        InsertStatement stmt = new InsertStatement(
                Identifier.of("db", "tbl"), Optional.of(List.of("id", "name")),
                List.of(
                        List.of(Literal.ofInteger(1), Literal.ofString("first")),
                        List.of(Literal.ofInteger(2), Literal.ofNull())));

        SqlNode node = converter.convert(stmt);

        assertThat(node).isInstanceOf(SqlInsert.class);
        assertThat(node.toString())
                .containsIgnoringCase("INSERT INTO")
                .containsIgnoringCase("VALUES")
                .contains("first")
                .containsIgnoringCase("NULL");
    }

    @Test
    void convertInsertValuesPreservesTypedVectorFileAndTemporalExpressions() {
        InsertStatement stmt = new InsertStatement(
                Identifier.of("db", "typed_values"),
                Optional.of(List.of("amount", "created_at", "embedding", "asset")),
                List.of(List.of(
                        Literal.ofDecimal(new java.math.BigDecimal("123.450")),
                        Literal.ofTimestamp("2026-09-19 08:30:00"),
                        new ArrayConstructor(List.of(
                                Literal.ofDecimal(new java.math.BigDecimal("0.25")),
                                Literal.ofDecimal(new java.math.BigDecimal("0.75")))),
                        new FunctionCall(Identifier.of("FILE_FROM_URL"),
                                List.of(Literal.ofString("https://example.invalid/a.pdf"))))));

        String sql = converter.convert(stmt).toString();

        assertThat(sql).contains("123.450", "2026-09-19 08:30:00", "0.25", "0.75")
                .containsIgnoringCase("FILE_FROM_URL")
                .containsIgnoringCase("VALUES");
    }

    @Test
    void convertInsertValuesPreservesEmptyArrayLiteral() {
        InsertStatement stmt = new InsertStatement(
                Identifier.of("db", "typed_values"), Optional.of(List.of("tags")),
                List.of(List.of(new ArrayConstructor(List.of()))));

        SqlInsert node = (SqlInsert) converter.convert(stmt);

        assertThat(node.toString()).contains("ARRAY[]");
        SqlCall values = (SqlCall) node.getSource();
        SqlCall row = values.operand(0);
        SqlCall array = row.operand(0);
        assertThat(array.getOperator().getClass().getSimpleName())
                .isEqualTo("EmptyArraySqlOperator");
        assertThat(array.operandCount()).isZero();

        SqlInsert parsed = (SqlInsert) new PangrovaParserFactory()
                .parseToSqlNode("INSERT INTO db.typed_values (tags) VALUES (ARRAY[])");
        SqlCall parsedRow = ((SqlCall) parsed.getSource()).operand(0);
        SqlCall parsedArray = parsedRow.operand(0);
        assertThat(parsedArray.getOperator().getClass().getSimpleName())
                .isEqualTo("EmptyArraySqlOperator");
    }

    @Test
    void convertInsertValuesPreservesMapConstructor() {
        SqlInsert insert = (SqlInsert) new PangrovaParserFactory().parseToSqlNode(
                "INSERT INTO db.typed_values (props) VALUES (MAP['width', 100, 'height', 200])");
        SqlCall row = ((SqlCall) insert.getSource()).operand(0);
        SqlCall map = row.operand(0);

        assertThat(map.getOperator()).isSameAs(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR);
        assertThat(map.getOperandList()).hasSize(4);
        assertThat(map.toString()).contains("'width'", "100", "'height'", "200");
    }

    @Test
    void convertInsertValuesRejectsEmptyBatch() {
        InsertStatement stmt = new InsertStatement(
                Identifier.of("db", "tbl"), Optional.empty(), List.of());

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> converter.convert(stmt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one row");
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
