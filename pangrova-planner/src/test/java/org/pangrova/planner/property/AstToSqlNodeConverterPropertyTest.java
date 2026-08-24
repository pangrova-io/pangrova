package org.pangrova.planner.property;

import org.pangrova.planner.parse.AstToSqlNodeConverter;
import net.jqwik.api.*;
import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.CaseExpression;
import org.pangrova.parser.ast.expression.CastExpression;
import org.pangrova.parser.ast.expression.ExistsExpression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.InExpression;
import org.pangrova.parser.ast.expression.InSubqueryExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.LikeExpression;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.ast.expression.StarFunctionCall;
import org.pangrova.parser.ast.expression.SubqueryExpression;
import org.pangrova.parser.ast.expression.BetweenExpression;
import org.pangrova.parser.ast.expression.WindowedFunctionCall;
import org.pangrova.parser.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.DescribeCatalogStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.parser.ast.expression.UnaryMinusExpression;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 2: AST → SqlNode mapping completeness.
 *
 * <p><b>Validates: Requirements 1.3, 9.7</b></p>
 *
 * <p>For any valid AST node type (all Statement and Expression subtypes),
 * {@link AstToSqlNodeConverter#convert} / {@link AstToSqlNodeConverter#convertExpression}
 * should produce a non-null Calcite {@link SqlNode} without throwing "unsupported" exceptions.</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Property 2: AST → SqlNode 映射完整性")
class AstToSqlNodeConverterPropertyTest {

    private final AstToSqlNodeConverter converter = new AstToSqlNodeConverter();

    // ── Property: Statement conversion completeness ──

    @Property(tries = 10)
    void statementConversionProducesNonNullSqlNode(@ForAll("statements") Statement stmt) {
        SqlNode result = converter.convert(stmt);
        assertThat(result).as("convert(%s) must not return null", stmt.getClass().getSimpleName())
                .isNotNull();
    }

    // ── Property: Expression conversion completeness ──

    @Property(tries = 10)
    void expressionConversionProducesNonNullSqlNode(@ForAll("expressions") Expression expr) {
        SqlNode result = converter.convertExpression(expr);
        assertThat(result).as("convertExpression(%s) must not return null", expr.getClass().getSimpleName())
                .isNotNull();
    }

    // ── Providers ──

    @Provide
    Arbitrary<Statement> statements() {
        return Arbitraries.oneOf(
                queryStatements(),
                setQueryStatements(),
                cteQueryStatements(),
                ddlStatements(),
                dmlStatements(),
                catalogStatements(),
                maintenanceStatements(),
                showDescribeStatements(),
                callStatements()
        );
    }

    @Provide
    Arbitrary<Expression> expressions() {
        return Arbitraries.oneOf(
                literalExpressions(),
                identifierExpressions(),
                binaryExpressions(),
                functionCallExpressions(),
                starFunctionCallExpressions(),
                windowedFunctionCallExpressions(),
                notExpressions(),
                inListExpressions(),
                inSubqueryExpressions(),
                notInListExpressions(),
                notInSubqueryExpressions(),
                subqueryExpressions(),
                castExpressions(),
                unaryMinusExpressions(),
                isNullExpressions(),
                betweenExpressions(),
                likeExpressions(),
                existsExpressions(),
                caseExpressions()
        );
    }


    // ── Leaf expression generators (no recursion) ──

    private Arbitrary<Expression> leafExpressions() {
        return Arbitraries.oneOf(literalExpressions(), identifierExpressions());
    }

    private Arbitrary<Expression> literalExpressions() {
        return Arbitraries.oneOf(
                Arbitraries.longs().between(-1000, 1000).map(Literal::ofInteger),
                Arbitraries.doubles().between(-1000.0, 1000.0)
                        .filter(d -> !Double.isNaN(d) && !Double.isInfinite(d))
                        .map(d -> Literal.ofDecimal(java.math.BigDecimal.valueOf(d))),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).map(Literal::ofString),
                Arbitraries.of(true, false).map(Literal::ofBoolean),
                Arbitraries.just(Literal.ofNull()),
                validDates().map(Literal::ofDate),
                validTimestamps().map(Literal::ofTimestamp)
        );
    }

    private Arbitrary<Expression> identifierExpressions() {
        return identifiers().map(id -> id);
    }

    // ── Composite expression generators ──

    private Arbitrary<Expression> binaryExpressions() {
        return Combinators.combine(leafExpressions(),
                        Arbitraries.of(BinaryExpression.Operator.values()),
                        leafExpressions())
                .as(BinaryExpression::new);
    }

    private Arbitrary<Expression> functionCallExpressions() {
        return Combinators.combine(
                        Arbitraries.of("ABS", "UPPER", "LOWER", "LENGTH", "COUNT", "SUM", "AVG", "MIN", "MAX")
                                .map(n -> Identifier.of(n)),
                        leafExpressions().list().ofMinSize(1).ofMaxSize(3),
                        Arbitraries.of(true, false))
                .as(FunctionCall::new);
    }

    private Arbitrary<Expression> starFunctionCallExpressions() {
        return Arbitraries.of("COUNT", "SUM").map(n ->
                new StarFunctionCall(Identifier.of(n), Optional.empty()));
    }

    private Arbitrary<Expression> windowedFunctionCallExpressions() {
        return Combinators.combine(
                        Arbitraries.of("ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "COUNT")
                                .map(n -> Identifier.of(n)),
                        leafExpressions().list().ofMinSize(0).ofMaxSize(2),
                        Arbitraries.of(false),
                        windowSpecs())
                .as(WindowedFunctionCall::new);
    }

    private Arbitrary<Expression> notExpressions() {
        return leafExpressions().map(NotExpression::new);
    }

    private Arbitrary<Expression> inListExpressions() {
        return Combinators.combine(leafExpressions(),
                        leafExpressions().list().ofMinSize(1).ofMaxSize(3))
                .as(InExpression::new);
    }

    private Arbitrary<Expression> inSubqueryExpressions() {
        return Combinators.combine(leafExpressions(), simpleQueries())
                .as(InSubqueryExpression::new);
    }

    private Arbitrary<Expression> notInListExpressions() {
        return Combinators.combine(leafExpressions(),
                        leafExpressions().list().ofMinSize(1).ofMaxSize(3))
                .as(NotInExpression::new);
    }

    private Arbitrary<Expression> notInSubqueryExpressions() {
        return Combinators.combine(leafExpressions(), simpleQueries())
                .as(NotInSubqueryExpression::new);
    }

    private Arbitrary<Expression> subqueryExpressions() {
        return simpleQueries().map(SubqueryExpression::new);
    }

    private Arbitrary<Expression> castExpressions() {
        return Combinators.combine(leafExpressions(),
                        Arbitraries.of("BIGINT", "INT", "INTEGER", "VARCHAR", "STRING",
                                "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"))
                .as(CastExpression::new);
    }

    private Arbitrary<Expression> unaryMinusExpressions() {
        return leafExpressions().map(UnaryMinusExpression::new);
    }

    private Arbitrary<Expression> isNullExpressions() {
        return Combinators.combine(leafExpressions(), Arbitraries.of(true, false))
                .as(IsNullExpression::new);
    }

    private Arbitrary<Expression> betweenExpressions() {
        return Combinators.combine(leafExpressions(), leafExpressions(), leafExpressions())
                .as(BetweenExpression::new);
    }

    private Arbitrary<Expression> likeExpressions() {
        return Combinators.combine(leafExpressions(),
                        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(5)
                                .map(s -> (Expression) Literal.ofString("%" + s + "%")))
                .as(LikeExpression::new);
    }

    private Arbitrary<Expression> existsExpressions() {
        return simpleQueries().map(ExistsExpression::new);
    }

    private Arbitrary<Expression> caseExpressions() {
        Arbitrary<CaseExpression.WhenClause> whenClause = Combinators.combine(
                        leafExpressions(), leafExpressions())
                .as(CaseExpression.WhenClause::new);
        return whenClause.list().ofMinSize(1).ofMaxSize(2).map(clauses ->
                new CaseExpression(Optional.empty(), clauses, Optional.of(Literal.ofNull())));
    }


    // ── Statement generators ──

    private Arbitrary<Statement> queryStatements() {
        return simpleQueries().map(q -> q);
    }

    private Arbitrary<Statement> setQueryStatements() {
        return Combinators.combine(
                        simpleQueries().list().ofMinSize(2).ofMaxSize(3),
                        Arbitraries.of(SetQuery.SetOperatorType.values())
                                .list().ofMinSize(1).ofMaxSize(2))
                .as((queries, ops) -> {
                    // ensure operators.size() == queries.size() - 1
                    List<SetQuery.SetOperatorType> trimmed = ops.subList(0,
                            Math.min(ops.size(), queries.size() - 1));
                    while (trimmed.size() < queries.size() - 1) {
                        trimmed = new java.util.ArrayList<>(trimmed);
                        trimmed.add(SetQuery.SetOperatorType.UNION_ALL);
                    }
                    return (Statement) new SetQuery(queries, trimmed);
                });
    }

    private Arbitrary<Statement> cteQueryStatements() {
        return Combinators.combine(
                        Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8),
                        simpleQueries(),
                        simpleQueries())
                .as((name, cteBody, mainBody) -> {
                    CteQuery.CteDefinition def = new CteQuery.CteDefinition(name, cteBody);
                    return (Statement) new CteQuery(List.of(def), mainBody);
                });
    }

    private Arbitrary<Statement> ddlStatements() {
        return Arbitraries.oneOf(
                identifiers().map(id -> new CreateTableStatement(id,
                        List.of(new CreateTableStatement.ColumnDefinition("id", DataTypeSpec.fromRawTypeString("BIGINT"))),
                        false, List.of(), List.of(), Optional.empty(), Map.of())),
                identifiers().map(id -> new DropTableStatement(id, false)),
                identifiers().map(id -> new AlterTableStatement.AddColumn(id, "col", DataTypeSpec.fromRawTypeString("VARCHAR"))),
                identifiers().map(id -> new AlterTableStatement.DropColumn(id, "col")),
                identifiers().map(id -> new AlterTableStatement.RenameColumn(id, "old_col", "new_col")),
                identifiers().map(id -> new AlterTableStatement.ChangeColumnType(id, "col", DataTypeSpec.fromRawTypeString("BIGINT"))),
                identifiers().map(id -> new CreateSchemaStatement(id, false))
        );
    }

    private Arbitrary<Statement> dmlStatements() {
        return Arbitraries.oneOf(
                identifiers().map(id -> new InsertStatement(id, Optional.empty(),
                        List.of(List.of(Literal.ofInteger(1))))),
                identifiers().map(id -> new InsertOverwriteStatement(id, Optional.empty(),
                        List.of(List.of(Literal.ofInteger(1))))),
                Combinators.combine(identifiers(), simpleQueries())
                        .as((id, q) -> (Statement) new InsertSelectStatement(id, Optional.empty(),
                                false, new SetQuery(List.of(q)))),
                identifiers().map(id -> new DeleteStatement(id, Optional.empty())),
                identifiers().map(id -> new UpdateStatement(id,
                        List.of(new UpdateStatement.Assignment("col", Literal.ofInteger(1))),
                        Optional.empty())),
                Combinators.combine(identifiers(), identifiers())
                        .as((target, source) -> (Statement) new MergeStatement(target, Optional.empty(),
                                new TableRef(source),
                                new BinaryExpression(Identifier.of("a"),
                                        BinaryExpression.Operator.EQUAL, Identifier.of("b")),
                                List.of()))
        );
    }

    private Arbitrary<Statement> catalogStatements() {
        return Arbitraries.oneOf(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(n -> new CreateCatalogStatement(n, Optional.empty(), Map.of())),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(DropCatalogStatement::new),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(DescribeCatalogStatement::new),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(n -> new AlterCatalogStatement(n, Map.of()))
        );
    }

    private Arbitrary<Statement> maintenanceStatements() {
        return Arbitraries.oneOf(
                identifiers().map(CompactStatement::new),
                identifiers().map(VacuumStatement::new),
                identifiers().map(OptimizeStatement::new)
        );
    }

    private Arbitrary<Statement> showDescribeStatements() {
        return Arbitraries.oneOf(
                Arbitraries.just(new ShowCatalogsStatement()),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .map(ShowSchemasStatement::new),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                        .list().ofMinSize(1).ofMaxSize(2)
                        .map(ShowTablesStatement::new),
                identifiers().map(DescribeTableStatement::new)
        );
    }

    private Arbitrary<Statement> callStatements() {
        return Combinators.combine(identifiers(),
                        leafExpressions().list().ofMinSize(0).ofMaxSize(3))
                .as(CallStatement::new);
    }


    // ── Shared helper generators ──

    private Arbitrary<Identifier> identifiers() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10)
                .list().ofMinSize(1).ofMaxSize(3)
                .map(Identifier::new);
    }

    private Arbitrary<Query> simpleQueries() {
        return Combinators.combine(
                        Arbitraries.of(true, false),
                        leafExpressions().map(e -> (SelectItem) new ExpressionItem(e))
                                .list().ofMinSize(1).ofMaxSize(3))
                .as((distinct, items) -> new Query(distinct, items,
                        List.of(new TableRef(Identifier.of("t"))),
                        Optional.empty(), List.of(), Optional.empty(),
                        List.of(), Optional.empty()));
    }

    private Arbitrary<WindowSpec> windowSpecs() {
        return Combinators.combine(
                        identifierExpressions().list().ofMinSize(0).ofMaxSize(2),
                        Arbitraries.just(List.<SortItem>of()))
                .as((partBy, orderBy) -> new WindowSpec(partBy, orderBy));
    }

    private Arbitrary<String> validDates() {
        return Combinators.combine(
                        Arbitraries.integers().between(2000, 2030),
                        Arbitraries.integers().between(1, 12),
                        Arbitraries.integers().between(1, 28))
                .as((y, m, d) -> String.format("%04d-%02d-%02d", y, m, d));
    }

    private Arbitrary<String> validTimestamps() {
        return Combinators.combine(
                        validDates(),
                        Arbitraries.integers().between(0, 23),
                        Arbitraries.integers().between(0, 59),
                        Arbitraries.integers().between(0, 59))
                .as((date, h, min, s) -> String.format("%s %02d:%02d:%02d", date, h, min, s));
    }
}
