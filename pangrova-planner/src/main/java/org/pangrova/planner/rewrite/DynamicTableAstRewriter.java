package org.pangrova.planner.rewrite;

import org.pangrova.parser.api.ast.CteQuery;
import org.pangrova.parser.api.ast.CubeGroupBy;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionItem;
import org.pangrova.parser.api.ast.GroupByExpression;
import org.pangrova.parser.api.ast.GroupByItem;
import org.pangrova.parser.api.ast.GroupingSets;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.JoinClause;
import org.pangrova.parser.api.ast.NamedWindow;
import org.pangrova.parser.api.ast.PivotClause;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.RollupGroupBy;
import org.pangrova.parser.api.ast.SelectItem;
import org.pangrova.parser.api.ast.SetQuery;
import org.pangrova.parser.api.ast.SortItem;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.TableFunctionRef;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.ast.TableSample;
import org.pangrova.parser.api.ast.UnpivotClause;
import org.pangrova.parser.api.ast.ValuesTable;
import org.pangrova.parser.api.ast.WindowSpec;
import org.pangrova.parser.api.ast.expression.ArrayConstructor;
import org.pangrova.parser.api.ast.expression.BetweenExpression;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.expression.CaseExpression;
import org.pangrova.parser.api.ast.expression.CastExpression;
import org.pangrova.parser.api.ast.expression.ExistsExpression;
import org.pangrova.parser.api.ast.expression.ExtractExpression;
import org.pangrova.parser.api.ast.expression.FunctionCall;
import org.pangrova.parser.api.ast.expression.InExpression;
import org.pangrova.parser.api.ast.expression.InSubqueryExpression;
import org.pangrova.parser.api.ast.expression.IsNullExpression;
import org.pangrova.parser.api.ast.expression.LikeExpression;
import org.pangrova.parser.api.ast.expression.MapConstructor;
import org.pangrova.parser.api.ast.expression.NotExpression;
import org.pangrova.parser.api.ast.expression.NotInExpression;
import org.pangrova.parser.api.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.api.ast.RerankClause;
import org.pangrova.parser.api.ast.expression.StarFunctionCall;
import org.pangrova.parser.api.ast.expression.StructConstructor;
import org.pangrova.parser.api.ast.expression.SubqueryExpression;
import org.pangrova.parser.api.ast.expression.SubscriptExpression;
import org.pangrova.parser.api.ast.expression.SubstringExpression;
import org.pangrova.parser.api.ast.expression.TruthTestExpression;
import org.pangrova.parser.api.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.api.ast.expression.WindowedFunctionCall;
import org.pangrova.parser.api.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Rewrites Dynamic Table references inside an already-parsed AST.
 *
 * <p>{@link DynamicTableSqlRewriter} handles SQL text before parsing. Statements
 * dispatched from a parsed AST (DML and CTAS) never pass through that path, so
 * this rewriter applies the same published-generation substitution directly on
 * the tree and reports the physical-to-logical authority mapping for the
 * resulting plan.</p>
 *
 * <p>CTE names visible in the current scope shadow catalog names and are never
 * rewritten. Reserved generation names are not special-cased here: only durable
 * logical publications resolve, so a client-supplied generation name stays
 * untouched and is rejected later by access control.</p>
 */
public final class DynamicTableAstRewriter {

    private final DynamicTableRewriter dynamicTableRewriter;

    public DynamicTableAstRewriter(DynamicTableRewriter dynamicTableRewriter) {
        this.dynamicTableRewriter = dynamicTableRewriter;
    }

    /** Rewrites one statement and returns the physical-to-logical scan mapping. */
    public RewriteWithAuthority rewrite(Statement statement) {
        Map<QualifiedTableName, QualifiedTableName> physicalToLogical = new HashMap<>();
        Statement rewritten = rewriteStatement(statement, Set.of(), physicalToLogical);
        return new RewriteWithAuthority(rewritten, physicalToLogical);
    }

    /** Rewritten statement plus the durable logical identity of each physical scan. */
    public record RewriteWithAuthority(Statement statement,
            Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        public RewriteWithAuthority {
            physicalToLogical = Map.copyOf(physicalToLogical);
        }
    }

    private Statement rewriteStatement(Statement statement, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        if (statement instanceof Query query) {
            return rewriteQuery(query, cteNames, mapping);
        }
        if (statement instanceof SetQuery setQuery) {
            return rewriteSetQuery(setQuery, cteNames, mapping);
        }
        if (statement instanceof CteQuery cteQuery) {
            return rewriteCteQuery(cteQuery, cteNames, mapping);
        }
        if (statement instanceof InsertSelectStatement insert) {
            return new InsertSelectStatement(insert.tableName(), insert.columns(), insert.overwrite(),
                    rewriteSetQuery(insert.selectQuery(), cteNames, mapping));
        }
        if (statement instanceof InsertOverwriteStatement insert) {
            List<List<Expression>> values = insert.values().stream()
                    .map(row -> row.stream().map(value -> rewriteExpression(value, cteNames, mapping)).toList())
                    .toList();
            return new InsertOverwriteStatement(insert.tableName(), insert.columns(),
                    insert.partitionSpec(), values);
        }
        if (statement instanceof UpdateStatement update) {
            List<UpdateStatement.Assignment> assignments = update.assignments().stream()
                    .map(assignment -> new UpdateStatement.Assignment(assignment.column(),
                            rewriteExpression(assignment.value(), cteNames, mapping)))
                    .toList();
            return new UpdateStatement(update.tableName(), assignments,
                    rewriteOptional(update.where(), cteNames, mapping));
        }
        if (statement instanceof DeleteStatement delete) {
            return new DeleteStatement(delete.tableName(),
                    rewriteOptional(delete.where(), cteNames, mapping));
        }
        if (statement instanceof MergeStatement merge) {
            return new MergeStatement(merge.targetTable(), merge.targetAlias(),
                    rewriteTableRef(merge.sourceTable(), cteNames, mapping),
                    rewriteExpression(merge.onCondition(), cteNames, mapping),
                    merge.clauses().stream()
                            .map(clause -> rewriteClause(clause, cteNames, mapping))
                            .toList());
        }
        if (statement instanceof CreateTableAsSelectStatement ctas) {
            return new CreateTableAsSelectStatement(ctas.tableName(), ctas.ifNotExists(),
                    rewriteStatement(ctas.query(), cteNames, mapping));
        }
        return statement;
    }

    private SetQuery rewriteSetQuery(SetQuery setQuery, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new SetQuery(
                setQuery.queries().stream()
                        .map(query -> rewriteQuery(query, cteNames, mapping)).toList(),
                setQuery.operators(),
                setQuery.orderBy().stream().map(item -> rewriteSortItem(item, cteNames, mapping)).toList(),
                rewriteOptional(setQuery.limit(), cteNames, mapping),
                rewriteOptional(setQuery.offset(), cteNames, mapping));
    }

    private CteQuery rewriteCteQuery(CteQuery cteQuery, Set<String> outerNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        Set<String> visible = new HashSet<>(outerNames);
        cteQuery.ctes().forEach(cte -> visible.add(normalize(cte.name())));
        List<CteQuery.CteDefinition> ctes = cteQuery.ctes().stream()
                .map(cte -> new CteQuery.CteDefinition(cte.name(), cte.columnList(),
                        rewriteStatement(cte.query(), visible, mapping),
                        cte.searchClause(), cte.cycleClause()))
                .toList();
        return new CteQuery(cteQuery.recursive(), ctes,
                rewriteStatement(cteQuery.body(), visible, mapping));
    }

    private MergeStatement.MergeClause rewriteClause(MergeStatement.MergeClause clause,
            Set<String> cteNames, Map<QualifiedTableName, QualifiedTableName> mapping) {
        if (clause instanceof MergeStatement.MergeClause.WhenMatchedUpdate update) {
            List<UpdateStatement.Assignment> assignments = update.assignments().stream()
                    .map(assignment -> new UpdateStatement.Assignment(assignment.column(),
                            rewriteExpression(assignment.value(), cteNames, mapping)))
                    .toList();
            return new MergeStatement.MergeClause.WhenMatchedUpdate(
                    rewriteOptional(update.condition(), cteNames, mapping), assignments);
        }
        if (clause instanceof MergeStatement.MergeClause.WhenMatchedDelete delete) {
            return new MergeStatement.MergeClause.WhenMatchedDelete(
                    rewriteOptional(delete.condition(), cteNames, mapping));
        }
        if (clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert insert) {
            return new MergeStatement.MergeClause.WhenNotMatchedInsert(
                    rewriteOptional(insert.condition(), cteNames, mapping), insert.columns(),
                    insert.values().stream()
                            .map(value -> rewriteExpression(value, cteNames, mapping)).toList());
        }
        return clause;
    }

    private Query rewriteQuery(Query query, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new Query(query.distinct(),
                query.selectItems().stream().map(item -> rewriteSelectItem(item, cteNames, mapping)).toList(),
                query.from().stream().map(ref -> rewriteTableRef(ref, cteNames, mapping)).toList(),
                query.joins().stream().map(join -> new JoinClause(join.joinType(),
                        rewriteTableRef(join.right(), cteNames, mapping),
                        rewriteOptional(join.condition(), cteNames, mapping),
                        join.usingColumns())).toList(),
                rewriteOptional(query.where(), cteNames, mapping),
                query.groupBy().stream().map(item -> rewriteGroupBy(item, cteNames, mapping)).toList(),
                rewriteOptional(query.having(), cteNames, mapping),
                rewriteOptional(query.qualify(), cteNames, mapping),
                query.windowDefinitions().stream()
                        .map(window -> rewriteNamedWindow(window, cteNames, mapping)).toList(),
                query.orderBy().stream().map(item -> rewriteSortItem(item, cteNames, mapping)).toList(),
                query.rerankBy().map(rerank -> rewriteRerank(rerank, cteNames, mapping)),
                rewriteOptional(query.limit(), cteNames, mapping),
                rewriteOptional(query.offset(), cteNames, mapping));
    }

    private SelectItem rewriteSelectItem(SelectItem item, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        if (item instanceof ExpressionItem expressionItem) {
            return new ExpressionItem(rewriteExpression(expressionItem.expression(), cteNames, mapping),
                    expressionItem.alias());
        }
        return item;
    }

    private GroupByItem rewriteGroupBy(GroupByItem item, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        if (item instanceof GroupByExpression expression) {
            return new GroupByExpression(rewriteExpression(expression.expression(), cteNames, mapping));
        }
        if (item instanceof GroupingSets groupingSets) {
            return new GroupingSets(groupingSets.sets().stream()
                    .map(set -> set.stream()
                            .map(expression -> rewriteExpression(expression, cteNames, mapping)).toList())
                    .toList());
        }
        if (item instanceof CubeGroupBy cube) {
            return new CubeGroupBy(cube.columns().stream()
                    .map(expression -> rewriteExpression(expression, cteNames, mapping)).toList());
        }
        if (item instanceof RollupGroupBy rollup) {
            return new RollupGroupBy(rollup.columns().stream()
                    .map(expression -> rewriteExpression(expression, cteNames, mapping)).toList());
        }
        return item;
    }

    private SortItem rewriteSortItem(SortItem item, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new SortItem(rewriteExpression(item.expression(), cteNames, mapping),
                item.direction(), item.nullOrdering());
    }

    private NamedWindow rewriteNamedWindow(NamedWindow window, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new NamedWindow(window.name(),
                window.partitionBy().stream()
                        .map(expression -> rewriteExpression(expression, cteNames, mapping)).toList(),
                window.orderBy().stream().map(item -> rewriteSortItem(item, cteNames, mapping)).toList(),
                window.frame().map(frame -> rewriteFrame(frame, cteNames, mapping)));
    }

    private RerankClause rewriteRerank(RerankClause rerank, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new RerankClause(rewriteStrategy(rerank.strategy(), cteNames, mapping),
                rerank.onColumns());
    }

    private RerankClause.RerankStrategy rewriteStrategy(RerankClause.RerankStrategy strategy,
            Set<String> cteNames, Map<QualifiedTableName, QualifiedTableName> mapping) {
        if (strategy instanceof RerankClause.RrfStrategy rrf) {
            return new RerankClause.RrfStrategy(rewriteExpression(rrf.k(), cteNames, mapping));
        }
        if (strategy instanceof RerankClause.LinearCombinationStrategy linear) {
            return new RerankClause.LinearCombinationStrategy(
                    rewriteExpression(linear.weight1(), cteNames, mapping),
                    rewriteExpression(linear.weight2(), cteNames, mapping));
        }
        if (strategy instanceof RerankClause.CrossEncoderStrategy crossEncoder) {
            return new RerankClause.CrossEncoderStrategy(crossEncoder.modelName(),
                    rewriteExpression(crossEncoder.topN(), cteNames, mapping));
        }
        if (strategy instanceof RerankClause.CascadeStrategy cascade) {
            return new RerankClause.CascadeStrategy(cascade.stages().stream()
                    .map(stage -> rewriteStrategy(stage, cteNames, mapping)).toList());
        }
        return strategy;
    }

    private TableRef rewriteTableRef(TableRef ref, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        Optional<Statement> subquery = ref.subquery()
                .map(statement -> rewriteStatement(statement, cteNames, mapping));
        TableFunctionRef functionRef = ref.tableFunctionRef()
                .map(function -> rewriteTableFunction(function, cteNames, mapping))
                .orElse(null);
        TableSample sample = ref.tableSample()
                .map(value -> rewriteTableSample(value, cteNames, mapping))
                .orElse(null);
        PivotClause pivot = ref.pivotClause()
                .map(value -> rewritePivot(value, cteNames, mapping))
                .orElse(null);
        UnpivotClause unpivot = ref.unpivotClause().orElse(null);
        ValuesTable values = ref.valuesTable()
                .map(value -> rewriteValues(value, cteNames, mapping))
                .orElse(null);
        Identifier name = ref.name();
        if (ref.systemTableRef().isEmpty() && !isCteReference(name, cteNames)) {
            name = resolve(name, mapping).orElse(name);
        }
        return new TableRef(name, ref.timeTravelSpec(), ref.alias(), subquery, ref.lateral(),
                Optional.ofNullable(functionRef), ref.systemTableRef(), Optional.ofNullable(sample),
                Optional.ofNullable(pivot), Optional.ofNullable(unpivot), Optional.ofNullable(values));
    }

    private Optional<Identifier> resolve(Identifier name,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        Optional<DynamicTableRewriter.PublishedTarget> target =
                dynamicTableRewriter.resolvePublishedTarget(name.parts());
        if (target.isEmpty()) {
            return Optional.empty();
        }
        var published = target.orElseThrow();
        QualifiedTableName previous = mapping.putIfAbsent(published.physical(), published.logical());
        if (previous != null && !previous.equals(published.logical())) {
            throw new IllegalStateException("Physical Dynamic Table generation has ambiguous authority");
        }
        QualifiedTableName physical = published.physical();
        return Optional.of(new Identifier(
                List.of(physical.catalog(), physical.schema(), physical.table())));
    }

    private static boolean isCteReference(Identifier name, Set<String> cteNames) {
        return name.parts().size() == 1 && cteNames.contains(normalize(name.parts().get(0)));
    }

    private static String normalize(String name) {
        return name.toLowerCase(java.util.Locale.ROOT);
    }

    private TableFunctionRef rewriteTableFunction(TableFunctionRef function, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new TableFunctionRef(function.functionType(),
                rewriteExpression(function.argument(), cteNames, mapping),
                rewriteOptional(function.endArgument(), cteNames, mapping),
                rewriteOptional(function.stepArgument(), cteNames, mapping),
                function.alias(), function.columnAliases());
    }

    private TableSample rewriteTableSample(TableSample sample, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new TableSample(sample.method(),
                rewriteExpression(sample.percentage(), cteNames, mapping),
                rewriteOptional(sample.seed(), cteNames, mapping));
    }

    private PivotClause rewritePivot(PivotClause pivot, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new PivotClause(
                pivot.aggregates().stream()
                        .map(aggregate -> new PivotClause.PivotAggregate(aggregate.function(),
                                rewriteExpression(aggregate.argument(), cteNames, mapping),
                                aggregate.alias()))
                        .toList(),
                pivot.forColumn(),
                pivot.inValues().stream()
                        .map(value -> new PivotClause.PivotValue(
                                rewriteExpression(value.value(), cteNames, mapping), value.alias()))
                        .toList());
    }

    private ValuesTable rewriteValues(ValuesTable values, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new ValuesTable(values.rows().stream()
                .map(row -> row.stream()
                        .map(expression -> rewriteExpression(expression, cteNames, mapping)).toList())
                .toList(), values.columnAliases());
    }

    private Optional<Expression> rewriteOptional(Optional<Expression> expression,
            Set<String> cteNames, Map<QualifiedTableName, QualifiedTableName> mapping) {
        return expression.map(value -> rewriteExpression(value, cteNames, mapping));
    }

    private Expression rewriteExpression(Expression expression, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        if (expression instanceof SubqueryExpression subquery) {
            return new SubqueryExpression(rewriteQuery(subquery.subquery(), cteNames, mapping));
        }
        if (expression instanceof ExistsExpression exists) {
            return new ExistsExpression(rewriteQuery(exists.subquery(), cteNames, mapping));
        }
        if (expression instanceof InSubqueryExpression in) {
            return new InSubqueryExpression(rewriteExpression(in.value(), cteNames, mapping),
                    rewriteQuery(in.subquery(), cteNames, mapping));
        }
        if (expression instanceof NotInSubqueryExpression notIn) {
            return new NotInSubqueryExpression(rewriteExpression(notIn.value(), cteNames, mapping),
                    rewriteQuery(notIn.subquery(), cteNames, mapping));
        }
        if (expression instanceof BinaryExpression binary) {
            return new BinaryExpression(rewriteExpression(binary.left(), cteNames, mapping),
                    binary.operator(), rewriteExpression(binary.right(), cteNames, mapping));
        }
        if (expression instanceof BetweenExpression between) {
            return new BetweenExpression(rewriteExpression(between.value(), cteNames, mapping),
                    rewriteExpression(between.low(), cteNames, mapping),
                    rewriteExpression(between.high(), cteNames, mapping));
        }
        if (expression instanceof CaseExpression caseExpression) {
            return rewriteCase(caseExpression, cteNames, mapping);
        }
        if (expression instanceof CastExpression cast) {
            return new CastExpression(rewriteExpression(cast.operand(), cteNames, mapping),
                    cast.targetType());
        }
        if (expression instanceof ExtractExpression extract) {
            return new ExtractExpression(extract.field(),
                    rewriteExpression(extract.source(), cteNames, mapping));
        }
        if (expression instanceof FunctionCall function) {
            return new FunctionCall(function.name(),
                    rewriteExpressions(function.arguments(), cteNames, mapping),
                    function.distinct(), rewriteOptional(function.filter(), cteNames, mapping));
        }
        if (expression instanceof InExpression in) {
            return new InExpression(rewriteExpression(in.value(), cteNames, mapping),
                    rewriteExpressions(in.list(), cteNames, mapping));
        }
        if (expression instanceof NotInExpression notIn) {
            return new NotInExpression(rewriteExpression(notIn.value(), cteNames, mapping),
                    rewriteExpressions(notIn.list(), cteNames, mapping));
        }
        if (expression instanceof IsNullExpression isNull) {
            return new IsNullExpression(rewriteExpression(isNull.operand(), cteNames, mapping),
                    isNull.negated());
        }
        if (expression instanceof TruthTestExpression truthTest) {
            return new TruthTestExpression(rewriteExpression(truthTest.operand(), cteNames, mapping),
                    truthTest.value(), truthTest.negated());
        }
        if (expression instanceof LikeExpression like) {
            return new LikeExpression(rewriteExpression(like.value(), cteNames, mapping),
                    rewriteExpression(like.pattern(), cteNames, mapping));
        }
        if (expression instanceof NotExpression not) {
            return new NotExpression(rewriteExpression(not.expression(), cteNames, mapping));
        }
        if (expression instanceof StarFunctionCall star) {
            return new StarFunctionCall(star.name(),
                    star.window().map(window -> rewriteWindow(window, cteNames, mapping)),
                    rewriteOptional(star.filter(), cteNames, mapping));
        }
        if (expression instanceof SubstringExpression substring) {
            return new SubstringExpression(
                    rewriteExpression(substring.source(), cteNames, mapping),
                    rewriteExpression(substring.start(), cteNames, mapping),
                    rewriteExpression(substring.length(), cteNames, mapping));
        }
        if (expression instanceof UnaryMinusExpression minus) {
            return new UnaryMinusExpression(rewriteExpression(minus.operand(), cteNames, mapping));
        }
        if (expression instanceof WindowedFunctionCall windowed) {
            return new WindowedFunctionCall(windowed.name(),
                    rewriteExpressions(windowed.arguments(), cteNames, mapping),
                    windowed.distinct(), rewriteWindow(windowed.window(), cteNames, mapping));
        }
        if (expression instanceof ArrayConstructor array) {
            return new ArrayConstructor(rewriteExpressions(array.elements(), cteNames, mapping));
        }
        if (expression instanceof MapConstructor map) {
            return new MapConstructor(rewriteExpressions(map.entries(), cteNames, mapping));
        }
        if (expression instanceof StructConstructor struct) {
            return new StructConstructor(struct.fields().stream()
                    .map(field -> new StructConstructor.Field(field.name(),
                            rewriteExpression(field.value(), cteNames, mapping)))
                    .toList());
        }
        if (expression instanceof SubscriptExpression subscript) {
            return new SubscriptExpression(rewriteExpression(subscript.base(), cteNames, mapping),
                    rewriteExpression(subscript.index(), cteNames, mapping));
        }
        return expression;
    }

    private CaseExpression rewriteCase(CaseExpression expression, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new CaseExpression(rewriteOptional(expression.operand(), cteNames, mapping),
                expression.whenClauses().stream()
                        .map(when -> new CaseExpression.WhenClause(
                                rewriteExpression(when.condition(), cteNames, mapping),
                                rewriteExpression(when.result(), cteNames, mapping)))
                        .toList(),
                rewriteOptional(expression.elseResult(), cteNames, mapping));
    }

    private List<Expression> rewriteExpressions(List<Expression> expressions, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        List<Expression> rewritten = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            rewritten.add(rewriteExpression(expression, cteNames, mapping));
        }
        return rewritten;
    }

    private WindowSpec rewriteWindow(WindowSpec window, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new WindowSpec(window.windowName(),
                window.partitionBy().stream()
                        .map(expression -> rewriteExpression(expression, cteNames, mapping)).toList(),
                window.orderBy().stream().map(item -> rewriteSortItem(item, cteNames, mapping)).toList(),
                window.frame().map(frame -> rewriteFrame(frame, cteNames, mapping)));
    }

    private WindowSpec.WindowFrame rewriteFrame(WindowSpec.WindowFrame frame, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new WindowSpec.WindowFrame(frame.frameType(),
                rewriteBound(frame.lowerBound(), cteNames, mapping),
                rewriteBound(frame.upperBound(), cteNames, mapping));
    }

    private WindowSpec.FrameBound rewriteBound(WindowSpec.FrameBound bound, Set<String> cteNames,
            Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new WindowSpec.FrameBound(bound.type(),
                rewriteOptional(bound.offset(), cteNames, mapping));
    }
}
