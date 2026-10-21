package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.expression.ArrayConstructor;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.CaseExpression;
import org.pangrova.parser.ast.expression.CastExpression;
import org.pangrova.parser.ast.expression.ExistsExpression;
import org.pangrova.parser.ast.expression.ExtractExpression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.InExpression;
import org.pangrova.parser.ast.expression.InSubqueryExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.LikeExpression;
import org.pangrova.parser.ast.expression.MapConstructor;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.ast.expression.RerankClause;
import org.pangrova.parser.ast.expression.RerankClause.RerankStrategy;
import org.pangrova.parser.ast.expression.StarFunctionCall;
import org.pangrova.parser.ast.expression.StructConstructor;
import org.pangrova.parser.ast.expression.SubstringExpression;
import org.pangrova.parser.ast.expression.SubqueryExpression;
import org.pangrova.parser.ast.expression.BetweenExpression;
import org.pangrova.parser.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.ast.expression.WindowedFunctionCall;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Formats AST nodes back into valid SQL text (pretty printer).
 * Supports round-trip: parse(print(ast)) == ast.
 *
 * <p>Statement dispatch is split: query/set/CTE live here;
 * DDL, DML, Dynamic Table, and Show/Describe are in {@link StatementPrinter}.</p>
 */
public final class AstPrinter {

    private final StatementPrinter stmtPrinter = new StatementPrinter(this);

    private final Map<Class<? extends Statement>, Function<Statement, String>> stmtDispatch =
            buildStmtDispatch();

    private Map<Class<? extends Statement>, Function<Statement, String>> buildStmtDispatch() {
        var map = new HashMap<Class<? extends Statement>, Function<Statement, String>>();
        // Query-level statements stay in AstPrinter
        map.put(Query.class, s -> printQuery((Query) s));
        map.put(CompactStatement.class, s -> "COMPACT " + printId(((CompactStatement) s).tableName()));
        map.put(OptimizeStatement.class, s -> "OPTIMIZE " + printId(((OptimizeStatement) s).tableName()));
        map.put(VacuumStatement.class, s -> "VACUUM " + printId(((VacuumStatement) s).tableName()));
        map.put(SetQuery.class, s -> printSetQuery((SetQuery) s));
        map.put(CteQuery.class, s -> printCteQuery((CteQuery) s));
        // DDL, DML, Dynamic Table, Show/Describe delegated to StatementPrinter
        map.putAll(stmtPrinter.buildDispatch());
        return Map.copyOf(map);
    }

    public String print(Statement stmt) {
        Function<Statement, String> handler = stmtDispatch.get(stmt.getClass());
        if (handler != null) return handler.apply(stmt);
        throw new IllegalArgumentException("Unknown statement: " + stmt.getClass());
    }

    // ── Query printers (kept here — expression-heavy) ──

    String printQuery(Query q) {
        var sb = new StringBuilder("SELECT ");
        if (q.distinct()) sb.append("DISTINCT ");
        sb.append(joinItems(q.selectItems()));
        appendListClause(sb, " FROM ", q.from(), this::joinTableRefs);
        for (JoinClause join : q.joins()) {
            sb.append(' ').append(joinTypeText(join.joinType())).append(" JOIN ");
            sb.append(printRef(join.right()));
            join.condition().ifPresent(c -> sb.append(" ON ").append(printExpr(c)));
        }
        q.where().ifPresent(w -> sb.append(" WHERE ").append(printExpr(w)));
        appendListClause(sb, " GROUP BY ", q.groupBy(), this::joinGroupByItems);
        q.having().ifPresent(h -> sb.append(" HAVING ").append(printExpr(h)));
        q.qualify().ifPresent(qf -> sb.append(" QUALIFY ").append(printExpr(qf)));
        appendListClause(sb, " ORDER BY ", q.orderBy(), this::joinSortItems);
        q.rerankBy().ifPresent(r -> sb.append(" RERANK BY ").append(printRerankClause(r)));
        q.limit().ifPresent(l -> sb.append(" LIMIT ").append(printExpr(l)));
        return sb.toString();
    }

    private static String joinTypeText(JoinClause.JoinType type) {
        return switch (type) {
            case INNER -> "INNER";
            case LEFT -> "LEFT";
            case RIGHT -> "RIGHT";
            case FULL -> "FULL OUTER";
            case CROSS -> "CROSS";
            case COMMA -> ",";
        };
    }

    private String printRerankClause(RerankClause clause) {
        var sb = new StringBuilder(printRerankStrategy(clause.strategy()));
        if (!clause.onColumns().isEmpty()) {
            sb.append(" ON (");
            sb.append(clause.onColumns().stream()
                    .map(this::printId)
                    .collect(Collectors.joining(", ")));
            sb.append(')');
        }
        return sb.toString();
    }

    private String printRerankStrategy(RerankStrategy strategy) {
        if (strategy instanceof RerankClause.RrfStrategy rrf) {
            return "rrf(" + printExpr(rrf.k()) + ")";
        }
        if (strategy instanceof RerankClause.LinearCombinationStrategy lc) {
            return "linear_combination(" + printExpr(lc.weight1()) + ", " + printExpr(lc.weight2()) + ")";
        }
        if (strategy instanceof RerankClause.CrossEncoderStrategy ce) {
            return "cross_encoder('" + ce.modelName() + "', " + printExpr(ce.topN()) + ")";
        }
        if (strategy instanceof RerankClause.CascadeStrategy cascade) {
            return "cascade(" + cascade.stages().stream()
                    .map(this::printRerankStrategy)
                    .collect(Collectors.joining(", ")) + ")";
        }
        throw new IllegalArgumentException("Unknown rerank strategy: " + strategy);
    }

    private <T> void appendListClause(StringBuilder sb, String keyword,
                                       List<T> items, Function<List<T>, String> formatter) {
        if (!items.isEmpty()) {
            sb.append(keyword).append(formatter.apply(items));
        }
    }

    String printSetQuery(SetQuery sq) {
        var sb = new StringBuilder();
        for (int i = 0; i < sq.queries().size(); i++) {
            if (i > 0) {
                sb.append(' ').append(setOpText(sq.operators().get(i - 1))).append(' ');
            }
            sb.append(printQuery(sq.queries().get(i)));
        }
        return sb.toString();
    }

    private String printCteQuery(CteQuery cte) {
        var sb = new StringBuilder("WITH ");
        for (int i = 0; i < cte.ctes().size(); i++) {
            if (i > 0) sb.append(", ");
            var def = cte.ctes().get(i);
            sb.append(quote(def.name())).append(" AS (").append(print(def.query())).append(')');
        }
        sb.append(' ').append(print(cte.body()));
        return sb.toString();
    }

    private String setOpText(SetQuery.SetOperatorType op) {
        return switch (op) {
            case UNION_ALL -> "UNION ALL";
            case UNION_DISTINCT -> "UNION";
            case INTERSECT -> "INTERSECT";
            case EXCEPT -> "EXCEPT";
        };
    }

    // ── Expression printer ──

    private final Map<Class<? extends Expression>, Function<Expression, String>> exprDispatch =
            buildExprDispatch();

    private Map<Class<? extends Expression>, Function<Expression, String>> buildExprDispatch() {
        return Map.ofEntries(
                Map.entry(Literal.class, e -> printLiteral((Literal) e)),
                Map.entry(Identifier.class, e -> printId((Identifier) e)),
                Map.entry(BinaryExpression.class, e -> printBinary((BinaryExpression) e)),
                Map.entry(FunctionCall.class, e -> printFn((FunctionCall) e)),
                Map.entry(NotExpression.class, e -> "NOT " + printExpr(((NotExpression) e).expression())),
                Map.entry(CastExpression.class, e -> printCast((CastExpression) e)),
                Map.entry(InExpression.class, e -> printIn((InExpression) e)),
                Map.entry(NotInExpression.class, e -> printNotIn((NotInExpression) e)),
                Map.entry(InSubqueryExpression.class, e -> printInSubquery((InSubqueryExpression) e)),
                Map.entry(NotInSubqueryExpression.class, e -> printNotInSubquery((NotInSubqueryExpression) e)),
                Map.entry(BetweenExpression.class, e -> printBetween((BetweenExpression) e)),
                Map.entry(CaseExpression.class, e -> printCase((CaseExpression) e)),
                Map.entry(ExistsExpression.class, e -> printExists((ExistsExpression) e)),
                Map.entry(SubqueryExpression.class, e -> printSubquery((SubqueryExpression) e)),
                Map.entry(WindowedFunctionCall.class, e -> printWindowedFn((WindowedFunctionCall) e)),
                Map.entry(StarFunctionCall.class, e -> printStarFn((StarFunctionCall) e)),
                Map.entry(LikeExpression.class, e -> printLike((LikeExpression) e)),
                Map.entry(IsNullExpression.class, e -> printIsNull((IsNullExpression) e)),
                Map.entry(UnaryMinusExpression.class, e -> printUnaryMinus((UnaryMinusExpression) e)),
                Map.entry(ExtractExpression.class, e -> printExtract((ExtractExpression) e)),
                Map.entry(SubstringExpression.class, e -> printSubstringExpr((SubstringExpression) e)),
                Map.entry(ArrayConstructor.class, e -> printArrayConstructor((ArrayConstructor) e)),
                Map.entry(MapConstructor.class, e -> printMapConstructor((MapConstructor) e)),
                Map.entry(StructConstructor.class, e -> printStructConstructor((StructConstructor) e))
        );
    }

    String printExpr(Expression expr) {
        var handler = exprDispatch.get(expr.getClass());
        if (handler != null) return handler.apply(expr);
        throw new IllegalArgumentException("Unknown expression: " + expr.getClass());
    }

    private final Map<Class<? extends LiteralValue>, Function<LiteralValue, String>> literalDispatch =
            buildLiteralDispatch();

    private Map<Class<? extends LiteralValue>, Function<LiteralValue, String>> buildLiteralDispatch() {
        var map = new java.util.HashMap<Class<? extends LiteralValue>, Function<LiteralValue, String>>();
        map.put(LiteralValue.IntegerValue.class, v -> String.valueOf(((LiteralValue.IntegerValue) v).value()));
        map.put(LiteralValue.DecimalValue.class, v -> ((LiteralValue.DecimalValue) v).value().toPlainString());
        map.put(LiteralValue.StringValue.class, v -> "'" + ((LiteralValue.StringValue) v).value().replace("'", "''") + "'");
        map.put(LiteralValue.BooleanValue.class, v -> ((LiteralValue.BooleanValue) v).value() ? "TRUE" : "FALSE");
        map.put(LiteralValue.NullValue.class, v -> "NULL");
        map.put(LiteralValue.DateValue.class, v -> "DATE '" + ((LiteralValue.DateValue) v).value() + "'");
        map.put(LiteralValue.UuidValue.class, v -> "UUID '" + ((LiteralValue.UuidValue) v).value() + "'");
        map.put(LiteralValue.TimestampValue.class, v -> "TIMESTAMP '" + ((LiteralValue.TimestampValue) v).value() + "'");
        map.put(LiteralValue.TimestampWithTimeZoneValue.class, v -> "TIMESTAMP WITH TIME ZONE '" + ((LiteralValue.TimestampWithTimeZoneValue) v).value() + "'");
        map.put(LiteralValue.IntervalValue.class, v -> {
            var iv = (LiteralValue.IntervalValue) v;
            var sb = new StringBuilder("INTERVAL '").append(iv.expression()).append("' ").append(iv.fromUnit());
            if (iv.isRange()) {
                sb.append(" TO ").append(iv.toUnit());
            }
            return sb.toString();
        });
        map.put(LiteralValue.BinaryHexValue.class, v -> "X'" + ((LiteralValue.BinaryHexValue) v).hexDigits() + "'");
        return Map.copyOf(map);
    }

    String printLiteral(Literal lit) {
        var handler = literalDispatch.get(lit.value().getClass());
        if (handler != null) return handler.apply(lit.value());
        throw new IllegalArgumentException("Unknown literal type: " + lit.value().getClass());
    }

    String printId(Identifier id) {
        return id.parts().stream().map(this::quote).collect(Collectors.joining("."));
    }

    private String printBinary(BinaryExpression bin) {
        return wrap(bin.left()) + " " + opSymbol(bin.operator()) + " " + wrap(bin.right());
    }

    private String wrap(Expression expr) {
        if (expr instanceof BinaryExpression || expr instanceof NotExpression) {
            return "(" + printExpr(expr) + ")";
        }
        return printExpr(expr);
    }

    private String printFn(FunctionCall fn) {
        String distinctPrefix = fn.distinct() ? "DISTINCT " : "";
        return printId(fn.name()) + "(" + distinctPrefix + joinExprs(fn.arguments()) + ")";
    }

    private String printCast(CastExpression cast) {
        return "CAST(" + printExpr(cast.operand()) + " AS " + cast.targetType() + ")";
    }

    private String printIn(InExpression in) {
        return printExpr(in.value()) + " IN (" + joinExprs(in.list()) + ")";
    }

    private String printNotIn(NotInExpression notIn) {
        return printExpr(notIn.value()) + " NOT IN (" + joinExprs(notIn.list()) + ")";
    }

    private String printInSubquery(InSubqueryExpression in) {
        return printExpr(in.value()) + " IN (" + printQuery(in.subquery()) + ")";
    }

    private String printNotInSubquery(NotInSubqueryExpression notIn) {
        return printExpr(notIn.value()) + " NOT IN (" + printQuery(notIn.subquery()) + ")";
    }

    private String printBetween(BetweenExpression between) {
        return printExpr(between.value()) + " BETWEEN " + printExpr(between.low())
                + " AND " + printExpr(between.high());
    }

    private String printCase(CaseExpression caseExpr) {
        var sb = new StringBuilder("CASE");
        caseExpr.operand().ifPresent(op -> sb.append(" ").append(printExpr(op)));
        for (var when : caseExpr.whenClauses()) {
            sb.append(" WHEN ").append(printExpr(when.condition()))
              .append(" THEN ").append(printExpr(when.result()));
        }
        caseExpr.elseResult().ifPresent(el -> sb.append(" ELSE ").append(printExpr(el)));
        sb.append(" END");
        return sb.toString();
    }

    private String printExists(ExistsExpression exists) {
        return "EXISTS (" + printQuery(exists.subquery()) + ")";
    }

    private String printSubquery(SubqueryExpression subquery) {
        return "(" + printQuery(subquery.subquery()) + ")";
    }

    private String printWindowedFn(WindowedFunctionCall wfn) {
        String distinctPrefix = wfn.distinct() ? "DISTINCT " : "";
        return printId(wfn.name()) + "(" + distinctPrefix + joinExprs(wfn.arguments()) + ") OVER ("
                + printWindowSpec(wfn.window()) + ")";
    }

    private String printStarFn(StarFunctionCall sfn) {
        var sb = new StringBuilder(printId(sfn.name())).append("(*)");
        sfn.window().ifPresent(w -> sb.append(" OVER (").append(printWindowSpec(w)).append(")"));
        return sb.toString();
    }

    private String printWindowSpec(WindowSpec spec) {
        var sb = new StringBuilder();
        if (!spec.partitionBy().isEmpty()) {
            sb.append("PARTITION BY ").append(joinExprs(spec.partitionBy()));
        }
        if (!spec.orderBy().isEmpty()) {
            if (!sb.isEmpty()) sb.append(" ");
            sb.append("ORDER BY ").append(joinSortItems(spec.orderBy()));
        }
        spec.frame().ifPresent(frame -> {
            if (!sb.isEmpty()) sb.append(" ");
            sb.append(frame.frameType().name()).append(" BETWEEN ").append(printFrameBound(frame.lowerBound()))
              .append(" AND ").append(printFrameBound(frame.upperBound()));
        });
        return sb.toString();
    }

    private String printFrameBound(WindowSpec.FrameBound bound) {
        return switch (bound.type()) {
            case UNBOUNDED_PRECEDING -> "UNBOUNDED PRECEDING";
            case UNBOUNDED_FOLLOWING -> "UNBOUNDED FOLLOWING";
            case CURRENT_ROW -> "CURRENT ROW";
            case EXPR_PRECEDING -> bound.offset().map(this::printExpr).orElse("0") + " PRECEDING";
            case EXPR_FOLLOWING -> bound.offset().map(this::printExpr).orElse("0") + " FOLLOWING";
        };
    }

    private String printLike(LikeExpression like) {
        return printExpr(like.value()) + " LIKE " + printExpr(like.pattern());
    }

    private String printIsNull(IsNullExpression isNull) {
        return printExpr(isNull.operand()) + (isNull.negated() ? " IS NOT NULL" : " IS NULL");
    }

    private String printUnaryMinus(UnaryMinusExpression unary) {
        return "-" + wrap(unary.operand());
    }

    private String printExtract(ExtractExpression ext) {
        return "EXTRACT(" + ext.field() + " FROM " + printExpr(ext.source()) + ")";
    }

    private String printSubstringExpr(SubstringExpression sub) {
        return "SUBSTRING(" + printExpr(sub.source()) + " FROM " + printExpr(sub.start())
                + " FOR " + printExpr(sub.length()) + ")";
    }

    private String printMapConstructor(MapConstructor map) {
        return "MAP[" + joinExprs(map.entries()) + "]";
    }

    private String printArrayConstructor(ArrayConstructor array) {
        return "ARRAY[" + joinExprs(array.elements()) + "]";
    }

    private String printStructConstructor(StructConstructor struct) {
        String fields = struct.fields().stream()
                .map(f -> f.name()
                        .map(n -> quote(n) + " => " + printExpr(f.value()))
                        .orElseGet(() -> printExpr(f.value())))
                .collect(Collectors.joining(", "));
        return "ROW(" + fields + ")";
    }

    // ── Collection helpers ──

    String joinItems(List<SelectItem> items) {
        return items.stream().map(this::printItem).collect(Collectors.joining(", "));
    }

    private String printItem(SelectItem item) {
        if (item instanceof AllColumns) return "*";
        if (item instanceof ExpressionItem ei) {
            String s = printExpr(ei.expression());
            return ei.alias().map(a -> s + " AS " + quote(a)).orElse(s);
        }
        throw new IllegalArgumentException("Unknown select item: " + item.getClass());
    }

    private String joinTableRefs(List<TableRef> refs) {
        return refs.stream().map(this::printRef).collect(Collectors.joining(", "));
    }

    String printRef(TableRef ref) {
        if (ref.isValues()) {
            ValuesTable values = ref.valuesTable().orElseThrow();
            String rows = values.rows().stream()
                    .map(row -> "(" + row.stream().map(this::printExpr)
                            .collect(Collectors.joining(", ")) + ")")
                    .collect(Collectors.joining(", "));
            var sb = new StringBuilder("(VALUES ").append(rows).append(")");
            ref.alias().ifPresent(alias -> sb.append(" AS ").append(quote(alias)));
            if (!values.columnAliases().isEmpty()) {
                sb.append("(").append(values.columnAliases().stream()
                        .map(this::quote).collect(Collectors.joining(", "))).append(")");
            }
            return sb.toString();
        }
        if (ref.isTableFunction()) {
            var funcRef = ref.tableFunctionRef().orElseThrow();
            var sb = new StringBuilder();
            if (funcRef.functionType() == TableFunctionRef.TableFunctionType.UNNEST
                    || funcRef.functionType() == TableFunctionRef.TableFunctionType.UNNEST_WITH_ORDINALITY) {
                sb.append("UNNEST(").append(printExpr(funcRef.argument())).append(")");
                if (funcRef.functionType() == TableFunctionRef.TableFunctionType.UNNEST_WITH_ORDINALITY) {
                    sb.append(" WITH ORDINALITY");
                }
            } else {
                if (ref.lateral()) {
                    sb.append("LATERAL ");
                }
                sb.append("TABLE(");
                sb.append(funcRef.functionType().name());
                sb.append("(").append(printExpr(funcRef.argument()));
                funcRef.endArgument().ifPresent(end ->
                        sb.append(", ").append(printExpr(end)));
                sb.append("))");
            }
            if (funcRef.alias().isPresent()) {
                sb.append(" AS ").append(quote(funcRef.alias().get()));
                if (!funcRef.columnAliases().isEmpty()) {
                    sb.append("(").append(String.join(", ", funcRef.columnAliases())).append(")");
                }
            }
            return sb.toString();
        }
        if (ref.isDerived()) {
            var sb = new StringBuilder();
            if (ref.lateral()) {
                sb.append("LATERAL ");
            }
            sb.append("(").append(print(ref.subquery().orElseThrow())).append(")");
            ref.alias().ifPresent(a -> sb.append(" AS ").append(quote(a)));
            return sb.toString();
        }
        if (ref.isSystemTable()) {
            var sysRef = ref.systemTableRef().orElseThrow();
            var sb = new StringBuilder(printId(sysRef.baseTable()));
            sb.append('$').append(sysRef.systemTableName());
            ref.timeTravelSpec().ifPresent(tt -> sb.append(" ").append(printTimeTravel(tt)));
            ref.alias().ifPresent(a -> sb.append(" AS ").append(quote(a)));
            return sb.toString();
        }
        var sb = new StringBuilder(printId(ref.name()));
        ref.timeTravelSpec().ifPresent(tt -> sb.append(" ").append(printTimeTravel(tt)));
        ref.alias().ifPresent(a -> sb.append(" AS ").append(quote(a)));
        return sb.toString();
    }

    private String printTimeTravel(TimeTravelSpec tt) {
        return switch (tt.type()) {
            case SYSTEM_TIME -> "FOR SYSTEM_TIME AS OF " + printExpr(tt.expression());
            case SYSTEM_VERSION -> "FOR SYSTEM_VERSION AS OF " + printExpr(tt.expression());
            case BRANCH -> "VERSION AS OF BRANCH '" + tt.refName() + "'";
            case TAG -> "VERSION AS OF TAG '" + tt.refName() + "'";
        };
    }

    String joinSortItems(List<SortItem> items) {
        return items.stream().map(this::printSort).collect(Collectors.joining(", "));
    }

    private String printSort(SortItem item) {
        String s = printExpr(item.expression());
        return item.direction().map(d -> s + " " + d.name()).orElse(s);
    }

    String joinExprs(List<Expression> exprs) {
        return exprs.stream().map(this::printExpr).collect(Collectors.joining(", "));
    }

    private String joinGroupByItems(List<GroupByItem> items) {
        return items.stream().map(this::printGroupByItem).collect(Collectors.joining(", "));
    }

    private String printGroupByItem(GroupByItem item) {
        if (item instanceof GroupByExpression gbe) {
            return printExpr(gbe.expression());
        }
        if (item instanceof GroupingSets gs) {
            return "GROUPING SETS ("
                    + gs.sets().stream()
                        .map(set -> set.isEmpty()
                                ? "()"
                                : "(" + set.stream().map(this::printExpr).collect(Collectors.joining(", ")) + ")")
                        .collect(Collectors.joining(", "))
                    + ")";
        }
        if (item instanceof CubeGroupBy cube) {
            return "CUBE(" + joinExprs(cube.columns()) + ")";
        }
        if (item instanceof RollupGroupBy rollup) {
            return "ROLLUP(" + joinExprs(rollup.columns()) + ")";
        }
        throw new IllegalArgumentException("Unknown GroupByItem type: " + item.getClass().getSimpleName());
    }

    String printProps(Map<String, Literal> props) {
        return props.entrySet().stream()
                .map(e -> quote(e.getKey()) + " = " + printLiteral(e.getValue()))
                .collect(Collectors.joining(", "));
    }

    // ── Enum helpers ──

    private String opSymbol(BinaryExpression.Operator op) {
        return switch (op) {
            case ADD -> "+";
            case SUBTRACT -> "-";
            case MULTIPLY -> "*";
            case DIVIDE -> "/";
            case MODULO -> "%";
            case CONCAT -> "||";
            case EQUAL -> "=";
            case NOT_EQUAL -> "<>";
            case LESS_THAN -> "<";
            case GREATER_THAN -> ">";
            case LESS_THAN_OR_EQUAL -> "<=";
            case GREATER_THAN_OR_EQUAL -> ">=";
            case AND -> "AND";
            case OR -> "OR";
        };
    }

    // ── Identifier quoting ──

    String quote(String name) {
        return needsQuoting(name) ? "\"" + name.replace("\"", "\"\"") + "\"" : name;
    }

    private boolean needsQuoting(String name) {
        if (name.isEmpty() || RESERVED.contains(name.toUpperCase())) return true;
        char first = name.charAt(0);
        if (!Character.isLetter(first) && first != '_') return true;
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') return true;
        }
        return false;
    }

    private static final Set<String> RESERVED = Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
            "ORDER", "AS", "ON", "AND", "OR", "NOT", "TRUE", "FALSE",
            "ASC", "DESC", "CREATE", "DROP", "ALTER", "FOR", "OF"
    );
}
