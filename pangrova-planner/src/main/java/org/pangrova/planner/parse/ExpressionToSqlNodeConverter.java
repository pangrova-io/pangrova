package org.pangrova.planner.parse;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.WindowSpec;
import org.pangrova.parser.ast.expression.ArrayConstructor;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.CaseExpression;
import org.pangrova.parser.ast.expression.CastExpression;
import org.pangrova.parser.ast.expression.ExistsExpression;
import org.pangrova.parser.ast.expression.ExtractExpression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.InExpression;
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.LikeExpression;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.ast.expression.StructConstructor;
import org.pangrova.parser.ast.expression.SubstringExpression;
import org.pangrova.parser.ast.expression.SubscriptExpression;
import org.pangrova.parser.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.ast.expression.WindowedFunctionCall;
import org.pangrova.parser.ast.expression.StarFunctionCall;
import org.pangrova.parser.ast.expression.BetweenExpression;
import org.pangrova.parser.ast.expression.InSubqueryExpression;
import org.pangrova.parser.ast.expression.SubqueryExpression;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Converts Pangrova AST expression nodes to Calcite SqlNode.
 * Extracted from {@link AstToSqlNodeConverter} to keep class size under 500 lines.
 * Covers: literals, binary/unary, function calls, window functions, CASE, CAST,
 * IN/NOT IN, BETWEEN, LIKE, IS NULL, EXISTS, subquery expressions.
 *
 * <p>Expression dispatch uses a type-keyed map to keep branch count per method ≤ 3.</p>
 */
final class ExpressionToSqlNodeConverter {

    private static final SqlParserPos POS = SqlParserPos.ZERO;

    private final AstToSqlNodeConverter parent;
    private final Map<Class<? extends Expression>, Function<Expression, SqlNode>> exprDispatch;

    ExpressionToSqlNodeConverter(AstToSqlNodeConverter parent) {
        this.parent = parent;
        this.exprDispatch = buildExprDispatch();
    }

    private Map<Class<? extends Expression>, Function<Expression, SqlNode>> buildExprDispatch() {
        return Map.ofEntries(
                entry(Literal.class, e -> convertLiteral((Literal) e)),
                entry(Identifier.class, e -> id((Identifier) e)),
                entry(BinaryExpression.class, e -> convertBinary((BinaryExpression) e)),
                entry(FunctionCall.class, e -> convertFunctionCall((FunctionCall) e)),
                entry(StarFunctionCall.class, e -> convertStarFunctionCall((StarFunctionCall) e)),
                entry(WindowedFunctionCall.class, e -> convertWindowedFunctionCall((WindowedFunctionCall) e)),
                entry(NotExpression.class, e -> convertNot((NotExpression) e)),
                entry(InExpression.class, e -> convertInList((InExpression) e)),
                entry(InSubqueryExpression.class, e -> convertInSubquery((InSubqueryExpression) e)),
                entry(NotInExpression.class, e -> convertNotInList((NotInExpression) e)),
                entry(NotInSubqueryExpression.class, e -> convertNotInSubquery((NotInSubqueryExpression) e)),
                entry(SubqueryExpression.class, e -> convertScalarSubquery((SubqueryExpression) e)),
                entry(CastExpression.class, e -> convertCast((CastExpression) e)),
                entry(UnaryMinusExpression.class, e -> convertUnaryMinus((UnaryMinusExpression) e)),
                entry(IsNullExpression.class, e -> convertIsNull((IsNullExpression) e)),
                entry(BetweenExpression.class, e -> convertBetween((BetweenExpression) e)),
                entry(LikeExpression.class, e -> convertLike((LikeExpression) e)),
                entry(ExistsExpression.class, e -> convertExists((ExistsExpression) e)),
                entry(CaseExpression.class, e -> convertCase((CaseExpression) e)),
                entry(ExtractExpression.class, e -> convertExtract((ExtractExpression) e)),
                entry(SubstringExpression.class, e -> convertSubstring((SubstringExpression) e)),
                entry(ArrayConstructor.class, e -> convertArrayConstructor((ArrayConstructor) e)),
                entry(StructConstructor.class, e -> convertStructConstructor((StructConstructor) e)),
                entry(SubscriptExpression.class, e -> convertSubscript((SubscriptExpression) e))
        );
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return Map.entry(key, value);
    }

    SqlNode convert(Expression expr) {
        Function<Expression, SqlNode> handler = exprDispatch.get(expr.getClass());
        if (handler != null) return handler.apply(expr);
        throw new IllegalArgumentException("Unknown expression type: " + expr.getClass());
    }

    // ── Literals ──

    private final Map<Class<? extends LiteralValue>, Function<LiteralValue, SqlNode>> literalDispatch = Map.of(
            LiteralValue.NullValue.class, v -> SqlLiteral.createNull(POS),
            LiteralValue.IntegerValue.class, v ->
                    SqlLiteral.createExactNumeric(String.valueOf(((LiteralValue.IntegerValue) v).value()), POS),
            LiteralValue.DecimalValue.class, v ->
                    SqlLiteral.createExactNumeric(((LiteralValue.DecimalValue) v).value().toPlainString(), POS),
            LiteralValue.StringValue.class,
                    v -> SqlLiteral.createCharString(((LiteralValue.StringValue) v).value(), POS),
            LiteralValue.DateValue.class, v -> TemporalLiteralSqlNodes.date((LiteralValue.DateValue) v),
            LiteralValue.TimestampValue.class, v ->
                    TemporalLiteralSqlNodes.timestamp((LiteralValue.TimestampValue) v),
            LiteralValue.BooleanValue.class,
                    v -> SqlLiteral.createBoolean(((LiteralValue.BooleanValue) v).value(), POS),
            LiteralValue.IntervalValue.class, v -> convertIntervalLiteral((LiteralValue.IntervalValue) v),
            LiteralValue.BinaryHexValue.class, v -> SqlLiteral.createBinaryString(
                    ((LiteralValue.BinaryHexValue) v).hexDigits(), POS)
    );

    private SqlNode convertLiteral(Literal lit) {
        var handler = literalDispatch.get(lit.value().getClass());
        if (handler != null) return handler.apply(lit.value());
        throw new IllegalArgumentException("Unknown literal type: " + lit.value().getClass());
    }

    // ── Binary / Unary ──

    private SqlNode convertBinary(BinaryExpression bin) {
        SqlNode left = convert(bin.left());
        SqlNode right = convert(bin.right());
        SqlOperator op = mapBinaryOperator(bin.operator());
        return new SqlBasicCall(op, new SqlNode[]{left, right}, POS);
    }

    private SqlNode convertNot(NotExpression not) {
        return new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[]{convert(not.expression())}, POS);
    }

    private SqlNode convertUnaryMinus(UnaryMinusExpression um) {
        return SqlStdOperatorTable.UNARY_MINUS.createCall(POS, convert(um.operand()));
    }

    // ── Function calls ──

    SqlNode convertFunctionCall(FunctionCall fn) {
        SqlIdentifier name = new SqlIdentifier(fn.name().parts(), POS);
        SqlNode[] operands = fn.arguments().stream().map(this::convert).toArray(SqlNode[]::new);
        String fnName = String.join(".", fn.name().parts()).toUpperCase();
        SqlOperator op = resolveAggOperator(fnName);
        if (op instanceof SqlUnresolvedFunction) {
            op = new SqlUnresolvedFunction(
                    name, null, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }
        SqlLiteral quantifier = fn.distinct()
                ? SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS) : null;
        SqlNode call = op.createCall(quantifier, POS, operands);
        return wrapAggregateFilter(call, fn.filter());
    }

    private SqlNode convertStarFunctionCall(StarFunctionCall sf) {
        SqlNode agg = wrapAggregateFilter(buildStarAggCall(sf.name()), sf.filter());
        return sf.window().isEmpty() ? agg : buildOverCall(agg, sf.window().get());
    }

    /**
     * Wraps an aggregate call with the SQL:2003 FILTER clause
     * ({@code agg(...) FILTER (WHERE condition)}). Calcite's validator and
     * SqlToRelConverter handle the rest: the condition is pushed down as a
     * boolean input projection and surfaces as {@code AggregateCall.filterArg}.
     */
    private SqlNode wrapAggregateFilter(SqlNode aggCall, Optional<Expression> filter) {
        if (filter.isEmpty()) {
            return aggCall;
        }
        return SqlStdOperatorTable.FILTER.createCall(POS, aggCall, convert(filter.get()));
    }

    private SqlNode convertWindowedFunctionCall(WindowedFunctionCall wf) {
        SqlNode fn = convertFunctionCall(new FunctionCall(wf.name(), wf.arguments(), wf.distinct()));
        return buildOverCall(fn, wf.window());
    }

    private SqlNode buildStarAggCall(Identifier name) {
        String fnName = String.join(".", name.parts()).toUpperCase();
        return new SqlBasicCall(resolveAggOperator(fnName), new SqlNode[]{SqlIdentifier.star(POS)}, POS);
    }

    private static final Map<String, SqlOperator> AGG_OPERATOR_TABLE = Map.ofEntries(
            Map.entry("COUNT", SqlStdOperatorTable.COUNT),
            Map.entry("SUM", SqlStdOperatorTable.SUM),
            Map.entry("AVG", SqlStdOperatorTable.AVG),
            Map.entry("MIN", SqlStdOperatorTable.MIN),
            Map.entry("MAX", SqlStdOperatorTable.MAX),
            Map.entry("ROW", SqlStdOperatorTable.ROW),
            Map.entry("ARRAY_AGG", org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_AGG),
            Map.entry("COLLECT", SqlStdOperatorTable.COLLECT),
            Map.entry("LISTAGG", SqlStdOperatorTable.LISTAGG),
            Map.entry("COALESCE", SqlStdOperatorTable.COALESCE),
            Map.entry("NULLIF", SqlStdOperatorTable.NULLIF),
            Map.entry("UPPER", SqlStdOperatorTable.UPPER),
            Map.entry("LOWER", SqlStdOperatorTable.LOWER),
            Map.entry("TRIM", SqlStdOperatorTable.TRIM),
            Map.entry("ABS", SqlStdOperatorTable.ABS),
            Map.entry("FLOOR", SqlStdOperatorTable.FLOOR),
            Map.entry("CEIL", SqlStdOperatorTable.CEIL),
            Map.entry("CEILING", SqlStdOperatorTable.CEIL),
            Map.entry("STDDEV", SqlStdOperatorTable.STDDEV_SAMP),
            Map.entry("STDDEV_SAMP", SqlStdOperatorTable.STDDEV_SAMP),
            Map.entry("STDDEV_POP", SqlStdOperatorTable.STDDEV_POP),
            Map.entry("VAR_SAMP", SqlStdOperatorTable.VAR_SAMP),
            Map.entry("VARIANCE", SqlStdOperatorTable.VAR_SAMP),
            Map.entry("VAR_POP", SqlStdOperatorTable.VAR_POP)
    );

    static SqlOperator resolveAggOperator(String name) {
        return AGG_OPERATOR_TABLE.getOrDefault(name, lookupStdOperator(name));
    }

    private static SqlOperator lookupStdOperator(String name) {
        List<SqlOperator> ops = new java.util.ArrayList<>();
        SqlStdOperatorTable.instance().lookupOperatorOverloads(
                new SqlIdentifier(name, POS),
                null, org.apache.calcite.sql.SqlSyntax.FUNCTION, ops,
                org.apache.calcite.sql.validate.SqlNameMatchers.withCaseSensitive(false));
        if (!ops.isEmpty()) {
            return ops.get(0);
        }
        return unresolvedFunction(name);
    }

    private static SqlOperator unresolvedFunction(String name) {
        return new SqlUnresolvedFunction(
                new SqlIdentifier(name, POS), null, null, null, null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    // ── Window ──

    SqlNode buildOverCall(SqlNode agg, WindowSpec window) {
        SqlNodeList partitionList = window.partitionBy().isEmpty() ? SqlNodeList.EMPTY
                : new SqlNodeList(window.partitionBy().stream().map(this::convert).toList(), POS);
        SqlNodeList orderList = window.orderBy().isEmpty() ? SqlNodeList.EMPTY
                : parent.convertOrderBy(window.orderBy());
        SqlNode lowerBound = null;
        SqlNode upperBound = null;
        if (window.frame().isPresent()) {
            WindowSpec.WindowFrame frame = window.frame().get();
            lowerBound = convertFrameBound(frame.lowerBound());
            upperBound = convertFrameBound(frame.upperBound());
        }
        SqlWindow sqlWindow = SqlWindow.create(
                null, null, partitionList, orderList, SqlLiteral.createBoolean(
                        window.frame().map(frame -> frame.frameType() == WindowSpec.FrameType.ROWS)
                                .orElse(false), POS),
                lowerBound, upperBound, null, POS);
        return SqlStdOperatorTable.OVER.createCall(POS, agg, sqlWindow);
    }

    private SqlNode convertFrameBound(WindowSpec.FrameBound bound) {
        return switch (bound.type()) {
            case UNBOUNDED_PRECEDING -> SqlWindow.createUnboundedPreceding(POS);
            case UNBOUNDED_FOLLOWING -> SqlWindow.createUnboundedFollowing(POS);
            case CURRENT_ROW -> SqlWindow.createCurrentRow(POS);
            case EXPR_PRECEDING -> SqlWindow.createPreceding(convert(bound.offset().orElseThrow()), POS);
            case EXPR_FOLLOWING -> SqlWindow.createFollowing(convert(bound.offset().orElseThrow()), POS);
        };
    }

    // ── IN / NOT IN ──

    private SqlNode convertInList(InExpression in) {
        SqlNode value = convert(in.value());
        SqlNodeList list = new SqlNodeList(in.list().stream().map(this::convert).toList(), POS);
        return new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[]{value, list}, POS);
    }

    private SqlNode convertInSubquery(InSubqueryExpression inSub) {
        SqlNode value = convert(inSub.value());
        return new SqlBasicCall(SqlStdOperatorTable.IN,
                new SqlNode[]{value, parent.convertQuery(inSub.subquery())}, POS);
    }

    private SqlNode convertNotInList(NotInExpression notIn) {
        SqlNode value = convert(notIn.value());
        SqlNodeList list = new SqlNodeList(notIn.list().stream().map(this::convert).toList(), POS);
        SqlNode inCall = new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[]{value, list}, POS);
        return new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[]{inCall}, POS);
    }

    private SqlNode convertNotInSubquery(NotInSubqueryExpression notInSub) {
        SqlNode value = convert(notInSub.value());
        SqlNode inCall = new SqlBasicCall(SqlStdOperatorTable.IN,
                new SqlNode[]{value, parent.convertQuery(notInSub.subquery())}, POS);
        return new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[]{inCall}, POS);
    }

    // ── Subquery / CAST / IS NULL / BETWEEN / LIKE / EXISTS ──

    private SqlNode convertScalarSubquery(SubqueryExpression sub) {
        return SqlStdOperatorTable.SCALAR_QUERY.createCall(POS, parent.convertQuery(sub.subquery()));
    }

    private SqlNode convertCast(CastExpression cast) {
        SqlNode operand = convert(cast.operand());
        SqlDataTypeSpec typeSpec = mapCastType(cast.targetType());
        return SqlStdOperatorTable.CAST.createCall(POS, operand, typeSpec);
    }

    private SqlNode convertIsNull(IsNullExpression isn) {
        SqlOperator op = isn.negated() ? SqlStdOperatorTable.IS_NOT_NULL : SqlStdOperatorTable.IS_NULL;
        return op.createCall(POS, convert(isn.operand()));
    }

    private SqlNode convertBetween(BetweenExpression bet) {
        return SqlStdOperatorTable.BETWEEN.createCall(POS,
                convert(bet.value()), convert(bet.low()), convert(bet.high()));
    }

    private SqlNode convertLike(LikeExpression like) {
        return SqlStdOperatorTable.LIKE.createCall(POS, convert(like.value()), convert(like.pattern()));
    }

    private SqlNode convertExists(ExistsExpression ex) {
        return SqlStdOperatorTable.EXISTS.createCall(POS, parent.convertQuery(ex.subquery()));
    }

    // ── CASE ──

    private SqlNode convertCase(CaseExpression ce) {
        SqlNode value = ce.operand().map(this::convert).orElse(null);
        List<SqlNode> whenNodes = new java.util.ArrayList<>();
        List<SqlNode> thenNodes = new java.util.ArrayList<>();
        for (CaseExpression.WhenClause wc : ce.whenClauses()) {
            whenNodes.add(convert(wc.condition()));
            thenNodes.add(convert(wc.result()));
        }
        SqlNode elseNode = ce.elseResult().map(this::convert).orElse(SqlLiteral.createNull(POS));
        return new SqlCase(POS, value,
                new SqlNodeList(whenNodes, POS), new SqlNodeList(thenNodes, POS), elseNode);
    }

    // ── INTERVAL / EXTRACT / SUBSTRING ──

    private SqlNode convertIntervalLiteral(LiteralValue.IntervalValue iv) {
        SqlIntervalQualifier qualifier = mapIntervalUnit(iv.fromUnit());
        return SqlLiteral.createInterval(1, iv.expression(), qualifier, POS);
    }

    private static SqlIntervalQualifier mapIntervalUnit(String unit) {
        org.apache.calcite.avatica.util.TimeUnit timeUnit = switch (unit) {
            case "YEAR" -> org.apache.calcite.avatica.util.TimeUnit.YEAR;
            case "MONTH" -> org.apache.calcite.avatica.util.TimeUnit.MONTH;
            case "DAY" -> org.apache.calcite.avatica.util.TimeUnit.DAY;
            case "HOUR" -> org.apache.calcite.avatica.util.TimeUnit.HOUR;
            case "MINUTE" -> org.apache.calcite.avatica.util.TimeUnit.MINUTE;
            case "SECOND" -> org.apache.calcite.avatica.util.TimeUnit.SECOND;
            default -> throw new IllegalArgumentException("Unsupported interval unit: " + unit);
        };
        return new SqlIntervalQualifier(timeUnit, null, POS);
    }

    private SqlNode convertExtract(ExtractExpression ext) {
        org.apache.calcite.avatica.util.TimeUnit timeUnit = switch (ext.field()) {
            case "YEAR" -> org.apache.calcite.avatica.util.TimeUnit.YEAR;
            case "MONTH" -> org.apache.calcite.avatica.util.TimeUnit.MONTH;
            case "DAY" -> org.apache.calcite.avatica.util.TimeUnit.DAY;
            case "HOUR" -> org.apache.calcite.avatica.util.TimeUnit.HOUR;
            case "MINUTE" -> org.apache.calcite.avatica.util.TimeUnit.MINUTE;
            case "SECOND" -> org.apache.calcite.avatica.util.TimeUnit.SECOND;
            default -> throw new IllegalArgumentException("Unsupported EXTRACT field: " + ext.field());
        };
        SqlIntervalQualifier qualifier = new SqlIntervalQualifier(timeUnit, null, POS);
        return SqlStdOperatorTable.EXTRACT.createCall(POS, qualifier, convert(ext.source()));
    }

    private SqlNode convertSubstring(SubstringExpression sub) {
        return SqlStdOperatorTable.SUBSTRING.createCall(POS,
                convert(sub.source()), convert(sub.start()), convert(sub.length()));
    }

    private SqlNode convertArrayConstructor(ArrayConstructor arr) {
        SqlNode[] operands = arr.elements().stream()
                .map(this::convert)
                .toArray(SqlNode[]::new);
        SqlOperator constructor = operands.length == 0
                ? EmptyArraySqlOperator.INSTANCE
                : SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
        return constructor.createCall(POS, operands);
    }

    private SqlNode convertStructConstructor(StructConstructor struct) {
        SqlNode[] operands = struct.fields().stream()
                .map(f -> convert(f.value()))
                .toArray(SqlNode[]::new);
        return SqlStdOperatorTable.ROW.createCall(POS, operands);
    }

    private SqlNode convertSubscript(SubscriptExpression sub) {
        SqlNode base = convert(sub.base());
        SqlNode index = convert(sub.index());
        return SqlStdOperatorTable.ITEM.createCall(POS, base, index);
    }

    // ── Type mapping ──

    private static SqlDataTypeSpec mapCastType(String typeName) {
        if (typeName.startsWith("ARRAY(") && typeName.endsWith(")")) {
            SqlDataTypeSpec element = mapCastType(typeName.substring(6, typeName.length() - 1));
            return new SqlDataTypeSpec(new org.apache.calcite.sql.SqlCollectionTypeNameSpec(
                    element.getTypeNameSpec(), SqlTypeName.ARRAY, POS), POS);
        }
        // Parameterized types: DECIMAL(p,s), NUMERIC(p,s), VARCHAR(n)
        int parenIdx = typeName.indexOf('(');
        String baseName = parenIdx >= 0 ? typeName.substring(0, parenIdx) : typeName;
        int precision = -1;
        int scale = -1;
        boolean scaleSpecified = false;
        if (parenIdx >= 0) {
            String params = typeName.substring(parenIdx + 1, typeName.length() - 1);
            String[] parts = params.split(",");
            precision = Integer.parseInt(parts[0].trim());
            if (parts.length > 1) {
                scale = Integer.parseInt(parts[1].trim());
                scaleSpecified = true;
            }
        }
        if (baseName.equals("TIMESTAMP_NS")) {
            precision = 9;
        }
        SqlTypeName sqlType = switch (baseName) {
            case "BIGINT" -> SqlTypeName.BIGINT;
            case "INT", "INTEGER" -> SqlTypeName.INTEGER;
            case "SMALLINT" -> SqlTypeName.SMALLINT;
            case "TINYINT" -> SqlTypeName.TINYINT;
            case "VARCHAR", "STRING" -> SqlTypeName.VARCHAR;
            case "DOUBLE" -> SqlTypeName.DOUBLE;
            case "FLOAT" -> SqlTypeName.FLOAT;
            case "BOOLEAN" -> SqlTypeName.BOOLEAN;
            case "DATE" -> SqlTypeName.DATE;
            case "TIMESTAMP" -> SqlTypeName.TIMESTAMP;
            case "DECIMAL", "NUMERIC" -> SqlTypeName.DECIMAL;
            case "BINARY", "VARBINARY", "VARIANT" -> SqlTypeName.VARBINARY;
            case "TIMESTAMP_NS" -> SqlTypeName.TIMESTAMP;
            case "GEOMETRY", "GEOGRAPHY" -> SqlTypeName.VARBINARY;
            case "TIME" -> SqlTypeName.TIME;
            default -> throw new IllegalArgumentException("Unsupported CAST type: " + typeName);
        };
        SqlBasicTypeNameSpec spec;
        if (precision < 0) {
            spec = new SqlBasicTypeNameSpec(sqlType, POS);
        } else if (scaleSpecified) {
            spec = new SqlBasicTypeNameSpec(sqlType, precision, scale, POS);
        } else {
            spec = new SqlBasicTypeNameSpec(sqlType, precision, POS);
        }
        return new SqlDataTypeSpec(spec, POS);
    }

    // ── Operator mapping ──

    static SqlOperator mapBinaryOperator(BinaryExpression.Operator op) {
        return switch (op) {
            case ADD -> SqlStdOperatorTable.PLUS;
            case SUBTRACT -> SqlStdOperatorTable.MINUS;
            case MULTIPLY -> SqlStdOperatorTable.MULTIPLY;
            case DIVIDE -> SqlStdOperatorTable.DIVIDE;
            case MODULO -> SqlStdOperatorTable.MOD;
            case EQUAL -> SqlStdOperatorTable.EQUALS;
            case NOT_EQUAL -> SqlStdOperatorTable.NOT_EQUALS;
            case LESS_THAN -> SqlStdOperatorTable.LESS_THAN;
            case GREATER_THAN -> SqlStdOperatorTable.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case CONCAT -> SqlStdOperatorTable.CONCAT;
            case AND -> SqlStdOperatorTable.AND;
            case OR -> SqlStdOperatorTable.OR;
        };
    }

    // ── Helpers ──

    private SqlNode id(Identifier ident) {
        return new SqlIdentifier(ident.parts(), POS);
    }
}
