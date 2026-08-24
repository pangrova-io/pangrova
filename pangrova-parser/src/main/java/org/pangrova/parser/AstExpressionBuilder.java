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
import org.pangrova.parser.ast.expression.IsNullExpression;
import org.pangrova.parser.ast.expression.LikeExpression;
import org.pangrova.parser.ast.expression.MapConstructor;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.NotInExpression;
import org.pangrova.parser.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.ast.expression.StarFunctionCall;
import org.pangrova.parser.ast.expression.StructConstructor;
import org.pangrova.parser.ast.expression.SubstringExpression;
import org.pangrova.parser.ast.expression.SubscriptExpression;
import org.pangrova.parser.ast.expression.BetweenExpression;
import org.pangrova.parser.ast.expression.InSubqueryExpression;
import org.pangrova.parser.ast.expression.SubqueryExpression;
import org.pangrova.parser.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.ast.expression.WindowedFunctionCall;
import org.pangrova.parser.ast.expression.BinaryExpression.Operator;

import java.util.List;
import java.util.Optional;

/**
 * Handles expression and literal parse-tree visitor methods, delegated from AstBuilder.
 * Covers: arithmetic, comparison, logical, CASE, CAST, IN, BETWEEN, LIKE,
 * IS NULL, EXISTS, subquery, function call, window function, and literal expressions.
 */
class AstExpressionBuilder extends PangrovaSqlBaseVisitor<Object> {

    private final AstBuilder parent;

    AstExpressionBuilder(AstBuilder parent) {
        this.parent = parent;
    }

    // ── Expressions ──

    @Override
    public Expression visitPrimaryExpr(PangrovaSqlParser.PrimaryExprContext ctx) {
        return (Expression) visit(ctx.primaryExpression());
    }

    @Override
    public Expression visitSubscriptExpr(PangrovaSqlParser.SubscriptExprContext ctx) {
        Expression base = (Expression) visit(ctx.expression(0));
        Expression index = (Expression) visit(ctx.expression(1));
        return new SubscriptExpression(base, index);
    }

    @Override
    public Expression visitMultiplicativeExpr(PangrovaSqlParser.MultiplicativeExprContext ctx) {
        return buildBinaryExpr(ctx.expression(0), ctx.expression(1), tokenToOperator(ctx.op.getType()));
    }

    @Override
    public Expression visitAdditiveExpr(PangrovaSqlParser.AdditiveExprContext ctx) {
        return buildBinaryExpr(ctx.expression(0), ctx.expression(1), tokenToOperator(ctx.op.getType()));
    }

    @Override
    public Expression visitConcatExpr(PangrovaSqlParser.ConcatExprContext ctx) {
        return buildBinaryExpr(ctx.expression(0), ctx.expression(1), BinaryExpression.Operator.CONCAT);
    }

    @Override
    public Expression visitComparisonExpr(PangrovaSqlParser.ComparisonExprContext ctx) {
        return buildBinaryExpr(ctx.expression(0), ctx.expression(1), comparisonOperator(ctx.comparisonOperator()));
    }

    @Override
    public Expression visitAndExpr(PangrovaSqlParser.AndExprContext ctx) {
        return buildBinaryExpr(ctx.expression(0), ctx.expression(1), Operator.AND);
    }

    @Override
    public Expression visitOrExpr(PangrovaSqlParser.OrExprContext ctx) {
        return buildBinaryExpr(ctx.expression(0), ctx.expression(1), Operator.OR);
    }

    @Override
    public Expression visitNotExpr(PangrovaSqlParser.NotExprContext ctx) {
        return new NotExpression((Expression) visit(ctx.expression()));
    }

    @Override
    public Expression visitUnaryMinusExpr(PangrovaSqlParser.UnaryMinusExprContext ctx) {
        return new UnaryMinusExpression((Expression) visit(ctx.expression()));
    }

    @Override
    public Expression visitInSubqueryExpr(PangrovaSqlParser.InSubqueryExprContext ctx) {
        Expression value = (Expression) visit(ctx.left);
        Query subquery = parent.visitQuery(ctx.query());
        return new InSubqueryExpression(value, subquery);
    }

    @Override
    public Expression visitInListExpr(PangrovaSqlParser.InListExprContext ctx) {
        Expression value = (Expression) visit(ctx.left);
        List<Expression> list = parent.toExpressionList(ctx.expressionList());
        return new InExpression(value, list);
    }

    @Override
    public Expression visitNotInSubqueryExpr(PangrovaSqlParser.NotInSubqueryExprContext ctx) {
        Expression value = (Expression) visit(ctx.left);
        Query subquery = parent.visitQuery(ctx.query());
        return new NotInSubqueryExpression(value, subquery);
    }

    @Override
    public Expression visitNotInListExpr(PangrovaSqlParser.NotInListExprContext ctx) {
        Expression value = (Expression) visit(ctx.left);
        List<Expression> list = parent.toExpressionList(ctx.expressionList());
        return new NotInExpression(value, list);
    }

    @Override
    public Expression visitParenExpr(PangrovaSqlParser.ParenExprContext ctx) {
        return (Expression) visit(ctx.expression());
    }

    @Override
    public Expression visitIsNullExpr(PangrovaSqlParser.IsNullExprContext ctx) {
        Expression operand = (Expression) visit(ctx.left);
        boolean negated = ctx.NOT() != null;
        return new IsNullExpression(operand, negated);
    }

    @Override
    public Expression visitBetweenExpr(PangrovaSqlParser.BetweenExprContext ctx) {
        Expression value = (Expression) visit(ctx.left);
        Expression low = (Expression) visit(ctx.low);
        Expression high = (Expression) visit(ctx.high);
        return rebalanceBetweenAnd(value, low, high);
    }

    /**
     * Handle SQL BETWEEN-AND precedence vs logical AND precedence.
     *
     * <p>The ANTLR4 grammar rule {@code expression BETWEEN low=expression AND high=expression}
     * generates parser calls that let any of {@code value}, {@code low}, or
     * {@code high} greedily consume logical AND chains. SQL semantics require
     * BETWEEN to bind tighter than AND, so {@code x BETWEEN a AND b AND c} must
     * parse as {@code (x BETWEEN a AND b) AND c}, never {@code x BETWEEN (a AND b) AND c}.
     *
     * <p>Depending on the surrounding tokens, the parser may produce any of:
     * <ul>
     *   <li>{@code low} = AndExpression: extract the deepest left chain into outer AND
     *       (e.g. {@code BETWEEN (a AND b) AND c} -> {@code (BETWEEN value a b) AND c})
     *   <li>{@code high} = AndExpression: extract the deepest left chain into outer AND
     *       (e.g. {@code BETWEEN a AND (b AND c) AND d} -> {@code (BETWEEN value a b) AND c AND d})
     *   <li>{@code value} = AndExpression: extract rightmost as real value and AND the rest
     *       (e.g. {@code (A AND B) BETWEEN c AND d} -> {@code A AND (B BETWEEN c AND d)})
     * </ul>
     */
    private static Expression rebalanceBetweenAnd(
            Expression value, Expression low, Expression high) {
        // Case 1: `low` greedily consumed an AND chain (most common, e.g. q13)
        if (isAnd(low)) {
            BinaryExpression deepest = (BinaryExpression) low;
            while (isAnd(deepest.left())) {
                deepest = (BinaryExpression) deepest.left();
            }
            Expression realLow = deepest.left();
            Expression realHigh = deepest.right();
            Expression between = new BetweenExpression(value, realLow, realHigh);
            Expression rewrittenLow = substituteAndLeftSpine(
                    (BinaryExpression) low, deepest, between);
            return new BinaryExpression(rewrittenLow, Operator.AND, high);
        }
        // Case 2: `high` greedily consumed an AND chain (e.g. q21 inner BETWEEN)
        if (isAnd(high)) {
            BinaryExpression deepest = (BinaryExpression) high;
            while (isAnd(deepest.left())) {
                deepest = (BinaryExpression) deepest.left();
            }
            Expression realHigh = deepest.left();
            Expression between = new BetweenExpression(value, low, realHigh);
            // Replace `deepest` (which is And(realHigh, X)) with And(between, X).
            Expression replacement = new BinaryExpression(between, Operator.AND, deepest.right());
            return substituteAndLeftSpine((BinaryExpression) high, deepest, replacement);
        }
        // Case 3: `value` (BETWEEN's left) was greedily consumed as part of an AND
        // chain (e.g. q21 outer BETWEEN, after the inner BETWEEN was rebalanced
        // into an AND tree).
        if (isAnd(value)) {
            BinaryExpression valueAnd = (BinaryExpression) value;
            Expression realValue = valueAnd.right();
            Expression between = new BetweenExpression(realValue, low, high);
            return new BinaryExpression(valueAnd.left(), Operator.AND, between);
        }
        return new BetweenExpression(value, low, high);
    }

    private static boolean isAnd(Expression e) {
        return e instanceof BinaryExpression b && b.operator() == Operator.AND;
    }

    private static Expression substituteAndLeftSpine(
            BinaryExpression node, BinaryExpression target, Expression replacement) {
        if (node == target) {
            return replacement;
        }
        // The target is somewhere on the LEFT spine; only the left subtree needs rebuild.
        Expression newLeft = (node.left() instanceof BinaryExpression bl && isAnd(bl))
                ? substituteAndLeftSpine(bl, target, replacement)
                : node.left();
        return new BinaryExpression(newLeft, Operator.AND, node.right());
    }

    @Override
    public Expression visitLikeExpr(PangrovaSqlParser.LikeExprContext ctx) {
        Expression value = (Expression) visit(ctx.left);
        Expression pattern = (Expression) visit(ctx.right);
        return new LikeExpression(value, pattern);
    }

    @Override
    public Expression visitExistsExpr(PangrovaSqlParser.ExistsExprContext ctx) {
        Query subquery = parent.visitQuery(ctx.query());
        return new ExistsExpression(subquery);
    }

    @Override
    public Expression visitSimpleCaseExpr(PangrovaSqlParser.SimpleCaseExprContext ctx) {
        List<PangrovaSqlParser.ExpressionContext> exprs = ctx.expression();
        Expression operand = (Expression) visit(exprs.get(0));
        List<CaseExpression.WhenClause> whenClauses = buildWhenClauses(exprs, 1);
        Optional<Expression> elseResult = extractCaseElse(ctx.ELSE() != null, exprs);
        return new CaseExpression(Optional.of(operand), whenClauses, elseResult);
    }

    @Override
    public Expression visitSearchedCaseExpr(PangrovaSqlParser.SearchedCaseExprContext ctx) {
        List<PangrovaSqlParser.ExpressionContext> exprs = ctx.expression();
        List<CaseExpression.WhenClause> whenClauses = buildWhenClauses(exprs, 0);
        Optional<Expression> elseResult = extractCaseElse(ctx.ELSE() != null, exprs);
        return new CaseExpression(Optional.empty(), whenClauses, elseResult);
    }

    // ── Primary expressions ──

    @Override
    public Expression visitLiteralExpr(PangrovaSqlParser.LiteralExprContext ctx) {
        return (Expression) visit(ctx.literal());
    }

    @Override
    public Expression visitParameterMarkerExpr(PangrovaSqlParser.ParameterMarkerExprContext ctx) {
        return new org.pangrova.parser.ast.expression.ParameterMarker();
    }

    @Override
    public Expression visitCastExpr(PangrovaSqlParser.CastExprContext ctx) {
        Expression operand = (Expression) visit(ctx.expression());
        String targetType = ctx.castType().getText().toUpperCase();
        return new CastExpression(operand, targetType);
    }

    @Override
    public Expression visitIdentifierExpr(PangrovaSqlParser.IdentifierExprContext ctx) {
        return parent.visitQualifiedName(ctx.qualifiedName());
    }

    @Override
    public Expression visitFunctionCallExpr(PangrovaSqlParser.FunctionCallExprContext ctx) {
        Identifier name = parent.visitQualifiedName(ctx.qualifiedName());
        List<Expression> args = ctx.expression().stream()
                .map(e -> (Expression) visit(e))
                .toList();
        boolean distinct = ctx.setQuantifier != null;
        Optional<Expression> filter = buildFilterClause(ctx.filterClause(), ctx.windowSpec() != null);
        if (ctx.windowSpec() != null) {
            WindowSpec window = visitWindowSpecNode(ctx.windowSpec());
            return new WindowedFunctionCall(name, args, distinct, window);
        }
        return new FunctionCall(name, args, distinct, filter);
    }

    @Override
    public Expression visitStarFunctionCallExpr(PangrovaSqlParser.StarFunctionCallExprContext ctx) {
        Identifier name = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<Expression> filter = buildFilterClause(ctx.filterClause(), ctx.windowSpec() != null);
        Optional<WindowSpec> window = ctx.windowSpec() != null
                ? Optional.of(visitWindowSpecNode(ctx.windowSpec()))
                : Optional.empty();
        return new StarFunctionCall(name, window, filter);
    }

    /**
     * Builds the optional SQL:2003 aggregate FILTER clause condition.
     *
     * <p>FILTER combined with OVER (windowed aggregate filtering) is not
     * supported: the window execution path has no mask plumbing. Fail loud
     * with a targeted message instead of a generic syntax error.</p>
     */
    private Optional<Expression> buildFilterClause(
            PangrovaSqlParser.FilterClauseContext filterCtx, boolean hasWindow) {
        if (filterCtx == null) {
            return Optional.empty();
        }
        if (hasWindow) {
            var token = filterCtx.getStart();
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "FILTER (WHERE ...) combined with OVER (...) is not supported. "
                            + "Rewrite the windowed aggregate using CASE WHEN inside the "
                            + "aggregate arguments instead.",
                    token.getLine(), token.getCharPositionInLine());
        }
        return Optional.of((Expression) visit(filterCtx.expression()));
    }

    @Override
    public Expression visitScalarSubqueryExpr(PangrovaSqlParser.ScalarSubqueryExprContext ctx) {
        Query subquery = parent.visitQuery(ctx.query());
        return new SubqueryExpression(subquery);
    }

    @Override
    public Expression visitArrayConstructorExpr(PangrovaSqlParser.ArrayConstructorExprContext ctx) {
        List<Expression> elements = ctx.expression().stream()
                .map(e -> (Expression) visit(e))
                .toList();
        return new ArrayConstructor(elements);
    }

    @Override
    public Expression visitMapConstructorExpr(PangrovaSqlParser.MapConstructorExprContext ctx) {
        List<Expression> entries = ctx.expression().stream()
                .map(e -> (Expression) visit(e))
                .toList();
        return new MapConstructor(entries);
    }

    @Override
    public Expression visitStructConstructorExpr(PangrovaSqlParser.StructConstructorExprContext ctx) {
        List<StructConstructor.Field> fields = ctx.namedField().stream()
                .map(this::buildStructField)
                .toList();
        return new StructConstructor(fields);
    }

    private StructConstructor.Field buildStructField(PangrovaSqlParser.NamedFieldContext ctx) {
        if (ctx instanceof PangrovaSqlParser.NamedFieldWithNameContext named) {
            String fieldName = named.identifier().getText();
            Expression value = (Expression) visit(named.expression());
            return StructConstructor.Field.named(fieldName, value);
        }
        PangrovaSqlParser.PositionalFieldContext positional = (PangrovaSqlParser.PositionalFieldContext) ctx;
        Expression value = (Expression) visit(positional.expression());
        return StructConstructor.Field.positional(value);
    }

    // ── Window spec ──

    WindowSpec visitWindowSpecNode(PangrovaSqlParser.WindowSpecContext ctx) {
        if (ctx instanceof PangrovaSqlParser.NamedWindowRefContext nwr) {
            return WindowSpec.ofNamedReference(parent.extractIdentifierText(nwr.windowName));
        }
        PangrovaSqlParser.InlineWindowSpecContext iws = (PangrovaSqlParser.InlineWindowSpecContext) ctx;
        List<Expression> partitionBy = iws.expressionList() != null
                ? parent.toExpressionList(iws.expressionList()) : List.of();
        List<SortItem> orderBy = iws.orderByList() != null
                ? iws.orderByList().sortItem().stream().map(si -> (SortItem) visit(si)).toList()
                : List.of();
        Optional<WindowSpec.WindowFrame> frame = iws.windowFrame() != null
                ? Optional.of(visitWindowFrameNode(iws.windowFrame()))
                : Optional.empty();
        return new WindowSpec(partitionBy, orderBy, frame);
    }

    WindowSpec.WindowFrame visitWindowFrameNode(PangrovaSqlParser.WindowFrameContext ctx) {
        if (ctx instanceof PangrovaSqlParser.RowsBetweenFrameContext rb) {
            return new WindowSpec.WindowFrame(WindowSpec.FrameType.ROWS,
                    visitFrameBoundNode(rb.frameBound(0)),
                    visitFrameBoundNode(rb.frameBound(1)));
        }
        if (ctx instanceof PangrovaSqlParser.RowsFrameContext rf) {
            return new WindowSpec.WindowFrame(WindowSpec.FrameType.ROWS,
                    visitFrameBoundNode(rf.frameBound()),
                    new WindowSpec.FrameBound(WindowSpec.FrameBoundType.CURRENT_ROW));
        }
        if (ctx instanceof PangrovaSqlParser.RangeBetweenFrameContext rangeBetween) {
            return new WindowSpec.WindowFrame(WindowSpec.FrameType.RANGE,
                    visitFrameBoundNode(rangeBetween.frameBound(0)),
                    visitFrameBoundNode(rangeBetween.frameBound(1)));
        }
        PangrovaSqlParser.RangeFrameContext rangeFrame = (PangrovaSqlParser.RangeFrameContext) ctx;
        return new WindowSpec.WindowFrame(WindowSpec.FrameType.RANGE,
                visitFrameBoundNode(rangeFrame.frameBound()),
                new WindowSpec.FrameBound(WindowSpec.FrameBoundType.CURRENT_ROW));
    }

    private WindowSpec.FrameBound visitFrameBoundNode(PangrovaSqlParser.FrameBoundContext ctx) {
        if (ctx instanceof PangrovaSqlParser.UnboundedPrecedingContext) {
            return new WindowSpec.FrameBound(WindowSpec.FrameBoundType.UNBOUNDED_PRECEDING);
        }
        if (ctx instanceof PangrovaSqlParser.UnboundedFollowingContext) {
            return new WindowSpec.FrameBound(WindowSpec.FrameBoundType.UNBOUNDED_FOLLOWING);
        }
        if (ctx instanceof PangrovaSqlParser.CurrentRowContext) {
            return new WindowSpec.FrameBound(WindowSpec.FrameBoundType.CURRENT_ROW);
        }
        if (ctx instanceof PangrovaSqlParser.ExprPrecedingContext ep) {
            Expression offset = (Expression) visit(ep.expression());
            return new WindowSpec.FrameBound(WindowSpec.FrameBoundType.EXPR_PRECEDING, Optional.of(offset));
        }
        PangrovaSqlParser.ExprFollowingContext ef = (PangrovaSqlParser.ExprFollowingContext) ctx;
        Expression offset = (Expression) visit(ef.expression());
        return new WindowSpec.FrameBound(WindowSpec.FrameBoundType.EXPR_FOLLOWING, Optional.of(offset));
    }

    // ── Literals ──

    @Override
    public Literal visitIntegerLiteral(PangrovaSqlParser.IntegerLiteralContext ctx) {
        String text = ctx.INTEGER_LITERAL().getText();
        boolean negate = ctx.MINUS() != null;
        long value;
        try {
            value = Long.parseLong(text);
        } catch (NumberFormatException e) {
            if (negate) {
                try {
                    value = Long.parseLong("-" + text);
                    return Literal.ofInteger(value);
                } catch (NumberFormatException ignored) {
                }
            }
            var token = ctx.INTEGER_LITERAL().getSymbol();
            var loc = new SourceLocation(token.getLine(), token.getCharPositionInLine(),
                    token.getTokenIndex(), token.getTokenIndex());
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Invalid integer literal: '" + text + "'", loc, e);
        }
        if (negate) {
            value = -value;
        }
        return Literal.ofInteger(value);
    }

    @Override
    public Literal visitDecimalLiteral(PangrovaSqlParser.DecimalLiteralContext ctx) {
        String text = ctx.DECIMAL_LITERAL().getText();
        java.math.BigDecimal value;
        try {
            value = new java.math.BigDecimal(text);
        } catch (NumberFormatException e) {
            int line = ctx.DECIMAL_LITERAL().getSymbol().getLine();
            int col = ctx.DECIMAL_LITERAL().getSymbol().getCharPositionInLine();
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Invalid decimal literal: '" + text + "'", line, col);
        }
        if (ctx.MINUS() != null) {
            value = value.negate();
        }
        return Literal.ofDecimal(value);
    }

    @Override
    public Literal visitStringLiteral(PangrovaSqlParser.StringLiteralContext ctx) {
        String raw = ctx.STRING_LITERAL().getText();
        return Literal.ofString(raw.substring(1, raw.length() - 1).replace("''", "'"));
    }

    @Override
    public Literal visitBooleanTrue(PangrovaSqlParser.BooleanTrueContext ctx) {
        return Literal.ofBoolean(true);
    }

    @Override
    public Literal visitBooleanFalse(PangrovaSqlParser.BooleanFalseContext ctx) {
        return Literal.ofBoolean(false);
    }

    @Override
    public Literal visitNullLiteral(PangrovaSqlParser.NullLiteralContext ctx) {
        return Literal.ofNull();
    }

    @Override
    public Literal visitDateLiteral(PangrovaSqlParser.DateLiteralContext ctx) {
        String raw = ctx.STRING_LITERAL().getText();
        return Literal.ofDate(raw.substring(1, raw.length() - 1));
    }

    @Override
    public Literal visitTimestampLiteral(PangrovaSqlParser.TimestampLiteralContext ctx) {
        String raw = ctx.STRING_LITERAL().getText();
        return Literal.ofTimestamp(raw.substring(1, raw.length() - 1));
    }

    @Override
    public Literal visitIntervalLiteral(PangrovaSqlParser.IntervalLiteralContext ctx) {
        String raw = ctx.STRING_LITERAL().getText();
        String expression = raw.substring(1, raw.length() - 1).trim();
        var timeUnits = ctx.timeUnit();
        String fromUnit = timeUnits.get(0).getText().toUpperCase();
        String toUnit = timeUnits.size() > 1 ? timeUnits.get(1).getText().toUpperCase() : null;
        if (toUnit != null) {
            return Literal.ofInterval(expression, fromUnit, toUnit);
        }
        return Literal.ofInterval(expression, fromUnit);
    }

    @Override
    public Literal visitBinaryHexLiteral(PangrovaSqlParser.BinaryHexLiteralContext ctx) {
        String raw = ctx.HEX_LITERAL().getText();
        String hexDigits = raw.substring(2, raw.length() - 1);
        return Literal.ofBinaryHex(hexDigits);
    }

    @Override
    public Literal visitTimestampWithTimeZoneLiteral(PangrovaSqlParser.TimestampWithTimeZoneLiteralContext ctx) {
        String raw = ctx.STRING_LITERAL().getText();
        return Literal.ofTimestampWithTimeZone(raw.substring(1, raw.length() - 1));
    }

    // ── EXTRACT / SUBSTRING ──

    @Override
    public Expression visitExtractExpr(PangrovaSqlParser.ExtractExprContext ctx) {
        String field = ctx.extractField().getText().toUpperCase();
        Expression source = (Expression) visit(ctx.expression());
        return new ExtractExpression(field, source);
    }

    @Override
    public Expression visitSubstringFromForExpr(PangrovaSqlParser.SubstringFromForExprContext ctx) {
        List<PangrovaSqlParser.ExpressionContext> exprs = ctx.expression();
        Expression source = (Expression) visit(exprs.get(0));
        Expression start = (Expression) visit(exprs.get(1));
        Expression length = (Expression) visit(exprs.get(2));
        return new SubstringExpression(source, start, length);
    }

    // ── Sort item (used by window ORDER BY) ──

    @Override
    public SortItem visitSortItem(PangrovaSqlParser.SortItemContext ctx) {
        Expression expr = (Expression) visit(ctx.expression());
        Optional<SortItem.SortDirection> dir = Optional.empty();
        if (ctx.ASC() != null) {
            dir = Optional.of(SortItem.SortDirection.ASC);
        } else if (ctx.DESC() != null) {
            dir = Optional.of(SortItem.SortDirection.DESC);
        }
        Optional<SortItem.NullOrdering> nullOrdering = Optional.empty();
        if (ctx.NULLS() != null) {
            if (ctx.FIRST_KW() != null) {
                nullOrdering = Optional.of(SortItem.NullOrdering.FIRST);
            } else if (ctx.LAST_KW() != null) {
                nullOrdering = Optional.of(SortItem.NullOrdering.LAST);
            }
        }
        return new SortItem(expr, dir, nullOrdering);
    }

    // ── Qualified name (delegated to parent for shared use) ──

    @Override
    public Identifier visitQualifiedName(PangrovaSqlParser.QualifiedNameContext ctx) {
        return parent.visitQualifiedName(ctx);
    }

    // ── Helper methods ──

    private List<CaseExpression.WhenClause> buildWhenClauses(
            List<PangrovaSqlParser.ExpressionContext> exprs, int startIdx) {
        List<CaseExpression.WhenClause> clauses = new java.util.ArrayList<>();
        int i = startIdx;
        while (i + 1 < exprs.size()) {
            Expression condition = (Expression) visit(exprs.get(i));
            Expression result = (Expression) visit(exprs.get(i + 1));
            clauses.add(new CaseExpression.WhenClause(condition, result));
            i += 2;
        }
        return clauses;
    }

    private Optional<Expression> extractCaseElse(
            boolean hasElse, List<PangrovaSqlParser.ExpressionContext> exprs) {
        if (!hasElse) return Optional.empty();
        return Optional.of((Expression) visit(exprs.get(exprs.size() - 1)));
    }

    private Expression buildBinaryExpr(
            PangrovaSqlParser.ExpressionContext left,
            PangrovaSqlParser.ExpressionContext right,
            Operator op) {
        return new BinaryExpression((Expression) visit(left), op, (Expression) visit(right));
    }

    private Operator tokenToOperator(int tokenType) {
        return switch (tokenType) {
            case PangrovaSqlParser.PLUS -> Operator.ADD;
            case PangrovaSqlParser.MINUS -> Operator.SUBTRACT;
            case PangrovaSqlParser.STAR -> Operator.MULTIPLY;
            case PangrovaSqlParser.SLASH -> Operator.DIVIDE;
            case PangrovaSqlParser.PERCENT -> Operator.MODULO;
            default -> throw new IllegalArgumentException("Unknown operator token: " + tokenType);
        };
    }

    private Operator comparisonOperator(PangrovaSqlParser.ComparisonOperatorContext ctx) {
        return switch (ctx.getStart().getType()) {
            case PangrovaSqlParser.EQ -> Operator.EQUAL;
            case PangrovaSqlParser.NEQ -> Operator.NOT_EQUAL;
            case PangrovaSqlParser.LT -> Operator.LESS_THAN;
            case PangrovaSqlParser.GT -> Operator.GREATER_THAN;
            case PangrovaSqlParser.LTE -> Operator.LESS_THAN_OR_EQUAL;
            case PangrovaSqlParser.GTE -> Operator.GREATER_THAN_OR_EQUAL;
            default -> throw new IllegalArgumentException("Unknown comparison operator: " + ctx.getText());
        };
    }
}
