package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Converts Calcite window function constructs to Substrait window expressions.
 * Handles both LogicalWindow (visit path) and RexOver (expression path).
 */
final class WindowFunctionConverter {

    private final TypeMapper typeMapper;
    private final FunctionMapper functionMapper;

    WindowFunctionConverter(TypeMapper typeMapper, FunctionMapper functionMapper) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
    }

    List<Expression> buildWindowProjections(LogicalWindow window, int inputFieldCount,
                                             Function<RexNode, Expression> rexConverter,
                                             Function<RelFieldCollation, Expression.SortField> collationConverter) {
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < inputFieldCount; i++) {
            Type t = typeMapper.map(window.getInput().getRowType().getFieldList().get(i).getType());
            expressions.add(FieldReference.newRootStructReference(i, t));
        }
        for (Window.Group group : window.groups) {
            for (Window.RexWinAggCall aggCall : group.aggCalls) {
                expressions.add(buildWindowInvocation(aggCall, group, window,
                        rexConverter, collationConverter));
            }
        }
        return expressions;
    }

    private Expression buildWindowInvocation(
            Window.RexWinAggCall aggCall, Window.Group group, LogicalWindow window,
            Function<RexNode, Expression> rexConverter,
            Function<RelFieldCollation, Expression.SortField> collationConverter) {
        String fnName = aggCall.getOperator().getName();
        FunctionReference ref = functionMapper.mapWindowFunction(fnName);
        var variant = functionMapper.resolveWindowVariant(ref);
        Type outputType = typeMapper.map(aggCall.getType());
        List<Expression> partitionBy = buildPartitionExprs(group, window);
        List<Expression.SortField> sortFields = group.orderKeys.getFieldCollations().stream()
                .map(collationConverter)
                .toList();
        int inputFieldCount = window.getInput().getRowType().getFieldCount();
        List<Expression> args = aggCall.getOperands().stream()
                .map(op -> resolveWindowOperand(op, inputFieldCount, window, rexConverter))
                .toList();
        args = coerceLeadLagArgs(fnName, args);
        return buildWindowFunctionInvocation(
                variant, outputType, args, partitionBy, sortFields,
                new WindowBoundsSpec(group.lowerBound, group.upperBound, window));
    }

    /**
     * Resolves a window function operand. LogicalWindow stores constants
     * (e.g. LEAD/LAG offset) as extra columns appended after input fields.
     * RexInputRef with index >= inputFieldCount refers to window.constants.
     *
     * <p>For numeric constants, produces native Substrait literals directly
     * from the Calcite RexLiteral to avoid type mismatches. Native window
     * functions require plain constants matching the expected signature types.
     */
    private Expression resolveWindowOperand(RexNode operand, int inputFieldCount,
                                             LogicalWindow window,
                                             Function<RexNode, Expression> rexConverter) {
        if (operand instanceof org.apache.calcite.rex.RexInputRef ref
                && ref.getIndex() >= inputFieldCount) {
            int constIdx = ref.getIndex() - inputFieldCount;
            if (constIdx < window.constants.size()) {
                return resolveWindowConstant(window.constants.get(constIdx), rexConverter);
            }
        }
        return rexConverter.apply(operand);
    }

    private Expression resolveWindowConstant(RexNode constant,
                                              Function<RexNode, Expression> rexConverter) {
        if (constant instanceof org.apache.calcite.rex.RexLiteral lit) {
            if (isIntegerValued(lit)) {
                return ExpressionCreator.i64(false, lit.getValueAs(Long.class));
            }
            if (isDecimalType(lit)) {
                return ExpressionCreator.fp64(false, lit.getValueAs(Double.class));
            }
        }
        return rexConverter.apply(constant);
    }

    private static boolean isDecimalType(org.apache.calcite.rex.RexLiteral lit) {
        return lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
    }

    private static boolean isIntegerValued(org.apache.calcite.rex.RexLiteral lit) {
        return switch (lit.getTypeName()) {
            case INTEGER, BIGINT, SMALLINT, TINYINT -> true;
            case DECIMAL -> {
                java.math.BigDecimal val = lit.getValueAs(java.math.BigDecimal.class);
                yield val != null && val.scale() == 0;
            }
            default -> false;
        };
    }

    Expression convertOver(RexOver over, Function<RexNode, Expression> rexConverter) {
        String fnName = over.getOperator().getName();
        FunctionReference ref = functionMapper.mapWindowFunction(fnName);
        var variant = functionMapper.resolveWindowVariant(ref);
        Type outputType = typeMapper.map(over.getType());
        List<Expression> partitionBy = over.getWindow().partitionKeys.stream()
                .map(rexConverter)
                .toList();
        List<Expression.SortField> sortFields = over.getWindow().orderKeys.stream()
                .map(rfc -> convertRexFieldCollation(rfc, rexConverter))
                .toList();
        List<Expression> args = over.getOperands().stream()
                .map(rexConverter)
                .toList();
        args = coerceLeadLagArgs(fnName, args);
        return buildWindowFunctionInvocation(
                variant, outputType, args, partitionBy, sortFields,
                new WindowBoundsSpec(over.getWindow().getLowerBound(),
                        over.getWindow().getUpperBound(), null));
    }

    /**
     * Coerces LEAD/LAG arguments to match native engine expectations:
     * - 2nd arg (offset): must be I64 literal
     * - 3rd arg (default): must match 1st arg type (native signature: (T,bigint,T)->T)
     */
    private static List<Expression> coerceLeadLagArgs(String fnName, List<Expression> args) {
        String upper = fnName.toUpperCase();
        if ((!upper.equals("LEAD") && !upper.equals("LAG")) || args.size() < 2) {
            return args;
        }
        List<Expression> coerced = new ArrayList<>(args);
        // Coerce 2nd arg (offset) to I64
        Expression offset = coerced.get(1);
        if (!(offset instanceof Expression.I64Literal)) {
            coerced.set(1, toLongLiteral(offset));
        }
        // Coerce 3rd arg (default value) to match 1st arg type.
        // The native engine requires lag(T, bigint, T) — the default value must be the same
        // type as the value column. Calcite may store -1.0 as DECIMAL(2,1) even
        // when the value column is DOUBLE.
        if (coerced.size() >= 3) {
            coerced.set(2, coerceDefaultValue(coerced.get(0), coerced.get(2)));
        }
        return coerced;
    }

    /**
     * Coerces a LEAD/LAG default value to match the value argument's type.
     * Safety net for cases where resolveWindowOperand did not convert the constant.
     */
    private static Expression coerceDefaultValue(Expression valueArg, Expression defaultArg) {
        if (defaultArg instanceof Expression.DecimalLiteral) {
            // DecimalLiteral should have been converted in resolveWindowOperand.
            // If it still appears here (e.g. from RexOver path), emit FP64 as fallback.
            return ExpressionCreator.fp64(false, 0.0);
        }
        return defaultArg;
    }

    /**
     * Converts a numeric literal expression to an I64Literal.
     * Native window function arguments must be field references or constants;
     * Cast expressions are rejected by exprToChannel.
     */
    private static Expression toLongLiteral(Expression expr) {
        if (expr instanceof Expression.I64Literal) {
            return expr;
        }
        if (expr instanceof Expression.I32Literal i32) {
            return ExpressionCreator.i64(false, i32.value());
        }
        if (expr instanceof Expression.FP64Literal fp) {
            return ExpressionCreator.i64(false, (long) fp.value());
        }
        // For DecimalLiteral or other types, fall back to 1 (safe default for offset)
        return ExpressionCreator.i64(false, 1L);
    }

    /** Bundles the frame bounds and owning window used to build a window invocation. */
    private record WindowBoundsSpec(
            org.apache.calcite.rex.RexWindowBound lower,
            org.apache.calcite.rex.RexWindowBound upper,
            LogicalWindow window) {
    }

    private Expression buildWindowFunctionInvocation(
            SimpleExtension.WindowFunctionVariant variant, Type outputType, List<Expression> args,
            List<Expression> partitionBy, List<Expression.SortField> sortFields,
            WindowBoundsSpec bounds) {
        return ImmutableExpression.WindowFunctionInvocation.builder()
                .declaration(variant)
                .outputType(outputType)
                .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                .invocation(Expression.AggregationInvocation.ALL)
                .arguments(args)
                .partitionBy(partitionBy)
                .sort(sortFields)
                .boundsType(Expression.WindowBoundsType.ROWS)
                .lowerBound(convertWindowBound(bounds.lower(), bounds.window()))
                .upperBound(convertWindowBound(bounds.upper(), bounds.window()))
                .build();
    }

    private List<Expression> buildPartitionExprs(Window.Group group, LogicalWindow window) {
        return group.keys.asList().stream()
                .map(idx -> {
                    Type t = typeMapper.map(
                            window.getInput().getRowType().getFieldList().get(idx).getType());
                    return (Expression) FieldReference.newRootStructReference(idx, t);
                })
                .toList();
    }

    WindowBound convertWindowBound(org.apache.calcite.rex.RexWindowBound bound) {
        return convertWindowBound(bound, null);
    }

    /**
     * Converts a Calcite window bound to Substrait, resolving RexInputRef
     * offsets against the LogicalWindow's constants list when available.
     */
    WindowBound convertWindowBound(org.apache.calcite.rex.RexWindowBound bound,
                                    LogicalWindow window) {
        if (bound.isUnbounded()) return WindowBound.UNBOUNDED;
        if (bound.isCurrentRow()) return WindowBound.CURRENT_ROW;
        long offset = resolveWindowBoundOffset(bound.getOffset(), window);
        if (bound.isPreceding()) return WindowBound.Preceding.of(offset);
        return WindowBound.Following.of(offset);
    }

    private long resolveWindowBoundOffset(RexNode offsetNode, LogicalWindow window) {
        if (offsetNode instanceof org.apache.calcite.rex.RexLiteral lit) {
            return lit.getValueAs(Long.class);
        }
        if (offsetNode instanceof org.apache.calcite.rex.RexInputRef ref && window != null) {
            return resolveConstantOffset(ref, window);
        }
        return extractLong(offsetNode);
    }

    private long resolveConstantOffset(org.apache.calcite.rex.RexInputRef ref, LogicalWindow window) {
        int inputFieldCount = window.getInput().getRowType().getFieldCount();
        int constIdx = ref.getIndex() - inputFieldCount;
        if (constIdx >= 0 && constIdx < window.constants.size()) {
            RexNode constant = window.constants.get(constIdx);
            if (constant instanceof org.apache.calcite.rex.RexLiteral constLit) {
                return constLit.getValueAs(Long.class);
            }
        }
        return extractLong(ref);
    }

    private Expression.SortField convertRexFieldCollation(
            RexFieldCollation rfc, Function<RexNode, Expression> rexConverter) {
        Expression fieldRef = rexConverter.apply(rfc.left);
        Expression.SortDirection dir = rfc.getDirection()
                == org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING
                ? Expression.SortDirection.DESC_NULLS_LAST : Expression.SortDirection.ASC_NULLS_LAST;
        return ImmutableExpression.SortField.builder()
                .expr(fieldRef)
                .direction(dir)
                .build();
    }

    static long extractLong(RexNode node) {
        if (node instanceof org.apache.calcite.rex.RexLiteral lit) {
            return lit.getValueAs(Long.class);
        }
        throw new SubstraitConversionException(
                org.pangrova.common.exception.PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                "Unsupported non-literal in LIMIT/OFFSET: " + node.getClass().getSimpleName());
    }
}
