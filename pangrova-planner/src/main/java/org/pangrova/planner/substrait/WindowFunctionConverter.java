package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Converts Calcite window function constructs to Substrait window expressions.
 * Handles both LogicalWindow (visit path) and RexOver (expression path).
 */
final class WindowFunctionConverter {

    private final TypeMapper typeMapper;
    private final FunctionMapper functionMapper;
    private final LiteralConverter literalConverter;
    private final RexBuilder rexBuilder;

    WindowFunctionConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                            LiteralConverter literalConverter, RexBuilder rexBuilder) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
        this.literalConverter = literalConverter;
        this.rexBuilder = rexBuilder;
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
        List<RexNode> operands = aggCall.getOperands().stream()
                .map(op -> resolveWindowOperand(op, window))
                .toList();
        List<Expression> args = convertArguments(fnName, operands, rexConverter);
        return buildWindowFunctionInvocation(
                ref, variant, outputType, args, partitionBy, sortFields,
                new WindowBoundsSpec(group.lowerBound, group.upperBound, window, group.isRows,
                        aggCall.distinct, aggCall.ignoreNulls));
    }

    /**
     * Resolves a window function operand. LogicalWindow stores constants
     * (e.g. LEAD/LAG offset) as extra columns appended after input fields.
     * RexInputRef with index >= inputFieldCount refers to window.constants.
     */
    private RexNode resolveWindowOperand(RexNode operand, LogicalWindow window) {
        int inputFieldCount = window.getInput().getRowType().getFieldCount();
        if (operand instanceof RexInputRef ref && ref.getIndex() >= inputFieldCount) {
            int constIdx = ref.getIndex() - inputFieldCount;
            if (constIdx < window.constants.size()) {
                return window.constants.get(constIdx);
            }
            throw invalidConstant();
        }
        return operand;
    }

    private List<Expression> convertArguments(String functionName, List<RexNode> operands,
                                               Function<RexNode, Expression> rexConverter) {
        String name = functionName.toUpperCase(Locale.ROOT);
        if ("NTILE".equals(name)) {
            return convertNtileArguments(operands, rexConverter);
        }
        if (!("LEAD".equals(name) || "LAG".equals(name))) {
            return operands.stream().map(rexConverter).toList();
        }
        List<Expression> arguments = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (i == 1) {
                arguments.add(convertIntegralArgument(operand, rexConverter));
            } else if (i == 2) {
                RelDataType targetType = rexBuilder.getTypeFactory().createTypeWithNullability(
                        operands.get(0).getType(), operand.getType().isNullable());
                arguments.add(convertDefaultValue(operand, targetType, rexConverter));
            } else {
                arguments.add(rexConverter.apply(operand));
            }
        }
        return List.copyOf(arguments);
    }

    private List<Expression> convertNtileArguments(List<RexNode> operands,
                                                  Function<RexNode, Expression> rexConverter) {
        return operands.stream().map(operand -> {
            if (operand instanceof RexLiteral literal && LiteralConverter.exactLong(literal) <= 0) {
                throw new SubstraitConversionException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "NTILE bucket count must be positive");
            }
            return convertIntegralArgument(operand, rexConverter);
        }).toList();
    }

    private Expression convertIntegralArgument(RexNode operand,
                                                Function<RexNode, Expression> rexConverter) {
        if (operand instanceof RexLiteral literal) {
            LiteralConverter.exactLong(literal);
            RelDataType bigint = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
            return literalConverter.convert(literal, bigint);
        }
        if (operand.getType().getSqlTypeName() != SqlTypeName.BIGINT) {
            throw new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                    "Window integral argument requires a materialized BIGINT expression");
        }
        return rexConverter.apply(operand);
    }

    private Expression convertDefaultValue(RexNode operand, RelDataType targetType,
                                             Function<RexNode, Expression> rexConverter) {
        if (operand instanceof RexLiteral literal) {
            return literalConverter.convert(literal, targetType);
        }
        RexNode typedOperand = operand.getType().equals(targetType)
                ? operand : rexBuilder.makeCast(targetType, operand);
        return rexConverter.apply(typedOperand);
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
        List<Expression> args = convertArguments(fnName, over.getOperands(), rexConverter);
        return buildWindowFunctionInvocation(
                ref, variant, outputType, args, partitionBy, sortFields,
                new WindowBoundsSpec(over.getWindow().getLowerBound(),
                        over.getWindow().getUpperBound(), null, over.getWindow().isRows(),
                        over.isDistinct(), over.ignoreNulls()));
    }

    /** Bundles the frame bounds and owning window used to build a window invocation. */
    private record WindowBoundsSpec(
            org.apache.calcite.rex.RexWindowBound lower,
            org.apache.calcite.rex.RexWindowBound upper,
            LogicalWindow window, boolean rows, boolean distinct, boolean ignoreNulls) {
    }

    private Expression buildWindowFunctionInvocation(
            FunctionReference reference, SimpleExtension.WindowFunctionVariant variant,
            Type outputType, List<Expression> args,
            List<Expression> partitionBy, List<Expression.SortField> sortFields,
            WindowBoundsSpec bounds) {
        return ImmutableExpression.WindowFunctionInvocation.builder()
                .declaration(variant)
                .outputType(outputType)
                .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                .invocation(bounds.distinct() ? Expression.AggregationInvocation.DISTINCT
                        : Expression.AggregationInvocation.ALL)
                .options(windowOptions(reference, bounds.ignoreNulls()))
                .arguments(args)
                .partitionBy(partitionBy)
                .sort(sortFields)
                .boundsType(bounds.rows() ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE)
                .lowerBound(convertWindowBound(bounds.lower(), bounds.window()))
                .upperBound(convertWindowBound(bounds.upper(), bounds.window()))
                .build();
    }

    private List<FunctionOption> windowOptions(FunctionReference reference, boolean ignoreNulls) {
        if (!ignoreNulls) return List.of();
        try {
            var option = FunctionBindingDeclarations.option("WINDOW", reference.namespace(), reference.name(),
                    "NULL_TREATMENT", true);
            return List.of(FunctionOption.builder().name(option.getName()).values(option.getPreferenceList()).build());
        } catch (IllegalArgumentException invalid) {
            throw new SubstraitConversionException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Window function does not support IGNORE NULLS: " + reference.name(), invalid);
        }
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
        var offsetExpression = bound.getOffset();
        if (offsetExpression == null) throw invalidConstant();
        long offset = resolveWindowBoundOffset(offsetExpression, window);
        if (offset < 0) {
            throw new SubstraitConversionException(
                    org.pangrova.common.enums.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Window bound offset must be non-negative");
        }
        if (offset == 0) return WindowBound.CURRENT_ROW;
        if (bound.isPreceding()) return WindowBound.Preceding.of(offset);
        return WindowBound.Following.of(offset);
    }

    private static SubstraitConversionException invalidConstant() {
        return new SubstraitConversionException(
                org.pangrova.common.enums.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "Window constant or bounded offset lacks a numeric value");
    }

    private long resolveWindowBoundOffset(RexNode offsetNode, LogicalWindow window) {
        if (offsetNode instanceof org.apache.calcite.rex.RexLiteral) {
            return extractLong(offsetNode);
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
            if (constant instanceof org.apache.calcite.rex.RexLiteral) {
                return extractLong(constant);
            }
        }
        return extractLong(ref);
    }

    private Expression.SortField convertRexFieldCollation(
            RexFieldCollation rfc, Function<RexNode, Expression> rexConverter) {
        Expression fieldRef = rexConverter.apply(rfc.left);
        Expression.SortDirection dir = SortDirectionMapper.map(rfc.getDirection(), rfc.getNullDirection());
        return ImmutableExpression.SortField.builder()
                .expr(fieldRef)
                .direction(dir)
                .build();
    }

    static long extractLong(RexNode node) {
        return LiteralConverter.exactLong(node);
    }
}
