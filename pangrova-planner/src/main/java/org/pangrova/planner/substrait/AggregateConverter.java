package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ImmutableAggregate;
import io.substrait.relation.ImmutableGrouping;
import io.substrait.relation.ImmutableMeasure;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Converts Calcite LogicalAggregate to Substrait Aggregate Rel.
 */
final class AggregateConverter {

    private final TypeMapper typeMapper;
    private final FunctionMapper functionMapper;

    AggregateConverter(TypeMapper typeMapper, FunctionMapper functionMapper) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
    }

    Rel convert(LogicalAggregate agg, Rel input) {
        List<Aggregate.Grouping> groupings = buildGroupings(agg);
        RelDataType inputRowType = agg.getInput().getRowType();
        List<Aggregate.Measure> measures = agg.getAggCallList().stream()
                .map(call -> buildMeasure(call, inputRowType))
                .toList();
        var builder = ImmutableAggregate.builder()
                .input(input).groupings(groupings).measures(measures);
        if (groupings.size() > 1) {
            // Hide the structural grouping set identifier from the SQL row.
            builder.remap(Rel.Remap.offset(0, agg.getRowType().getFieldCount()));
        }
        return builder.build();
    }

    /**
     * Builds one Grouping per grouping set. For simple GROUP BY (single set),
     * produces one Grouping. For GROUPING SETS / CUBE / ROLLUP, produces
     * multiple Groupings — one per set. Calcite expands CUBE/ROLLUP into
     * explicit grouping sets before reaching this converter.
     */
    private List<Aggregate.Grouping> buildGroupings(LogicalAggregate agg) {
        int fieldCount = agg.getInput().getRowType().getFieldCount();
        return agg.getGroupSets().stream()
                .map(groupSet -> {
                    List<Expression> groupExprs = groupSet.asList().stream()
                            .map(idx -> {
                                if (idx < 0 || idx >= fieldCount) {
                                    throw new SubstraitConversionException(
                                            PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                                            "Aggregate grouping index " + idx + " out of bounds for "
                                                    + fieldCount + " input fields");
                                }
                                Type type = typeMapper.map(
                                        agg.getInput().getRowType().getFieldList().get(idx).getType());
                                return (Expression) FieldReference.newRootStructReference(idx, type);
                            })
                            .toList();
                    return (Aggregate.Grouping) ImmutableGrouping.builder().expressions(groupExprs).build();
                })
                .toList();
    }

    private Aggregate.Measure buildMeasure(AggregateCall call, RelDataType inputRowType) {
        // SINGLE_VALUE: Calcite's scalar subquery enforcement (exactly one row).
        // LITERAL_AGG: Calcite's internal aggregate for decorrelation of NOT IN
        // subqueries — produces a constant value per group. Both have no Substrait
        // equivalent; min() is semantically safe (single-row or constant input).
        // WARNING: If Calcite decorrelation produces multi-row input for SINGLE_VALUE
        // (a Calcite bug), min() will silently return the wrong result instead of
        // raising an error. The native engine should assert single-row semantics
        // for SINGLE_VALUE-mapped aggregates.
        FunctionReference ref;
        String opName = call.getAggregation().getName();
        if (call.getAggregation().getKind() == SqlKind.SINGLE_VALUE
                || "LITERAL_AGG".equals(opName)) {
            ref = new FunctionReference("aggregate", "min");
        } else {
            ref = functionMapper.map(call.getAggregation());
        }
        var variant = functionMapper.resolveAggregateVariant(ref);
        Type outputType = resolveAggregateOutputType(call, ref);
        int fieldCount = inputRowType.getFieldCount();
        List<Expression> args = call.getArgList().stream()
                .map(idx -> {
                    if (idx < 0 || idx >= fieldCount) {
                        throw new SubstraitConversionException(
                                PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                                "Aggregate measure arg index " + idx + " out of bounds for "
                                        + fieldCount + " input fields (Calcite REDUCE_EXPRESSIONS bug)");
                    }
                    Type t = typeMapper.map(inputRowType.getFieldList().get(idx).getType());
                    return (Expression) FieldReference.newRootStructReference(idx, t);
                })
                .toList();
        // LITERAL_AGG has no arguments; supply a boolean TRUE constant so
        // the aggregate function (mapped to min) has a valid input.
        if (args.isEmpty()) {
            args = List.of(ExpressionCreator.bool(false, true));
        }
        var invocation = ImmutableAggregateFunctionInvocation.builder()
                .declaration(variant).outputType(outputType)
                .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                .invocation(call.isDistinct()
                        ? Expression.AggregationInvocation.DISTINCT
                        : Expression.AggregationInvocation.ALL)
                .arguments(args).build();
        var measureBuilder = ImmutableMeasure.builder().function(invocation);
        if (call.filterArg >= 0) {
            if (call.filterArg >= fieldCount) {
                throw new SubstraitConversionException(
                        PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                        "Aggregate FILTER arg index " + call.filterArg + " out of bounds for "
                                + fieldCount + " input fields");
            }
            Type filterType = typeMapper.map(
                    inputRowType.getFieldList().get(call.filterArg).getType());
            measureBuilder.preMeasureFilter(
                    FieldReference.newRootStructReference(call.filterArg, filterType));
        }
        return measureBuilder.build();
    }

    /**
     * The native engine's avg() always returns FP64 regardless of input type.
     * Calcite returns INTEGER for AVG(INTEGER), causing a type mismatch
     * at the native execution layer. Override to FP64 for avg.
     */
    private Type resolveAggregateOutputType(AggregateCall call, FunctionReference ref) {
        if ("avg".equals(ref.name())) {
            TypeCreator creator = TypeCreator.of(call.getType().isNullable());
            return creator.FP64;
        }
        return typeMapper.map(call.getType());
    }
}
