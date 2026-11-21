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
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
        if (agg.getGroupSets().size() > 1) {
            return convertMultiGroupSet(agg, input);
        }

        List<Aggregate.Grouping> groupings = buildGroupings(agg);
        List<Aggregate.Measure> measures = agg.getAggCallList().stream()
                .map(call -> buildMeasure(call, agg.getInput().getRowType()))
                .toList();
        var builder = ImmutableAggregate.builder()
                .input(input).groupings(groupings).measures(measures);

        return builder.build();
    }

    /**
     * Handles multi-grouping-set aggregates (ROLLUP/CUBE/GROUPING SETS).
     * Inserts a compact Project below the Aggregate so that field indices
     * are contiguous starting from 0. The native GroupIdNode expects compact
     * indices — without this normalization, field references from the original
     * scan (e.g. index 3 for o_totalprice in a 9-column table) exceed the
     * GroupIdNode output column count.
     */
    private Rel convertMultiGroupSet(LogicalAggregate agg, Rel input) {
        RelDataType inputRowType = agg.getInput().getRowType();
        CompactLayout layout = buildCompactLayout(agg, inputRowType);
        Rel aggInput = layout.needsProject()
                ? buildCompactingProject(input, inputRowType, layout.sortedIndices())
                : input;
        List<Aggregate.Grouping> groupings = buildCompactedGroupings(
                agg, inputRowType, layout.indexMapping());
        List<Aggregate.Measure> measures = agg.getAggCallList().stream()
                .map(call -> buildMeasureWithMapping(call, inputRowType, layout.indexMapping()))
                .toList();
        return ImmutableAggregate.builder()
                .input(aggInput).groupings(groupings).measures(measures)
                .remap(Rel.Remap.offset(0, agg.getRowType().getFieldCount()))
                .build();
    }

    /** Compact field layout derived from the indices referenced by a multi-grouping-set aggregate. */
    private record CompactLayout(List<Integer> sortedIndices,
                                 Map<Integer, Integer> indexMapping,
                                 boolean needsProject) {
    }

    private CompactLayout buildCompactLayout(LogicalAggregate agg, RelDataType inputRowType) {
        Set<Integer> referencedIndices = new TreeSet<>();
        for (var groupSet : agg.getGroupSets()) {
            referencedIndices.addAll(groupSet.asList());
        }
        for (AggregateCall call : agg.getAggCallList()) {
            referencedIndices.addAll(call.getArgList());
            if (call.filterArg >= 0) {
                referencedIndices.add(call.filterArg);
            }
        }
        List<Integer> sortedIndices = new ArrayList<>(referencedIndices);
        Map<Integer, Integer> indexMapping = new HashMap<>();
        for (int i = 0; i < sortedIndices.size(); i++) {
            indexMapping.put(sortedIndices.get(i), i);
        }
        boolean needsProject = sortedIndices.size() < inputRowType.getFieldCount();
        for (int i = 0; i < sortedIndices.size() && !needsProject; i++) {
            if (sortedIndices.get(i) != i) {
                needsProject = true;
            }
        }
        return new CompactLayout(sortedIndices, indexMapping, needsProject);
    }

    private Rel buildCompactingProject(Rel input, RelDataType inputRowType,
                                       List<Integer> sortedIndices) {
        List<Expression> projections = sortedIndices.stream()
                .map(idx -> {
                    Type type = typeMapper.map(inputRowType.getFieldList().get(idx).getType());
                    return (Expression) FieldReference.newRootStructReference(idx, type);
                })
                .toList();
        return ImmutableProject.builder().input(input).expressions(projections)
                .remap(Rel.Remap.offset(input.getRecordType().fields().size(), projections.size())).build();
    }

    private List<Aggregate.Grouping> buildCompactedGroupings(LogicalAggregate agg,
            RelDataType inputRowType, Map<Integer, Integer> indexMapping) {
        return agg.getGroupSets().stream()
                .map(groupSet -> {
                    List<Expression> groupExprs = groupSet.asList().stream()
                            .map(idx -> {
                                int compactIdx = indexMapping.get(idx);
                                Type type = typeMapper.map(
                                        inputRowType.getFieldList().get(idx).getType());
                                return (Expression) FieldReference.newRootStructReference(
                                        compactIdx, type);
                            })
                            .toList();
                    return (Aggregate.Grouping) ImmutableGrouping.builder()
                            .expressions(groupExprs).build();
                })
                .toList();
    }

    private Aggregate.Measure buildMeasureWithMapping(AggregateCall call,
            RelDataType inputRowType, Map<Integer, Integer> indexMapping) {
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
        List<Expression> args = call.getArgList().stream()
                .map(idx -> {
                    int compactIdx = indexMapping.get(idx);
                    Type t = typeMapper.map(inputRowType.getFieldList().get(idx).getType());
                    return (Expression) FieldReference.newRootStructReference(compactIdx, t);
                })
                .toList();
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
            int compactFilterIdx = indexMapping.get(call.filterArg);
            Type filterType = typeMapper.map(
                    inputRowType.getFieldList().get(call.filterArg).getType());
            measureBuilder.preMeasureFilter(
                    FieldReference.newRootStructReference(compactFilterIdx, filterType));
        }
        return measureBuilder.build();
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
