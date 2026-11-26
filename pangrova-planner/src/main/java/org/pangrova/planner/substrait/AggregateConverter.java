package org.pangrova.planner.substrait;

import org.pangrova.planner.exception.SubstraitConversionException;
import org.pangrova.planner.extension.EnforceSingleRowRelDetail;
import org.pangrova.planner.exchange.StageAggregate;
import org.pangrova.protocol.v2.AggregatePhaseDetail;
import org.pangrova.protocol.v2.AggregatePhaseMeasure;

import com.google.protobuf.Any;
import io.substrait.expression.Expression;
import io.substrait.extension.ImmutableAdvancedExtension;
import io.substrait.proto.AggregationPhase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ExtensionSingle;
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
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
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
        if (agg.getAggCallList().stream().anyMatch(AggregateConverter::singleValue)) {
            return enforceSingleRow(agg, input);
        }
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

    Rel convert(StageAggregate aggregate, Rel input) {
        boolean producesState = aggregate.phase() == AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE;
        var phase = producesState ? Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE
                : Expression.AggregationPhase.INTERMEDIATE_TO_RESULT;
        var calls = aggregate.calls();
        var bindings = aggregate.bindings();
        var detail = AggregatePhaseDetail.newBuilder()
                .setImplementationBuildId(bindings.get(0).implementationBuildId());
        var measures = new ArrayList<Aggregate.Measure>(calls.size());
        var fields = aggregate.getRowType().getFieldList();
        for (int ordinal = 0; ordinal < calls.size(); ordinal++) {
            var call = calls.get(ordinal);
            var binding = bindings.get(ordinal);
            var ref = functionMapper.map(call.getAggregation());
            if (!binding.function().getName().equals(ref.name())
                    || !binding.implementationBuildId().equals(detail.getImplementationBuildId())) {
                throw new IllegalArgumentException("Stage aggregate does not match its bound function");
            }
            var measureDetail = AggregatePhaseMeasure.newBuilder()
                    .setRegistrationSha256(binding.actualRegistrationSha256());
            if (!producesState) {
                measureDetail.setOriginalRawArguments(binding.originalArguments());
            }
            detail.addMeasures(measureDetail);
            measures.add(buildMeasure(call, ref, aggregate.getInput().getRowType(),
                    producesState ? call.getArgList() : List.of(ordinal), producesState ? call.filterArg : -1,
                    phase, typeMapper.map(fields.get(ordinal).getType())));
        }
        var phaseDetail = detail.build();
        var enhancement = ImmutableAdvancedExtension
                .<PangrovaOptimization, PangrovaExtensionProtoConverter.Enhancement>builder()
                .enhancement(() -> Any.pack(phaseDetail)).build();
        return ImmutableAggregate.builder().input(input).measures(measures)
                .groupings(List.of(ImmutableGrouping.builder().build())).extension(enhancement).build();
    }

    private static boolean singleValue(AggregateCall call) {
        return call.getAggregation().getKind() == SqlKind.SINGLE_VALUE;
    }

    private static Rel enforceSingleRow(LogicalAggregate aggregate, Rel input) {
        if (aggregate.getGroupCount() != 0 || aggregate.getGroupSets().size() != 1
                || aggregate.getAggCallList().size() != 1) {
            throw new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Single-row enforcement requires one scalar subquery result");
        }
        var call = aggregate.getAggCallList().get(0);
        if (call.getArgList().size() != 1 || call.filterArg >= 0 || call.isDistinct() || call.isApproximate()
                || call.ignoreNulls() || call.distinctKeys != null || call.hasCollation() || !call.rexList.isEmpty()) {
            throw new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Single-row enforcement requires one unmodified input value");
        }
        int ordinal = call.getArgList().get(0);
        var fields = input.getRecordType().fields();
        if (ordinal < 0 || ordinal >= fields.size()) {
            throw new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Single-row value is outside the scalar subquery input");
        }
        var detail = new EnforceSingleRowRelDetail(ordinal);
        return ExtensionSingle.from(detail, input).build();
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
        // The decorrelator uses LITERAL_AGG for one constant per group.
        var ref = "LITERAL_AGG".equals(call.getAggregation().getName())
                ? new FunctionReference("aggregate", "min") : functionMapper.map(call.getAggregation());
        return buildMeasure(call, ref, inputRowType, call.getArgList(), call.filterArg,
                Expression.AggregationPhase.INITIAL_TO_RESULT, resolveAggregateOutputType(call, ref));
    }

    private Aggregate.Measure buildMeasure(AggregateCall call, FunctionReference ref, RelDataType inputRowType,
            List<Integer> argumentOrdinals, int filterOrdinal, Expression.AggregationPhase phase, Type outputType) {
        var variant = functionMapper.resolveAggregateVariant(ref);
        int fieldCount = inputRowType.getFieldCount();
        List<Expression> args = argumentOrdinals.stream()
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
                .aggregationPhase(phase)
                .invocation(call.isDistinct()
                        ? Expression.AggregationInvocation.DISTINCT
                        : Expression.AggregationInvocation.ALL)
                .arguments(args).build();
        var measureBuilder = ImmutableMeasure.builder().function(invocation);
        if (filterOrdinal >= 0) {
            if (filterOrdinal >= fieldCount) {
                throw new SubstraitConversionException(
                        PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                        "Aggregate FILTER arg index " + filterOrdinal + " out of bounds for "
                                + fieldCount + " input fields");
            }
            Type filterType = typeMapper.map(
                    inputRowType.getFieldList().get(filterOrdinal).getType());
            measureBuilder.preMeasureFilter(
                    FieldReference.newRootStructReference(filterOrdinal, filterType));
        }
        return measureBuilder.build();
    }

    private Type resolveAggregateOutputType(AggregateCall call, FunctionReference ref) {
        // The actual DECIMAL registration returns the original precision and
        // scale. Calcite can declare integral AVG as integral while Velox
        // returns FP64 for that registered signature.
        if ("avg".equals(ref.name()) && call.getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
            TypeCreator creator = TypeCreator.of(call.getType().isNullable());
            return creator.FP64;
        }
        return typeMapper.map(call.getType());
    }
}
