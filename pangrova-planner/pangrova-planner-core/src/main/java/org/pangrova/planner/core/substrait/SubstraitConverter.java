package org.pangrova.planner.core.substrait;

import org.pangrova.planner.core.metadata.PlanTable;

import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableExecutionBehavior;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import io.substrait.type.NamedFieldCountingTypeVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Converts a Calcite RelNode plan to a Substrait Plan using visitor pattern.
 */
public final class SubstraitConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SubstraitConverter.class);

    private final TypeMapper typeMapper;
    private final FunctionMapper functionMapper;

    public SubstraitConverter() {
        this(new TypeMapper(), new FunctionMapper());
    }

    public SubstraitConverter(TypeMapper typeMapper, FunctionMapper functionMapper) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
    }

    /** Convert a Calcite RelNode plan to a Substrait Plan. */
    public Plan convert(RelNode relNode) {
        return convertWithResult(relNode, List.of()).plan();
    }

    /**
     * Convert a Calcite RelNode plan to a Substrait Plan, using the provided
     * column names for Root.names when they match the output field count.
     *
     * <p>Calcite HEP rules (e.g. PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW) replace
     * user-defined SQL aliases with synthetic names ($1, w0$o0). Passing the
     * pre-optimization column names here ensures Root.names carry the original
     * SQL aliases through to the native execution engine.</p>
     *
     * @param relNode the optimized Calcite plan
     * @param preOptimizationColumnNames column names captured before optimization;
     *        empty list falls back to relNode's row type field names
     */
    public Plan convert(RelNode relNode, List<String> preOptimizationColumnNames) {
        return convertWithResult(relNode, preOptimizationColumnNames).plan();
    }

    /**
     * Convert a Calcite RelNode plan to a Substrait {@link ConversionResult}
     * containing both the plan and the non-equi cross-join details.
     *
     * <p>This method is thread-safe: all mutable state is local to the call,
     * eliminating the previous shared-mutable-field race condition
     * (F-SUBSTRAIT-03).</p>
     *
     * @param relNode the optimized Calcite plan
     * @param preOptimizationColumnNames column names captured before optimization;
     *        empty list falls back to relNode's row type field names
     * @return an immutable {@link ConversionResult} with plan and cross-join details
     */
    public ConversionResult convertWithResult(RelNode relNode, List<String> preOptimizationColumnNames) {
        RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
        PlanNodeConverter visitor = new PlanNodeConverter(typeMapper, functionMapper, rexBuilder);
        Rel rel = visitor.visit(relNode);
        List<org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint> retrievalTopKHints =
                visitor.getRetrievalTopKHints();
        List<org.pangrova.planner.api.invocation.AiInvocationPlanHint> aiInvocationHints =
                visitor.getAiInvocationHints();
        List<String> outputNames = resolveOutputNames(
                relNode, rel, preOptimizationColumnNames);
        LOG.debug("event=substrait_convert relNodeClass={} outputNames={}", relNode.getClass().getSimpleName(),
                outputNames);
        Plan.Root root = ImmutableRoot.builder()
                .input(rel)
                .names(outputNames)
                .build();
        Plan plan = ImmutablePlan.builder()
                .addRoots(root)
                .executionBehavior(ImmutableExecutionBehavior.builder()
                        .variableEvaluationMode(
                                Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
                        .build())
                .build();
        return new ConversionResult(plan, retrievalTopKHints, aiInvocationHints);
    }

    private static List<String> resolveOutputNames(
            RelNode relNode, Rel converted, List<String> preOptimizationColumnNames) {
        int emittedNameCount = NamedFieldCountingTypeVisitor.countNames(
                converted.getRecordType());
        if (relNode instanceof LogicalTableModify modify) {
            // TableModify's Calcite result is a single row-count column, while
            // its Substrait input is the relation consumed by the Native writer.
            List<String> targetNames = SubstraitFieldNames.flatten(
                    PlanTable.require(modify).getRowType(), List.of());
            if (targetNames.size() == emittedNameCount) {
                return targetNames;
            }
            List<String> inputNames = SubstraitFieldNames.flatten(
                    modify.getInput().getRowType(), List.of());
            if (inputNames.size() == emittedNameCount) {
                return inputNames;
            }
            throw new SubstraitConversionException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "DML output names do not match the emitted relation: target="
                            + targetNames.size() + " input=" + inputNames.size()
                            + " emitted=" + emittedNameCount);
        }
        List<String> names = SubstraitFieldNames.flatten(
                relNode.getRowType(), preOptimizationColumnNames);
        if (names.size() != emittedNameCount) {
            throw new SubstraitConversionException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Output names do not match the emitted relation: rel="
                            + relNode.getRelTypeName() + " calcite=" + names.size()
                            + " emitted=" + emittedNameCount);
        }
        return names;
    }
}
