package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.ImmutableJoin;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.Join;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Converts LogicalCorrelate (lateral/correlated subquery) nodes to Substrait,
 * including UNNEST extension encoding and manual scalar-subquery decorrelation.
 */
final class CorrelateConverter {

    private static final Logger LOG = LoggerFactory.getLogger(CorrelateConverter.class);

    private final TypeMapper typeMapper;
    private final FunctionMapper functionMapper;
    private final ExpressionConverter rexConverter;
    private final Function<RelNode, Rel> relVisitor;
    private final UnnestConverter unnestConverter;

    CorrelateConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                       ExpressionConverter rexConverter, Function<RelNode, Rel> relVisitor,
                       UnnestConverter unnestConverter) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
        this.rexConverter = rexConverter;
        this.relVisitor = relVisitor;
        this.unnestConverter = unnestConverter;
    }

    /**
     * Converts LogicalCorrelate (lateral/correlated subquery) to a Substrait Join.
     *
     * <p>Calcite decorrelation (FILTER_SUB_QUERY_TO_CORRELATE etc.) rewrites
     * correlated subqueries into LogicalCorrelate nodes where the correlation
     * variable is embedded in the right-side plan as RexCorrelVariable references.
     * Native joins have no per-row correlation binding, so supported forms must
     * be decorrelated into an explicit join condition before serialization.</p>
     */
    Rel convert(LogicalCorrelate correlate) {
        // Detect UNNEST pattern: LogicalCorrelate with Uncollect on the right side.
        // This pattern comes from LATERAL TABLE(EXPLODE(arr)) or CROSS JOIN UNNEST(arr).
        // Convert to ExtensionSingle with unnest metadata instead of JoinRel,
        // so the native engine can build a native UnnestNode (single-input operator)
        // rather than a NestedLoopJoin (which doesn't support correlation).
        RelNode rightNode = correlate.getRight();
        if (rightNode instanceof Uncollect uncollect) {
            return unnestConverter.convert(correlate, uncollect, null);
        }
        // Preserve the actual right projection; aliases, reordering and computed
        // expressions are part of the lateral relation, not a disposable wrapper.
        if (rightNode instanceof LogicalProject proj
                && proj.getInput() instanceof Uncollect uncollect) {
            return unnestConverter.convert(correlate, uncollect, proj);
        }

        // Try manual decorrelation for scalar subquery pattern:
        // Correlate → Aggregate(group=[]) → Filter(correlation_equality) → input.
        // Calcite's RelDecorrelator fails for LEFT correlate + scalar aggregate;
        // we manually transform it into a standard JoinRel with explicit condition.
        Rel decorrelated = tryDecorrelateScalarSubquery(correlate);
        if (decorrelated != null) {
            return decorrelated;
        }
        decorrelated = tryDecorrelateExistenceSubquery(correlate);
        if (decorrelated != null) {
            return decorrelated;
        }

        if (!correlate.getRequiredColumns().isEmpty()) {
            throw new IllegalArgumentException("Unsupported correlated subquery: "
                    + correlate.getCorrelationId());
        }
        // A non-correlated lateral relation can be converted as an ordinary join.
        Rel left = relVisitor.apply(correlate.getLeft());
        Rel right = relVisitor.apply(correlate.getRight());
        Join.JoinType joinType = PlanNodeConverter.mapJoinType(correlate.getJoinType());
        LOG.debug("event=convert_correlate correlationId={} requiredColumns={} joinType={}",
                correlate.getCorrelationId(), correlate.getRequiredColumns(), correlate.getJoinType());
        return ImmutableJoin.builder()
                .left(left)
                .right(right)
                .joinType(joinType)
                .build();
    }

    private Rel tryDecorrelateExistenceSubquery(LogicalCorrelate correlate) {
        boolean explicitSemi = correlate.getJoinType() == JoinRelType.SEMI
                || correlate.getJoinType() == JoinRelType.ANTI;
        int rightOutputFields = correlate.getRight().getRowType().getFieldCount();
        boolean projectedExistence = correlate.getJoinType() == JoinRelType.INNER
                && (rightOutputFields == 0 || rightOutputFields == 1
                        && correlate.getRight().getRowType().getFieldList().get(0)
                                .getType().getSqlTypeName() == SqlTypeName.BOOLEAN);
        boolean projectedAbsence = correlate.getJoinType() == JoinRelType.LEFT
                && rightOutputFields == 1
                && correlate.getRight().getRowType().getFieldList().get(0)
                        .getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
        if (!explicitSemi && !projectedExistence && !projectedAbsence) {
            return null;
        }
        RelNode right = correlate.getRight();
        // SELECT-list expressions do not affect EXISTS/NOT EXISTS membership.
        while (right instanceof LogicalProject project) {
            right = project.getInput();
        }
        if (projectedExistence || projectedAbsence) {
            if (!(right instanceof LogicalAggregate aggregate)
                    || !aggregate.getAggCallList().isEmpty()
                    || aggregate.getGroupCount() != 1
                    || !(aggregate.getInput() instanceof LogicalProject keys)
                    || !(keys.getProjects().get(aggregate.getGroupSet().nth(0))
                            instanceof RexLiteral)) {
                return null;
            }
            right = keys.getInput();
        }
        if (!(right instanceof LogicalFilter filter)) {
            return null;
        }
        List<int[]> equalities = extractCorrelationEqualities(
                filter.getCondition(), correlate.getCorrelationId());
        if (equalities.isEmpty()) {
            return null;
        }
        RexNode remaining = removeCorrelationFromCondition(
                filter.getCondition(), correlate.getCorrelationId());
        if (remaining != null && RexUtil.containsCorrelation(remaining)) {
            throw new IllegalArgumentException("Unsupported residual correlated EXISTS predicate");
        }
        RelNode rightInput = remaining == null ? filter.getInput()
                : LogicalFilter.create(filter.getInput(), remaining);
        if (projectedAbsence) {
            return decorrelateNullableExistenceMarker(correlate, rightInput, equalities);
        }
        int rightFields = rightInput.getRowType().getFieldCount();
        Expression condition = buildCorrelationJoinCondition(equalities,
                correlate.getLeft().getRowType().getFieldCount(),
                ImmutableBitSet.range(rightFields),
                correlate.getLeft().getRowType(), rightInput.getRowType());
        Rel joined = ImmutableJoin.builder()
                .left(relVisitor.apply(correlate.getLeft()))
                .right(relVisitor.apply(rightInput))
                .joinType(projectedExistence ? Join.JoinType.SEMI
                        : PlanNodeConverter.mapJoinType(correlate.getJoinType()))
                .condition(condition)
                .build();
        if (!projectedExistence || rightOutputFields == 0) {
            return joined;
        }
        List<Expression> output = new ArrayList<>();
        for (int i = 0; i < joined.getRecordType().fields().size(); i++) {
            output.add(FieldReference.newRootStructReference(i,
                    joined.getRecordType().fields().get(i)));
        }
        output.add(ExpressionCreator.bool(false, true));
        return ImmutableProject.builder()
                .input(joined)
                .expressions(output)
                .remap(Rel.Remap.offset(joined.getRecordType().fields().size(), output.size()))
                .build();
    }

    private Rel decorrelateNullableExistenceMarker(LogicalCorrelate correlate,
                                                   RelNode rightInput,
                                                   List<int[]> equalities) {
        // Calcite represents NOT EXISTS as LEFT correlate to a grouped TRUE
        // marker followed by IS NULL(marker). Deduplicate matching right keys,
        // then leave the marker nullable across the LEFT join so the outer
        // filter distinguishes absence from presence without multiplying rows.
        ImmutableBitSet groupSet = buildGroupSetFromEqualities(equalities);
        LogicalAggregate grouped = LogicalAggregate.create(
                rightInput, List.of(), groupSet, null, List.of());
        Rel groupedRel = relVisitor.apply(grouped);
        List<Expression> markedFields = new ArrayList<>();
        for (int i = 0; i < groupedRel.getRecordType().fields().size(); i++) {
            markedFields.add(FieldReference.newRootStructReference(i,
                    groupedRel.getRecordType().fields().get(i)));
        }
        markedFields.add(ExpressionCreator.bool(false, true));
        Rel markedRight = ImmutableProject.builder()
                .input(groupedRel)
                .expressions(markedFields)
                .remap(Rel.Remap.offset(groupedRel.getRecordType().fields().size(),
                        markedFields.size()))
                .build();

        int leftFieldCount = correlate.getLeft().getRowType().getFieldCount();
        Expression condition = buildCorrelationJoinCondition(equalities,
                leftFieldCount, groupSet, correlate.getLeft().getRowType(), grouped.getRowType());
        Rel joined = ImmutableJoin.builder()
                .left(relVisitor.apply(correlate.getLeft()))
                .right(markedRight)
                .joinType(Join.JoinType.LEFT)
                .condition(condition)
                .build();
        List<Expression> output = new ArrayList<>();
        for (int i = 0; i < leftFieldCount; i++) {
            output.add(FieldReference.newRootStructReference(i,
                    joined.getRecordType().fields().get(i)));
        }
        int markerIndex = leftFieldCount + grouped.getRowType().getFieldCount();
        output.add(FieldReference.newRootStructReference(markerIndex,
                joined.getRecordType().fields().get(markerIndex)));
        return ImmutableProject.builder()
                .input(joined)
                .expressions(output)
                .remap(Rel.Remap.offset(joined.getRecordType().fields().size(), output.size()))
                .build();
    }


    /**
     * Manually decorrelates scalar subquery pattern:
     * Correlate → Aggregate(group=[]) → Filter(corr_eq) → input.
     *
     * <p>Transforms to: Project → Join(condition) → [left, Aggregate(group=[corr_keys])].
     * This handles cases where Calcite's RelDecorrelator fails (LEFT correlate with
     * scalar aggregate), producing a valid JoinRel with explicit equi-join condition
     * that the native engine can execute correctly.</p>
     */
    private Rel tryDecorrelateScalarSubquery(LogicalCorrelate correlate) {
        RelNode rightNode = correlate.getRight();
        var corId = correlate.getCorrelationId();
        int leftFieldCount = correlate.getLeft().getRowType().getFieldCount();

        // Scalar expressions such as AVG can produce several projects above the
        // aggregate. Keep them so the grouped aggregate can be reconstructed.
        List<LogicalProject> projects = new ArrayList<>();
        while (rightNode instanceof LogicalProject proj) {
            projects.add(proj);
            rightNode = proj.getInput();
        }

        // Match pattern: right = Aggregate(group=[]) with Filter below it
        if (!(rightNode instanceof LogicalAggregate agg) || agg.getGroupCount() != 0) {
            return null;
        }
        if (!(agg.getInput() instanceof LogicalFilter filter)) {
            return null;
        }

        // Extract correlation equalities: [$cor0.field[i] = inputRef[j]]
        List<int[]> equalities = extractCorrelationEqualities(filter.getCondition(), corId);
        if (equalities.isEmpty()) {
            return null;
        }

        ImmutableBitSet newGroupSet = buildGroupSetFromEqualities(equalities);
        LogicalAggregate newAgg = rebuildAggregateWithoutCorrelation(
                agg, filter, newGroupSet, corId);

        Rel left = relVisitor.apply(correlate.getLeft());
        Rel right = relVisitor.apply(newAgg);
        int groupKeyCount = newGroupSet.cardinality();
        Expression joinCondition = buildCorrelationJoinCondition(
                equalities, leftFieldCount, newGroupSet,
                correlate.getLeft().getRowType(), newAgg.getRowType());

        // For scalar subquery decorrelation, ALWAYS use LEFT join.
        // The original Aggregate(group=[]) guarantees exactly one output row per
        // correlated input (e.g., COUNT(*) returns 0 for empty input). After
        // decorrelation with GROUP BY, inputs without matching rows produce NO row
        // in the aggregate output. LEFT join preserves all left rows (with NULL
        // for the aggregate value when no match exists).
        Rel joinRel = ImmutableJoin.builder()
                .left(left)
                .right(right)
                .joinType(Join.JoinType.LEFT)
                .condition(joinCondition)
                .build();

        // Evaluate scalar projections after the LEFT join. A missing grouped
        // row makes COUNT null at this boundary; restore its empty-input zero
        // before evaluating expressions such as COUNT(*) + 1.
        Rel projected = restoreCountDefaults(joinRel, newAgg, leftFieldCount, groupKeyCount);
        for (int p = projects.size() - 1; p >= 0; p--) {
            projected = restoreScalarProject(projects.get(p), projected,
                    leftFieldCount + groupKeyCount);
        }

        List<Expression> projections = buildDecorrelationProjections(
                correlate, leftFieldCount, groupKeyCount);

        LOG.debug("event=manual_decorrelation correlationId={} groupKeys={} projects={} joinType={}",
                corId, newGroupSet, projects.size(), correlate.getJoinType());

        return ImmutableProject.builder()
                .input(projected)
                .expressions(projections)
                .remap(Rel.Remap.offset(projected.getRecordType().fields().size(), projections.size()))
                .build();
    }

    private Rel restoreCountDefaults(Rel joined, LogicalAggregate aggregate,
                                     int leftFieldCount, int groupKeyCount) {
        int prefixCount = leftFieldCount + groupKeyCount;
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < prefixCount; i++) {
            expressions.add(FieldReference.newRootStructReference(i,
                    joined.getRecordType().fields().get(i)));
        }
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            int fieldIndex = prefixCount + i;
            Expression value = FieldReference.newRootStructReference(fieldIndex,
                    joined.getRecordType().fields().get(fieldIndex));
            if (aggregate.getAggCallList().get(i).getAggregation().getKind() == SqlKind.COUNT) {
                Type type = typeMapper.map(aggregate.getRowType().getFieldList()
                        .get(groupKeyCount + i).getType());
                value = ImmutableExpression.ScalarFunctionInvocation.builder()
                        .declaration(functionMapper.resolveScalarVariant(
                                new FunctionReference("boolean", "coalesce")))
                        .outputType(type)
                        .arguments(List.of(value, ExpressionCreator.i64(false, 0)))
                        .build();
            }
            expressions.add(value);
        }
        return ImmutableProject.builder()
                .input(joined)
                .expressions(expressions)
                .remap(Rel.Remap.offset(joined.getRecordType().fields().size(), expressions.size()))
                .build();
    }

    private Rel restoreScalarProject(LogicalProject project, Rel input, int prefixCount) {
        List<Expression> expressions = new ArrayList<>();
        for (int i = 0; i < prefixCount; i++) {
            expressions.add(FieldReference.newRootStructReference(i,
                    input.getRecordType().fields().get(i)));
        }
        RexShuttle shift = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return project.getCluster().getRexBuilder().makeInputRef(
                        ref.getType(), ref.getIndex() + prefixCount);
            }
        };
        for (RexNode expression : project.getProjects()) {
            expressions.add(rexConverter.convert(expression.accept(shift)));
        }
        return ImmutableProject.builder()
                .input(input)
                .expressions(expressions)
                .remap(Rel.Remap.offset(input.getRecordType().fields().size(), expressions.size()))
                .build();
    }

    private static ImmutableBitSet buildGroupSetFromEqualities(List<int[]> equalities) {
        // Build new group-by set from the right-side fields in the equalities
        ImmutableBitSet.Builder groupBuilder = ImmutableBitSet.builder();
        for (int[] eq : equalities) {
            groupBuilder.set(eq[1]); // right field index
        }
        return groupBuilder.build();
    }

    /** Builds the modified right side: Aggregate with group keys, without correlation filter. */
    private static LogicalAggregate rebuildAggregateWithoutCorrelation(
            LogicalAggregate agg, LogicalFilter filter,
            ImmutableBitSet newGroupSet, CorrelationId corId) {
        RelNode newAggInput = filter.getInput();
        RexNode remainingCond = removeCorrelationFromCondition(filter.getCondition(), corId);
        if (remainingCond != null) {
            newAggInput = LogicalFilter.create(newAggInput, remainingCond);
        }
        return LogicalAggregate.create(
                newAggInput, agg.getHints(), newGroupSet, null, agg.getAggCallList());
    }

    /**
     * Projects the decorrelated join output back to the original schema.
     * Original = left_fields + agg_results (no group keys);
     * current  = left_fields + group_keys + agg_results, so the group keys
     * on the right side are projected out.
     */
    private List<Expression> buildDecorrelationProjections(
            LogicalCorrelate correlate,
            int leftFieldCount, int groupKeyCount) {
        int rightFieldCount = correlate.getRight().getRowType().getFieldCount();
        List<Expression> projections = new ArrayList<>();
        for (int i = 0; i < leftFieldCount; i++) {
            Type type = typeMapper.map(
                    correlate.getLeft().getRowType().getFieldList().get(i).getType());
            projections.add(FieldReference.newRootStructReference(i, type));
        }
        for (int i = 0; i < rightFieldCount; i++) {
            int fieldIdx = leftFieldCount + groupKeyCount + i;
            Type type = typeMapper.map(
                    correlate.getRight().getRowType().getFieldList().get(i).getType());
            projections.add(FieldReference.newRootStructReference(fieldIdx, type));
        }
        return projections;
    }

    /**
     * Extracts correlation equality pairs from a filter condition.
     * Each pair is [leftFieldIndex, rightFieldIndex] where left refers to the
     * correlation variable's field and right refers to the filter input's field.
     */
    private List<int[]> extractCorrelationEqualities(RexNode condition, CorrelationId corId) {
        List<int[]> result = new ArrayList<>();
        if (condition instanceof RexCall call) {
            if (call.getKind() == SqlKind.EQUALS) {
                int[] eq = matchCorrelationEquality(call, corId);
                if (eq != null) result.add(eq);
            } else if (call.getKind() == SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    if (operand instanceof RexCall eqCall
                            && eqCall.getKind() == SqlKind.EQUALS) {
                        int[] eq = matchCorrelationEquality(eqCall, corId);
                        if (eq != null) result.add(eq);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Matches a single equality: $cor0.field[i] = $j OR $j = $cor0.field[i].
     * Returns [leftFieldIndex, rightFieldIndex] or null if not a correlation equality.
     */
    private static int[] matchCorrelationEquality(RexCall eq, CorrelationId corId) {
        RexNode left = eq.getOperands().get(0);
        RexNode right = eq.getOperands().get(1);
        // $cor0.field[i] = $j
        if (left instanceof RexFieldAccess fa
                && fa.getReferenceExpr() instanceof RexCorrelVariable cv
                && cv.id.equals(corId)
                && right instanceof RexInputRef ref) {
            return new int[]{fa.getField().getIndex(), ref.getIndex()};
        }
        // $j = $cor0.field[i]
        if (right instanceof RexFieldAccess fa
                && fa.getReferenceExpr() instanceof RexCorrelVariable cv
                && cv.id.equals(corId)
                && left instanceof RexInputRef ref) {
            return new int[]{fa.getField().getIndex(), ref.getIndex()};
        }
        return null;
    }

    /**
     * Removes correlation conditions from a filter condition, returning the
     * remaining non-correlation conditions, or null if nothing remains.
     */
    private static RexNode removeCorrelationFromCondition(RexNode condition, CorrelationId corId) {
        if (condition instanceof RexCall call && call.getKind() == SqlKind.EQUALS) {
            return matchCorrelationEquality(call, corId) != null ? null : condition;
        }
        if (condition instanceof RexCall call && call.getKind() == SqlKind.AND) {
            List<RexNode> remaining = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                if (operand instanceof RexCall eqCall
                        && eqCall.getKind() == SqlKind.EQUALS
                        && matchCorrelationEquality(eqCall, corId) != null) {
                    continue;
                }
                remaining.add(operand);
            }
            if (remaining.isEmpty()) return null;
            if (remaining.size() == 1) return remaining.get(0);
            return call.clone(call.getType(), remaining);
        }
        return condition;
    }

    /**
     * Builds the Substrait join condition for correlation equalities.
     * Maps: left.corr_field = right.group_key (with proper combined-schema indices).
     */
    private Expression buildCorrelationJoinCondition(
            List<int[]> equalities, int leftFieldCount,
            ImmutableBitSet rightGroupSet,
            RelDataType leftType, RelDataType rightType) {
        List<Expression> conditions = new ArrayList<>();
        int[] groupKeyPositions = rightGroupSet.toArray();

        for (int[] eq : equalities) {
            int leftIdx = eq[0];
            int rightFieldInGroup = -1;
            for (int i = 0; i < groupKeyPositions.length; i++) {
                if (groupKeyPositions[i] == eq[1]) {
                    rightFieldInGroup = i;
                    break;
                }
            }
            if (rightFieldInGroup < 0) continue;

            int rightIdxInCombined = leftFieldCount + rightFieldInGroup;
            Type lType = typeMapper.map(leftType.getFieldList().get(leftIdx).getType());
            Type rType = typeMapper.map(rightType.getFieldList().get(rightFieldInGroup).getType());
            Expression leftRef = FieldReference.newRootStructReference(leftIdx, lType);
            Expression rightRef = FieldReference.newRootStructReference(rightIdxInCombined, rType);

            var eqVariant = functionMapper.resolveScalarVariant(
                    new FunctionReference("comparison", "equal"));
            conditions.add(ImmutableExpression.ScalarFunctionInvocation.builder()
                    .declaration(eqVariant)
                    .outputType(TypeCreator.of(true).BOOLEAN)
                    .arguments(List.of(leftRef, rightRef))
                    .build());
        }

        if (conditions.size() == 1) return conditions.get(0);
        var andVariant = functionMapper.resolveScalarVariant(
                new FunctionReference("boolean", "and"));
        return ImmutableExpression.ScalarFunctionInvocation.builder()
                .declaration(andVariant)
                .outputType(TypeCreator.of(true).BOOLEAN)
                .arguments(conditions)
                .build();
    }

}
