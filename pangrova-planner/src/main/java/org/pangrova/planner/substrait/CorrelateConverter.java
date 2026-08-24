package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ImmutableJoin;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.Join;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
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

    CorrelateConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                       ExpressionConverter rexConverter, Function<RelNode, Rel> relVisitor) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
        this.rexConverter = rexConverter;
        this.relVisitor = relVisitor;
    }

    /**
     * Converts LogicalCorrelate (lateral/correlated subquery) to a Substrait Join.
     *
     * <p>Calcite decorrelation (FILTER_SUB_QUERY_TO_CORRELATE etc.) rewrites
     * correlated subqueries into LogicalCorrelate nodes where the correlation
     * variable is embedded in the right-side plan as RexCorrelVariable references.
     * The right-side plan already contains the correlation condition (e.g., a
     * Filter with {@code $cor0.id = t2.id}), so the Substrait Join does not need
     * an explicit join condition — the native engine resolves correlation variables
     * during execution by binding the left-side row to the right-side plan.</p>
     *
     * <p>We log the correlation ID and required columns for observability so that
     * plan debugging can trace which correlation variables are in play.</p>
     */
    Rel convert(LogicalCorrelate correlate) {
        // Detect UNNEST pattern: LogicalCorrelate with Uncollect on the right side.
        // This pattern comes from LATERAL TABLE(EXPLODE(arr)) or CROSS JOIN UNNEST(arr).
        // Convert to ExtensionSingle with unnest metadata instead of JoinRel,
        // so the native engine can build a native UnnestNode (single-input operator)
        // rather than a NestedLoopJoin (which doesn't support correlation).
        RelNode rightNode = correlate.getRight();
        if (rightNode instanceof Uncollect uncollect) {
            return buildUnnestExtension(correlate, uncollect);
        }
        // Strip LogicalProject wrapper if present (Calcite sometimes wraps Uncollect).
        if (rightNode instanceof LogicalProject proj
                && proj.getInput() instanceof Uncollect uncollect) {
            return buildUnnestExtension(correlate, uncollect);
        }

        // Try manual decorrelation for scalar subquery pattern:
        // Correlate → Aggregate(group=[]) → Filter(correlation_equality) → input.
        // Calcite's RelDecorrelator fails for LEFT correlate + scalar aggregate;
        // we manually transform it into a standard JoinRel with explicit condition.
        Rel decorrelated = tryDecorrelateScalarSubquery(correlate);
        if (decorrelated != null) {
            return decorrelated;
        }

        // Fallback: non-UNNEST correlate without recognizable pattern.
        // The native NestedLoopJoinNode handles simple cross-join cases.
        Rel left = relVisitor.apply(correlate.getLeft());
        int leftFieldCount = correlate.getLeft().getRowType().getFieldCount();
        rexConverter.setCorrelateOuterFieldOffset(leftFieldCount);
        Rel right = relVisitor.apply(correlate.getRight());
        rexConverter.setCorrelateOuterFieldOffset(0);
        Join.JoinType joinType = PlanNodeConverter.mapJoinType(correlate.getJoinType());
        LOG.debug("event=convert_correlate correlationId={} requiredColumns={} joinType={}",
                correlate.getCorrelationId(), correlate.getRequiredColumns(), correlate.getJoinType());
        return ImmutableJoin.builder()
                .left(left)
                .right(right)
                .joinType(joinType)
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

        // Strip optional LogicalProject wrapper (Calcite adds this for LATERAL
        // subqueries to alias the output column, e.g., SELECT COUNT(*) AS item_count).
        if (rightNode instanceof LogicalProject proj) {
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

        List<Expression> projections = buildDecorrelationProjections(
                correlate, newAgg, leftFieldCount, newGroupSet.cardinality());

        LOG.debug("event=manual_decorrelation correlationId={} groupKeys={} joinType={}",
                corId, newGroupSet, correlate.getJoinType());

        return ImmutableProject.builder()
                .input(joinRel)
                .expressions(projections)
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
            LogicalCorrelate correlate, LogicalAggregate newAgg,
            int leftFieldCount, int groupKeyCount) {
        int rightAggCount = newAgg.getAggCallList().size();
        List<Expression> projections = new ArrayList<>();
        for (int i = 0; i < leftFieldCount; i++) {
            Type type = typeMapper.map(
                    correlate.getLeft().getRowType().getFieldList().get(i).getType());
            projections.add(FieldReference.newRootStructReference(i, type));
        }
        for (int i = 0; i < rightAggCount; i++) {
            int fieldIdx = leftFieldCount + groupKeyCount + i;
            Type type = typeMapper.map(
                    newAgg.getRowType().getFieldList().get(groupKeyCount + i).getType());
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

    /**
     * Builds an ExtensionSingle Rel encoding UNNEST (EXPLODE) semantics.
     * The native engine interprets this as a native UnnestNode.
     *
     * <p>The detail encodes: unnest column indices (from the left input),
     * output column names, and whether ordinality (POSEXPLODE) is requested.</p>
     */
    private Rel buildUnnestExtension(LogicalCorrelate correlate, Uncollect uncollect) {
        Rel leftRel = relVisitor.apply(correlate.getLeft());
        var requiredColumns = correlate.getRequiredColumns();

        var unnestColumnIndices = requiredColumns.asList();

        var unnestOutputNames = uncollect.getRowType().getFieldList().stream()
                .map(RelDataTypeField::getName)
                .toList();

        boolean withOrdinality = uncollect.withOrdinality;

        // Build output type: left input fields + unnest output fields.
        var outputFields = correlate.getRowType().getFieldList();
        var outputTypes = outputFields.stream()
                .map(f -> typeMapper.map(f.getType()))
                .toList();
        var outputType = io.substrait.type.TypeCreator.NULLABLE.struct(outputTypes);

        var detail = new UnnestRelDetail(
                unnestColumnIndices, unnestOutputNames, withOrdinality, outputType);

        LOG.debug("event=convert_unnest correlationId={} unnestColumns={} unnestNames={} ordinality={}",
                correlate.getCorrelationId(), unnestColumnIndices, unnestOutputNames, withOrdinality);

        return ExtensionSingle.from(detail, leftRel).build();
    }
}
