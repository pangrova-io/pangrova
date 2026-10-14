package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.relation.ImmutableSort;
import io.substrait.relation.ImmutableJoin;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.ComparisonJoinKey;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.PangrovaNestedLoopJoin;
import org.pangrova.planner.cost.join.PangrovaSortMergeJoin;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

/**
 * Converts optimizer-selected physical join and exchange nodes
 * (merge/hash/broadcast/nested-loop joins, exchanges) to Substrait
 * official Substrait physical relations. Pangrova protocol extensions are not
 * used for operators that the Substrait contract already defines.
 */
final class PhysicalJoinConverter {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalJoinConverter.class);

    /** Planner-only exchange sentinel; stage fragmentation must bind it. */
    static final int UNBOUND_PARTITION_COUNT = 0;

    private final ExpressionConverter rexConverter;
    private final Function<RelNode, Rel> relVisitor;

    PhysicalJoinConverter(ExpressionConverter rexConverter, Function<RelNode, Rel> relVisitor) {
        this.rexConverter = rexConverter;
        this.relVisitor = relVisitor;
    }

    /**
     * Converts a sort-merge join to the standard Substrait MergeJoinRel. Sort
     * requirements are represented as standard SortRel inputs, rather than a
     * private hint interpreted differently by Java and native code.
     */
    Rel visitMergeJoin(PangrovaSortMergeJoin mergeJoin) {
        Rel left = relVisitor.apply(mergeJoin.getLeft());
        Rel right = relVisitor.apply(mergeJoin.getRight());

        JoinInfo joinInfo = mergeJoin.analyzeCondition();
        if (!joinInfo.getRemaining(mergeJoin.getCluster().getRexBuilder()).isAlwaysTrue()) {
            return joinWithFullCondition(mergeJoin, left, right);
        }
        // PERF-T13: Use ImmutableIntList directly (read-only) instead of copying to ArrayList.
        List<Integer> leftKeys = joinInfo.leftKeys;
        List<Integer> rightKeys = joinInfo.rightKeys;

        boolean inputsPreSorted = areInputsPreSortedOnKeys(
                mergeJoin.getLeft(), mergeJoin.getRight(), leftKeys, rightKeys);
        if (!inputsPreSorted) {
            left = sortOnKeys(left, leftKeys);
            right = sortOnKeys(right, rightKeys);
        }

        var builder = MergeJoin.builder()
                .left(left)
                .right(right)
                .keys(comparisonKeys(left, right, leftKeys, rightKeys))
                .joinType(mapMergeJoinType(mergeJoin.getJoinType()));

        LOG.debug("event=convert_merge_join_standard joinType={} leftKeys={} rightKeys={} preSorted={}",
                mergeJoin.getJoinType(), leftKeys, rightKeys, inputsPreSorted);
        return StandardsCompliantMergeJoin.wrap(builder.build());
    }

    /**
     * Converts a PangrovaExchange to the standard Substrait ExchangeRel.
     * Distributed partition counts are intentionally emitted as unbound and must
     * be bound by stage scheduling; native execution rejects an unbound writer.
     */
    Rel visitExchange(PangrovaExchange exchange) {
        Rel input = relVisitor.apply(exchange.getInput());
        var dist = exchange.targetDistribution();
        int partitionCount = exchange.partitionCount();
        LOG.debug("event=convert_exchange_standard distribution={} hashKeys={}",
                dist.type(), dist.keys());
        return switch (dist.type()) {
            case HASH -> ScatterExchange.builder()
                    .input(input)
                    .partitionCount(partitionCount)
                    .fields(dist.keys().stream()
                            .map(index -> FieldReference.newInputRelReference(index, input))
                            .toList())
                    .build();
            case BROADCAST -> BroadcastExchange.builder()
                    .input(input)
                    .partitionCount(partitionCount)
                    .build();
            case ROUND_ROBIN -> RoundRobinExchange.builder()
                    .input(input)
                    .partitionCount(partitionCount)
                    .exact(false)
                    .build();
            case SINGLETON -> RoundRobinExchange.builder()
                    .input(input)
                    .partitionCount(1)
                    .exact(true)
                    .build();
            case ANY -> throw new SubstraitConversionException(
                    org.pangrova.common.exception.PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "ANY distribution must not materialize as an ExchangeRel");
        };
    }

    /**
     * Converts a CBO-selected hash join to standard Substrait HashJoinRel.
     */
    Rel visitHashJoin(PangrovaHashJoin hashJoin) {
        return buildPhysicalHashJoin(hashJoin);
    }

    /** A broadcast join is a HashJoinRel over a planner-visible exchange. */
    Rel visitBroadcastJoin(PangrovaBroadcastJoin broadcastJoin) {
        RelNode right = broadcastJoin.getRight();
        if (!(right instanceof PangrovaExchange exchange
                && exchange.targetDistribution().type()
                        == org.pangrova.planner.cost.distribution.PangrovaDistribution.Type.BROADCAST)
                && !(right instanceof org.pangrova.planner.cost.distribution.PangrovaExchangeInput input
                && input.distribution().type()
                        == org.pangrova.planner.cost.distribution.PangrovaDistribution.Type.BROADCAST)) {
            throw new IllegalStateException(
                    "Broadcast join requires a planner-visible broadcast exchange");
        }
        return buildPhysicalHashJoin(broadcastJoin);
    }

    /**
     * Converts a CBO-selected nested loop join to standard NestedLoopJoinRel.
     *
     * <p>Used for non-equi (theta) joins where hash or merge strategies are
     * not applicable. The native engine evaluates the join condition row-by-row.</p>
     */
    Rel visitNestedLoopJoin(PangrovaNestedLoopJoin nestedLoopJoin) {
        Rel left = relVisitor.apply(nestedLoopJoin.getLeft());
        Rel right = relVisitor.apply(nestedLoopJoin.getRight());

        LOG.debug("event=convert_nested_loop_join_standard joinType={}",
                nestedLoopJoin.getJoinType());
        return NestedLoopJoin.builder()
                .left(left)
                .right(right)
                .condition(rexConverter.convert(nestedLoopJoin.getCondition()))
                .joinType(mapNestedLoopJoinType(nestedLoopJoin.getJoinType()))
                .build();
    }

    /**
     * Shared standard HashJoinRel conversion.
     */
    private Rel buildPhysicalHashJoin(org.apache.calcite.rel.core.Join join) {
        Rel left = relVisitor.apply(join.getLeft());
        Rel right = relVisitor.apply(join.getRight());
        return buildPhysicalHashJoin(join, left, right);
    }

    private Rel buildPhysicalHashJoin(
            org.apache.calcite.rel.core.Join join, Rel left, Rel right) {

        JoinInfo joinInfo = join.analyzeCondition();
        if (!joinInfo.getRemaining(join.getCluster().getRexBuilder()).isAlwaysTrue()) {
            return joinWithFullCondition(join, left, right);
        }
        // PERF-T13: Use ImmutableIntList directly (read-only) instead of copying to ArrayList.
        List<Integer> leftKeys = joinInfo.leftKeys;
        List<Integer> rightKeys = joinInfo.rightKeys;

        var builder = HashJoin.builder()
                .left(left)
                .right(right)
                .keys(comparisonKeys(left, right, leftKeys, rightKeys))
                .joinType(mapHashJoinType(join.getJoinType()));

        LOG.debug("event=convert_hash_join_standard joinType={} leftKeys={} rightKeys={}",
                join.getJoinType(), leftKeys, rightKeys);
        return builder.build();
    }

    /** A residual ON predicate must run inside the join, before outer-row padding. */
    private Rel joinWithFullCondition(org.apache.calcite.rel.core.Join join, Rel left, Rel right) {
        return ImmutableJoin.builder()
                .left(left).right(right)
                .condition(rexConverter.convert(join.getCondition()))
                .joinType(PlanNodeConverter.mapJoinType(join.getJoinType()))
                .build();
    }

    private static List<ComparisonJoinKey> comparisonKeys(
            Rel left,
            Rel right,
            List<Integer> leftKeys,
            List<Integer> rightKeys) {
        if (leftKeys.size() != rightKeys.size() || leftKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Physical equality join requires non-empty, paired keys");
        }
        return java.util.stream.IntStream.range(0, leftKeys.size())
                .mapToObj(i -> ComparisonJoinKey.of(
                        FieldReference.newInputRelReference(leftKeys.get(i), left),
                        FieldReference.newInputRelReference(rightKeys.get(i), right),
                        ComparisonJoinKey.SimpleComparisonType.EQ))
                .toList();
    }

    private static Rel sortOnKeys(Rel input, List<Integer> keys) {
        List<Expression.SortField> fields = keys.stream()
                .<Expression.SortField>map(index -> ImmutableExpression.SortField.builder()
                        .expr(FieldReference.newInputRelReference(index, input))
                        .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                        .build())
                .toList();
        return ImmutableSort.builder().input(input).sortFields(fields).build();
    }

    /**
     * Checks whether both join inputs are pre-sorted on the join key columns.
     * An input is considered pre-sorted if it has a collation trait whose leading
     * field indices match the join keys in order.
     */
    private static boolean areInputsPreSortedOnKeys(
            RelNode left, RelNode right,
            List<Integer> leftKeys, List<Integer> rightKeys) {
        return hasMatchingCollation(left, leftKeys)
                && hasMatchingCollation(right, rightKeys);
    }

    private static boolean hasMatchingCollation(RelNode input, List<Integer> keys) {
        if (keys.isEmpty()) {
            return false;
        }
        RelCollation collation = input.getTraitSet().getCollation();
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            return false;
        }
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        if (fieldCollations.size() < keys.size()) {
            return false;
        }
        for (int i = 0; i < keys.size(); i++) {
            if (fieldCollations.get(i).getFieldIndex() != keys.get(i)) {
                return false;
            }
        }
        return true;
    }

    private static HashJoin.JoinType mapHashJoinType(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> HashJoin.JoinType.INNER;
            case LEFT -> HashJoin.JoinType.LEFT;
            case RIGHT -> HashJoin.JoinType.RIGHT;
            case FULL -> HashJoin.JoinType.OUTER;
            case SEMI -> HashJoin.JoinType.LEFT_SEMI;
            case ANTI -> HashJoin.JoinType.LEFT_ANTI;
            case ASOF, LEFT_ASOF, LEFT_MARK -> throw new IllegalArgumentException(
                    "Join type is not supported by the Substrait physical join converter: "
                            + joinType);
        };
    }

    private static MergeJoin.JoinType mapMergeJoinType(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> MergeJoin.JoinType.INNER;
            case LEFT -> MergeJoin.JoinType.LEFT;
            case RIGHT -> MergeJoin.JoinType.RIGHT;
            case FULL -> MergeJoin.JoinType.OUTER;
            case SEMI -> MergeJoin.JoinType.LEFT_SEMI;
            case ANTI -> MergeJoin.JoinType.LEFT_ANTI;
            case ASOF, LEFT_ASOF, LEFT_MARK -> throw new IllegalArgumentException(
                    "Join type is not supported by the Substrait merge-join converter: "
                            + joinType);
        };
    }

    private static NestedLoopJoin.JoinType mapNestedLoopJoinType(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> NestedLoopJoin.JoinType.INNER;
            case LEFT -> NestedLoopJoin.JoinType.LEFT;
            case RIGHT -> NestedLoopJoin.JoinType.RIGHT;
            case FULL -> NestedLoopJoin.JoinType.OUTER;
            case SEMI -> NestedLoopJoin.JoinType.LEFT_SEMI;
            case ANTI -> NestedLoopJoin.JoinType.LEFT_ANTI;
            case ASOF, LEFT_ASOF, LEFT_MARK -> throw new IllegalArgumentException(
                    "Join type is not supported by the Substrait nested-loop converter: "
                            + joinType);
        };
    }
}
