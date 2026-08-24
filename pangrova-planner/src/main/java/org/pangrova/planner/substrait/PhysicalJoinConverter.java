package org.pangrova.planner.substrait;

import io.substrait.relation.ExtensionMulti;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.Rel;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
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
import org.apache.calcite.rel.type.RelDataType;
import io.substrait.type.TypeCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

/**
 * Converts optimizer-selected physical join and exchange nodes
 * (merge/hash/broadcast/nested-loop joins, exchanges) to Substrait
 * ExtensionRel nodes with native-engine detail payloads.
 */
final class PhysicalJoinConverter {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalJoinConverter.class);

    private final TypeMapper typeMapper;
    private final Function<RelNode, Rel> relVisitor;

    PhysicalJoinConverter(TypeMapper typeMapper, Function<RelNode, Rel> relVisitor) {
        this.typeMapper = typeMapper;
        this.relVisitor = relVisitor;
    }

    /**
     * Converts an optimizer-selected sort-merge join to a Substrait ExtensionMultiRel
     * encoding MergeJoinNode semantics for the native engine.
     *
     * <p>The native engine parses the MergeJoinRelDetail JSON payload and builds
     * a native MergeJoinNode. When inputs are not pre-sorted on join keys, the
     * native engine inserts SortNode operators before the MergeJoinNode.</p>
     */
    Rel visitMergeJoin(PangrovaSortMergeJoin mergeJoin) {
        Rel left = relVisitor.apply(mergeJoin.getLeft());
        Rel right = relVisitor.apply(mergeJoin.getRight());

        JoinInfo joinInfo = mergeJoin.analyzeCondition();
        // PERF-T13: Use ImmutableIntList directly (read-only) instead of copying to ArrayList.
        List<Integer> leftKeys = joinInfo.leftKeys;
        List<Integer> rightKeys = joinInfo.rightKeys;

        boolean inputsPreSorted = areInputsPreSortedOnKeys(
                mergeJoin.getLeft(), mergeJoin.getRight(), leftKeys, rightKeys);

        var outputType = TypeCreator.NULLABLE.struct(mapOutputTypes(mergeJoin.getRowType()));

        String joinTypeName = mapJoinTypeToString(mergeJoin.getJoinType());

        var detail = new MergeJoinRelDetail(
                joinTypeName, leftKeys, rightKeys, inputsPreSorted, outputType);

        LOG.debug("event=convert_merge_join joinType={} leftKeys={} rightKeys={} preSorted={}",
                joinTypeName, leftKeys, rightKeys, inputsPreSorted);

        return ExtensionMulti.from(detail, List.of(left, right)).build();
    }

    /**
     * Converts an PangrovaExchange to a Substrait ExtensionSingleRel with
     * ExchangeRelDetail metadata, making physical distribution explicit in the IR.
     */
    Rel visitExchange(PangrovaExchange exchange) {
        Rel input = relVisitor.apply(exchange.getInput());
        var outputType = TypeCreator.NULLABLE.struct(mapOutputTypes(exchange.getRowType()));

        var dist = exchange.targetDistribution();
        String distName = dist.type().name();
        List<Integer> hashKeys = dist.type() == PangrovaDistribution.Type.HASH
                ? dist.keys() : List.of();

        var detail = new ExchangeRelDetail(distName, hashKeys, outputType);
        LOG.debug("event=convert_exchange distribution={} hashKeys={}", distName, hashKeys);
        return ExtensionSingle.from(detail, input).build();
    }

    /**
     * Converts a CBO-selected hash join to a Substrait ExtensionMultiRel
     * encoding HashJoinNode semantics for the native engine.
     */
    Rel visitHashJoin(PangrovaHashJoin hashJoin) {
        return buildPhysicalHashJoin(hashJoin, "hash_join");
    }

    /**
     * Converts a CBO-selected broadcast join to a Substrait ExtensionMultiRel
     * encoding broadcast HashJoinNode semantics for the native engine.
     */
    Rel visitBroadcastJoin(PangrovaBroadcastJoin broadcastJoin) {
        return buildPhysicalHashJoin(broadcastJoin, "broadcast_join");
    }

    /**
     * Converts a CBO-selected nested loop join to a Substrait ExtensionMultiRel
     * encoding NestedLoopJoinNode semantics for the native engine.
     *
     * <p>Used for non-equi (theta) joins where hash or merge strategies are
     * not applicable. The native engine evaluates the join condition row-by-row.</p>
     */
    Rel visitNestedLoopJoin(PangrovaNestedLoopJoin nestedLoopJoin) {
        Rel left = relVisitor.apply(nestedLoopJoin.getLeft());
        Rel right = relVisitor.apply(nestedLoopJoin.getRight());

        var outputType = TypeCreator.NULLABLE.struct(mapOutputTypes(nestedLoopJoin.getRowType()));

        String joinTypeName = mapJoinTypeToString(nestedLoopJoin.getJoinType());

        var detail = new NestedLoopJoinRelDetail(joinTypeName, outputType);

        LOG.debug("event=convert_nested_loop_join joinType={}", joinTypeName);

        return ExtensionMulti.from(detail, List.of(left, right)).build();
    }

    /**
     * Shared logic for converting physical hash-based joins (hash and broadcast)
     * to Substrait ExtensionMultiRel with join strategy metadata.
     */
    private Rel buildPhysicalHashJoin(org.apache.calcite.rel.core.Join join, String strategy) {
        Rel left = relVisitor.apply(join.getLeft());
        Rel right = relVisitor.apply(join.getRight());

        JoinInfo joinInfo = join.analyzeCondition();
        // PERF-T13: Use ImmutableIntList directly (read-only) instead of copying to ArrayList.
        List<Integer> leftKeys = joinInfo.leftKeys;
        List<Integer> rightKeys = joinInfo.rightKeys;

        var outputType = TypeCreator.NULLABLE.struct(mapOutputTypes(join.getRowType()));

        String joinTypeName = mapJoinTypeToString(join.getJoinType());

        var detail = new HashJoinRelDetail(
                strategy, joinTypeName, leftKeys, rightKeys, outputType);

        LOG.debug("event=convert_{} joinType={} leftKeys={} rightKeys={}",
                strategy, joinTypeName, leftKeys, rightKeys);

        return ExtensionMulti.from(detail, List.of(left, right)).build();
    }

    private List<io.substrait.type.Type> mapOutputTypes(RelDataType outputRowType) {
        return outputRowType.getFieldList().stream()
                .map(f -> typeMapper.map(f.getType()))
                .toList();
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

    private static String mapJoinTypeToString(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> "INNER";
            case LEFT -> "LEFT";
            case RIGHT -> "RIGHT";
            case FULL -> "OUTER";
            case SEMI -> "SEMI";
            case ANTI -> "ANTI";
        };
    }
}
