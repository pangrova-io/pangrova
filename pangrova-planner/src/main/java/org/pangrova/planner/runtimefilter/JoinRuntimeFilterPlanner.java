package org.pangrova.planner.runtimefilter;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Analyzes an optimized RelNode plan to detect eligible joins and produce
 * runtime filter descriptors. Eligible joins are INNER, LEFT, or SEMI joins with
 * equi-join conditions on columns from both sides.
 *
 * <p>This component bridges the gap between the Calcite plan tree and the
 * {@link RuntimeFilterGenerator} / {@link RuntimeFilterRegistry} DPP subsystem.
 * It does NOT execute the build side — it produces {@link RuntimeFilterDescriptor}
 * metadata that downstream stages use to generate and apply actual filters.</p>
 *
 * <p>Thread-safe: stateless, all state is passed via method parameters.</p>
 */
public final class JoinRuntimeFilterPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(JoinRuntimeFilterPlanner.class);

    /**
     * Analyze the plan tree and produce runtime filter descriptors for eligible joins.
     *
     * @param plan optimized RelNode plan
     * @return list of runtime filter descriptors (empty if no eligible joins found)
     */
    public List<RuntimeFilterDescriptor> analyze(RelNode plan) {
        if (plan == null) {
            return Collections.emptyList();
        }
        List<RuntimeFilterDescriptor> descriptors = new ArrayList<>();
        collectJoinFilters(plan, descriptors);
        LOG.debug("event=dpp_analysis_complete filterCount={}", descriptors.size());
        return Collections.unmodifiableList(descriptors);
    }

    private void collectJoinFilters(RelNode node, List<RuntimeFilterDescriptor> descriptors) {
        // Pattern: Filter directly above a Join whose own condition is trivial (true).
        // Calcite's JOIN_EXTRACT_FILTER rule extracts non-equi-or-extracted join
        // conditions into a Filter above the Join, leaving the Join with condition=true.
        // We treat the parent Filter's condition as the effective join condition for DPP.
        if (node instanceof Filter filter && filter.getInput() instanceof Join join
                && isTrivialTrue(join.getCondition())) {
            collectMultiColumnJoinFilters(join, filter.getCondition(), descriptors);
            // Skip recursion into the join itself — we'd just see condition=true and
            // produce no descriptors. Recurse into the join's inputs to find nested joins.
            for (RelNode input : join.getInputs()) {
                collectJoinFilters(input, descriptors);
            }
            return;
        }
        if (node instanceof Join join) {
            collectMultiColumnJoinFilters(join, join.getCondition(), descriptors);
        }
        for (RelNode input : node.getInputs()) {
            collectJoinFilters(input, descriptors);
        }
    }

    private static boolean isTrivialTrue(RexNode condition) {
        return condition instanceof RexLiteral lit
                && Boolean.TRUE.equals(lit.getValueAs(Boolean.class));
    }

    private void collectMultiColumnJoinFilters(
            Join join, RexNode condition, List<RuntimeFilterDescriptor> descriptors) {
        JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT
                && joinType != JoinRelType.SEMI) {
            return;
        }
        List<EquiJoinPair> pairs = extractEquiJoinColumns(condition, join);
        for (EquiJoinPair pair : pairs) {
            String filterId = "dpp-" + UUID.randomUUID();
            descriptors.add(new RuntimeFilterDescriptor(
                    filterId, pair.buildColumn(), pair.probeColumn(),
                    joinType.name(), pair.buildColumnType()));
        }
    }

    /**
     * Extract all equi-join column pairs from a join condition.
     * Supports simple EQUALS conditions and multiple EQUALS in an AND chain.
     */
    private List<EquiJoinPair> extractEquiJoinColumns(RexNode condition, Join join) {
        if (!(condition instanceof RexCall call)) {
            return List.of();
        }

        if (call.getKind() == SqlKind.EQUALS) {
            return extractFromEquals(call, join)
                    .map(List::of)
                    .orElse(List.of());
        }

        if (call.getKind() == SqlKind.AND) {
            List<EquiJoinPair> pairs = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                pairs.addAll(extractEquiJoinColumns(operand, join));
            }
            return pairs;
        }

        return List.of();
    }

    private Optional<EquiJoinPair> extractFromEquals(RexCall equalsCall, Join join) {
        List<RexNode> operands = equalsCall.getOperands();
        if (operands.size() != 2) {
            return Optional.empty();
        }

        if (!(operands.get(0) instanceof RexInputRef leftRef)
                || !(operands.get(1) instanceof RexInputRef rightRef)) {
            return Optional.empty();
        }

        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        return resolveBuildProbePair(join, leftRef.getIndex(), rightRef.getIndex(), leftFieldCount);
    }

    /**
     * Resolve build/probe sides from equi-join column indices.
     *
     * <p>For LEFT/SEMI joins, right side is always the build side (semantic constraint).
     * For INNER joins, the smaller side (by estimated row count) is selected as build.
     * Falls back to right=build when statistics are unavailable.</p>
     */
    private Optional<EquiJoinPair> resolveBuildProbePair(
            Join join, int leftIdx, int rightIdx, int leftFieldCount) {
        if (leftIdx < leftFieldCount && rightIdx >= leftFieldCount) {
            boolean leftIsBuild = selectLeftAsBuild(join);
            if (leftIsBuild) {
                String buildColumn = resolveColumnName(join.getLeft(), leftIdx);
                String probeColumn = resolveColumnName(join.getRight(), rightIdx - leftFieldCount);
                PangrovaType buildType = resolveColumnType(join.getLeft(), leftIdx);
                return Optional.of(new EquiJoinPair(buildColumn, probeColumn, buildType));
            }
            String probeColumn = resolveColumnName(join.getLeft(), leftIdx);
            String buildColumn = resolveColumnName(join.getRight(), rightIdx - leftFieldCount);
            PangrovaType buildType = resolveColumnType(join.getRight(), rightIdx - leftFieldCount);
            return Optional.of(new EquiJoinPair(buildColumn, probeColumn, buildType));
        }
        if (rightIdx < leftFieldCount && leftIdx >= leftFieldCount) {
            boolean leftIsBuild = selectLeftAsBuild(join);
            if (leftIsBuild) {
                String buildColumn = resolveColumnName(join.getLeft(), rightIdx);
                String probeColumn = resolveColumnName(join.getRight(), leftIdx - leftFieldCount);
                PangrovaType buildType = resolveColumnType(join.getLeft(), rightIdx);
                return Optional.of(new EquiJoinPair(buildColumn, probeColumn, buildType));
            }
            String probeColumn = resolveColumnName(join.getLeft(), rightIdx);
            String buildColumn = resolveColumnName(join.getRight(), leftIdx - leftFieldCount);
            PangrovaType buildType = resolveColumnType(join.getRight(), leftIdx - leftFieldCount);
            return Optional.of(new EquiJoinPair(buildColumn, probeColumn, buildType));
        }
        return Optional.empty();
    }

    /**
     * Determine whether the left side should be the build side based on estimated row count.
     *
     * <p>For LEFT/SEMI joins, right is always build (semantic requirement).
     * For INNER joins, the smaller side is selected as build.
     * Falls back to right=build when row count statistics are unavailable.</p>
     */
    boolean selectLeftAsBuild(Join join) {
        if (join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.SEMI) {
            return false;
        }
        try {
            RelMetadataQuery mq = join.getCluster().getMetadataQuery();
            Double leftRowCount = mq.getRowCount(join.getLeft());
            Double rightRowCount = mq.getRowCount(join.getRight());
            if (leftRowCount == null || rightRowCount == null) {
                LOG.debug("event=build_side_fallback reason=stats_unavailable");
                return false;
            }
            boolean leftSmaller = leftRowCount < rightRowCount;
            LOG.debug("event=build_side_selected leftRows={} rightRows={} buildSide={}",
                    leftRowCount, rightRowCount, leftSmaller ? "LEFT" : "RIGHT");
            return leftSmaller;
        } catch (Exception e) {
            LOG.debug("event=build_side_fallback reason=metadata_error", e);
            return false;
        }
    }

    private String resolveColumnName(RelNode input, int index) {
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        if (index >= 0 && index < fields.size()) {
            return fields.get(index).getName();
        }
        return "col_" + index;
    }

    private PangrovaType resolveColumnType(RelNode input, int index) {
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        if (index >= 0 && index < fields.size()) {
            return PangrovaType.fromSqlTypeName(
                    fields.get(index).getType().getSqlTypeName().getName());
        }
        return PangrovaType.VARCHAR;
    }

    private record EquiJoinPair(String buildColumn, String probeColumn, PangrovaType buildColumnType) {}
}
