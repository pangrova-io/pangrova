package org.pangrova.planner.runtimefilter;

import org.pangrova.types.enums.PangrovaType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Analyzes an optimized RelNode plan to detect eligible joins and produce
 * runtime filter descriptors. Eligible joins are INNER or SEMI joins with
 * equi-join conditions on columns from both sides.
 *
 * <p>Descriptors describe candidate keys in the input ordering of this plan.
 * A publisher still needs physical exchange, scan and attempt identities.</p>
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
        // A filter on the preserved probe side of an outer join would discard unmatched rows.
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.SEMI) {
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

        if (!join.getSystemFieldList().isEmpty()) {
            return Optional.empty();
        }
        var probeFields = join.getLeft().getRowType().getFieldList();
        var buildFields = join.getRight().getRowType().getFieldList();
        int probeIndex = Math.min(leftRef.getIndex(), rightRef.getIndex());
        int buildIndex = Math.max(leftRef.getIndex(), rightRef.getIndex()) - probeFields.size();
        if (probeIndex < 0 || probeIndex >= probeFields.size() || buildIndex < 0 || buildIndex >= buildFields.size()) {
            return Optional.empty();
        }
        // Native HashJoin builds its right input. Physical join reordering already changes the input tree.
        var probe = probeFields.get(probeIndex);
        var build = buildFields.get(buildIndex);
        if (!SqlTypeUtil.equalSansNullability(
                join.getCluster().getTypeFactory(), probe.getType(), build.getType())) {
            return Optional.empty();
        }
        PangrovaType type = switch (build.getType().getSqlTypeName()) {
            case TINYINT -> PangrovaType.TINYINT;
            case SMALLINT -> PangrovaType.SMALLINT;
            case INTEGER -> PangrovaType.INTEGER;
            case BIGINT -> PangrovaType.BIGINT;
            case VARCHAR -> PangrovaType.VARCHAR;
            default -> null;
        };
        return type == null ? Optional.empty() : Optional.of(new EquiJoinPair(build.getName(), probe.getName(), type));
    }

    private record EquiJoinPair(String buildColumn, String probeColumn, PangrovaType buildColumnType) {}
}
