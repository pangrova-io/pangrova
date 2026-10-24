package org.pangrova.planner.mv;

import java.util.Locale;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

/**
 * Conservative whitelist of materialized view definition shapes.
 *
 * <p>Transparent rewrite is enabled only for deterministic SPJG definitions
 * with decomposable aggregates. Everything else is rejected before Calcite
 * substitution runs; there is no best-effort fallback.
 */
public final class MaterializedViewPlanEligibility {

    private MaterializedViewPlanEligibility() {
    }

    /**
     * Returns why a definition cannot participate in transparent rewrite.
     *
     * @param plan compiled definition plan
     * @return skip reason, or empty when the definition is eligible
     */
    public static Optional<String> ineligibleReason(RelNode plan) {
        if (plan instanceof TableScan) {
            return Optional.empty();
        }
        if (plan instanceof Project project) {
            return deterministic(project.getProjects())
                    .or(() -> ineligibleReason(project.getInput()));
        }
        if (plan instanceof Filter filter) {
            if (!RexUtil.isDeterministic(filter.getCondition())) {
                return Optional.of("definition filter is not deterministic");
            }
            return ineligibleReason(filter.getInput());
        }
        if (plan instanceof Join join) {
            if (join.getJoinType() != JoinRelType.INNER) {
                return Optional.of("definition join is " + join.getJoinType() + ", only INNER is supported");
            }
            if (!RexUtil.isDeterministic(join.getCondition())) {
                return Optional.of("definition join condition is not deterministic");
            }
            return ineligibleReason(join.getLeft())
                    .or(() -> ineligibleReason(join.getRight()));
        }
        if (plan instanceof Aggregate aggregate) {
            if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
                return Optional.of("definition aggregate uses grouping sets");
            }
            for (AggregateCall call : aggregate.getAggCallList()) {
                if (call.isDistinct()) {
                    return Optional.of("definition aggregate uses DISTINCT");
                }
                Optional<String> unsupported = unsupportedAggregate(call);
                if (unsupported.isPresent()) {
                    return unsupported;
                }
            }
            return ineligibleReason(aggregate.getInput());
        }
        return Optional.of("definition uses unsupported relational operator "
                + plan.getRelTypeName());
    }

    private static Optional<String> deterministic(java.util.List<RexNode> expressions) {
        for (RexNode expression : expressions) {
            if (!RexUtil.isDeterministic(expression)) {
                return Optional.of("definition projection is not deterministic");
            }
        }
        return Optional.empty();
    }

    private static Optional<String> unsupportedAggregate(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        if (kind == SqlKind.SUM || kind == SqlKind.SUM0
                || kind == SqlKind.COUNT || kind == SqlKind.MIN || kind == SqlKind.MAX) {
            return Optional.empty();
        }
        return Optional.of("definition aggregate " + call.getAggregation().getName()
                .toUpperCase(Locale.ROOT) + " is not decomposable");
    }
}
