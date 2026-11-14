package org.pangrova.planner.rule;

import org.pangrova.planner.metadata.PlanTable;

import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlKind;
import org.pangrova.connector.api.metadata.AggregationApplication;
import org.pangrova.connector.api.metadata.AggregationApplication.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite rule that pushes simple aggregations (COUNT, MIN, MAX, SUM) into
 * the connector table scan via {@link ConnectorPushdownTable#pushAggregate}.
 *
 * <p>Matches {@code Aggregate -> TableScan} where the aggregate uses only
 * functions that connectors commonly support via metadata (manifest stats,
 * Puffin sketches, partition pre-aggregation).</p>
 *
 * <p>Result handling follows the same guaranteed/residual pattern as
 * {@link LimitPushdownRule}.</p>
 */
public final class AggregatePushdownRule extends RelOptRule {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatePushdownRule.class);

    /** Aggregate functions eligible for pushdown. */
    private static final java.util.Set<SqlKind> PUSHABLE_KINDS = java.util.Set.of(
            SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX, SqlKind.SUM, SqlKind.SUM0);

    public static final AggregatePushdownRule INSTANCE = new AggregatePushdownRule();

    private AggregatePushdownRule() {
        super(operand(LogicalAggregate.class, operand(LogicalTableScan.class, none())),
                "AggregatePushdownRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        // All aggregate calls must be pushable
        for (AggregateCall aggCall : agg.getAggCallList()) {
            if (!PUSHABLE_KINDS.contains(aggCall.getAggregation().getKind())) {
                return false;
            }
        }

        return PlanTable.require(scan).unwrap(ConnectorPushdownTable.class) != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        ConnectorPushdownTable pushdown = PlanTable.require(scan).unwrap(ConnectorPushdownTable.class);
        if (pushdown == null) {
            return;
        }

        // Build the AggregationApplication from Calcite's Aggregate
        List<AggregateFunction> functions = new ArrayList<>();
        for (AggregateCall aggCall : agg.getAggCallList()) {
            String funcName = aggCall.getAggregation().getName().toLowerCase(Locale.ROOT);
            // For now, use empty column handles (connector resolves by position)
            functions.add(new AggregateFunction(funcName, List.of(), aggCall.isDistinct()));
        }

        AggregationApplication application = new AggregationApplication(functions, List.of());

        Optional<ConnectorPushdownTable.PushdownResult> result = pushdown.pushAggregate(application);
        if (result.isEmpty()) {
            return;
        }

        LOG.info("event=aggregate_pushdown_applied table={} functions={} guaranteed={}",
                PlanTable.require(scan).getQualifiedName(), functions.size(), result.get().guaranteed());

        RelNode rewritten;
        if (result.get().guaranteed()) {
            rewritten = scan;
        } else {
            rewritten = agg.copy(agg.getTraitSet(), scan, agg.getGroupSet(),
                    agg.getGroupSets(), agg.getAggCallList());
        }
        call.transformTo(rewritten);
    }
}
