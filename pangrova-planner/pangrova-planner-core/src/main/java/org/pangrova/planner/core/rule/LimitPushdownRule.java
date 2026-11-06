package org.pangrova.planner.core.rule;

import org.pangrova.planner.core.metadata.PlanTable;

import java.util.Optional;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite RelOptRule that pushes a LIMIT clause into a connector table scan.
 *
 * <p>Matches {@code Sort(fetch = N, no order-by) -> TableScan} where the table's
 * underlying Calcite implementation supports the {@link ConnectorPushdownTable}
 * SPI. Calls
 * {@link ConnectorPushdownTable#pushLimit(long)} so the connector can absorb the
 * limit (typically by encoding it on the table handle and stopping I/O early).</p>
 *
 * <p>Pure {@code LIMIT N} is matched (no offset, no order-by). Top-N — i.e.
 * {@code ORDER BY ... LIMIT N} — is left to a separate top-N pushdown rule because
 * it requires the connector to honor the sort order, which Iceberg/Paimon do not
 * support at the storage layer today.</p>
 *
 * <p>Result handling:</p>
 * <ul>
 *   <li>{@link ConnectorPushdownTable.PushdownResult#GUARANTEED} — connector strictly
 *   caps to N rows, so the {@code Sort} (which here is a pure limit operator) is
 *   dropped: the rule rewrites the plan to the bare scan.</li>
 *   <li>{@link ConnectorPushdownTable.PushdownResult#RESIDUAL} — connector applies
 *   the limit best-effort, so the Sort is retained over the pushed-down scan to
 *   guarantee correctness.</li>
 * </ul>
 *
 * <p>Idempotency: once the connector has absorbed the limit, the rule's matches
 * check via {@link ConnectorPushdownTable#pushLimit(long)} returning empty (the
 * connector itself rejects "no smaller limit" requests) ensures HEP convergence.</p>
 */
public final class LimitPushdownRule extends RelOptRule {

    private static final Logger LOG = LoggerFactory.getLogger(LimitPushdownRule.class);

    public static final LimitPushdownRule INSTANCE = new LimitPushdownRule();

    private LimitPushdownRule() {
        super(operand(LogicalSort.class, operand(LogicalTableScan.class, none())),
                "LimitPushdownRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        // Pure LIMIT only: no order-by, no offset.
        if (!RelCollations.EMPTY.equals(sort.getCollation())) {
            return false;
        }
        if (sort.fetch == null) {
            return false;
        }
        if (sort.offset != null) {
            return false;
        }
        LogicalTableScan scan = call.rel(1);
        return PlanTable.require(scan).unwrap(ConnectorPushdownTable.class) != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        ConnectorPushdownTable pushdown = PlanTable.require(scan).unwrap(ConnectorPushdownTable.class);
        if (pushdown == null) {
            return;
        }

        long limit = parseLimit(sort.fetch);
        if (limit <= 0) {
            return;
        }

        Optional<ConnectorPushdownTable.PushdownResult> result = pushdown.pushLimit(limit);
        if (result.isEmpty()) {
            return;
        }

        LOG.info("event=limit_pushdown_applied table={} limit={} guaranteed={}",
                PlanTable.require(scan).getQualifiedName(), limit, result.get().guaranteed());

        // The same RelOptTable now references a table whose connector handle
        // encodes the limit; the existing scan node still points at it, so we
        // just reuse the scan as-is.
        RelNode rewritten;
        if (result.get().guaranteed()) {
            // Connector strictly caps to N rows — drop the Sort entirely.
            rewritten = scan;
        } else {
            // Connector applies a best-effort early-stop — retain the Sort
            // over the pushed-down scan to guarantee correctness.
            rewritten = ((Sort) sort).copy(sort.getTraitSet(), scan,
                    sort.getCollation(), sort.offset, sort.fetch);
        }
        call.transformTo(rewritten);
    }

    private static long parseLimit(RexNode fetch) {
        if (!(fetch instanceof RexLiteral lit)) {
            return -1;
        }
        Number value = lit.getValueAs(Number.class);
        return value == null ? -1 : value.longValue();
    }
}
