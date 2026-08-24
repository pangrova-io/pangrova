package org.pangrova.planner.rule;

import java.util.List;

import org.pangrova.planner.stats.StatisticsCollector;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite RelOptRule that prunes partitions by evaluating filter predicates
 * against column min/max statistics at optimization time.
 *
 * <p>Matches on LogicalFilter over LogicalTableScan. For each simple comparison
 * predicate (column op literal), checks whether the literal falls outside the
 * column's [min, max] range. If all rows in the table are guaranteed to not
 * satisfy the predicate, replaces the filter with an always-false condition
 * (empty result).</p>
 *
 * <p>Delegates the pruning decision to {@link PartitionPruningRule} for the
 * actual min/max range check logic.</p>
 */
public final class PartitionPruningRelOptRule extends RelOptRule {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionPruningRelOptRule.class);

    private final StatisticsCollector statisticsCollector;
    private final PartitionPruningRule pruningLogic;

    /** Tracks whether the rule has fired during optimization (for testing). */
    private static volatile boolean lastFired;

    /**
     * Creates a partition pruning rule backed by the given statistics collector.
     *
     * @param statisticsCollector statistics source used for pruning decisions
     */
    public PartitionPruningRelOptRule(StatisticsCollector statisticsCollector) {
        super(operand(LogicalFilter.class, operand(LogicalTableScan.class, none())),
                "PartitionPruningRelOptRule");
        this.statisticsCollector = statisticsCollector;
        this.pruningLogic = new PartitionPruningRule();
    }

    /** Returns true if the rule fired during the last optimization pass. */
    public static boolean didFire() {
        return lastFired;
    }

    /** Resets the fired state (call before optimization in tests). */
    public static void resetFired() {
        lastFired = false;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(1);
        String tableName = resolveTableName(scan);
        return tableName != null
                && statisticsCollector.getStatistics(tableName).isPresent();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        String tableName = resolveTableName(scan);
        if (tableName == null) {
            return;
        }

        RexNode condition = filter.getCondition();
        List<String> fieldNames = scan.getRowType().getFieldNames();

        if (canPruneEntirePartition(condition, tableName, fieldNames)) {
            lastFired = true;
            LOG.info("event=partition_pruned table={}", tableName);
            RexNode falseCondition = call.builder().getRexBuilder().makeLiteral(false);
            LogicalFilter pruned = filter.copy(
                    filter.getTraitSet(), scan, falseCondition);
            call.transformTo(pruned);
        }
    }

    /**
     * Checks if the predicate guarantees no rows match based on column statistics.
     * Supports simple comparisons: column = literal where the literal falls
     * outside the column's [min, max] range.
     */
    @SuppressWarnings("unchecked")
    private boolean canPruneEntirePartition(RexNode condition, String tableName,
                                           List<String> fieldNames) {
        if (!(condition instanceof RexCall rexCall)) {
            return false;
        }
        SqlKind kind = rexCall.getKind();
        if (!isComparisonKind(kind)) {
            return false;
        }
        List<RexNode> operands = rexCall.getOperands();
        if (operands.size() != 2) {
            return false;
        }

        RexInputRef colRef = null;
        RexLiteral literal = null;
        if (operands.get(0) instanceof RexInputRef ref
                && operands.get(1) instanceof RexLiteral lit) {
            colRef = ref;
            literal = lit;
        } else if (operands.get(1) instanceof RexInputRef ref
                && operands.get(0) instanceof RexLiteral lit) {
            colRef = ref;
            literal = lit;
        }
        if (colRef == null || literal == null) {
            return false;
        }

        int fieldIndex = colRef.getIndex();
        if (fieldIndex >= fieldNames.size()) {
            return false;
        }
        String columnName = fieldNames.get(fieldIndex);
        Comparable<?> value = literal.getValueAs(Comparable.class);
        if (value == null) {
            return false;
        }

        return pruningLogic.shouldPrune(tableName, columnName, value, statisticsCollector);
    }

    private static boolean isComparisonKind(SqlKind kind) {
        return kind == SqlKind.EQUALS
                || kind == SqlKind.GREATER_THAN
                || kind == SqlKind.GREATER_THAN_OR_EQUAL
                || kind == SqlKind.LESS_THAN
                || kind == SqlKind.LESS_THAN_OR_EQUAL;
    }

    private static String resolveTableName(LogicalTableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        if (qualifiedName.isEmpty()) {
            return null;
        }
        // Use the last segment (table name) to match statistics keys,
        // stripping schema/catalog prefixes that may be empty strings
        return qualifiedName.get(qualifiedName.size() - 1);
    }
}
