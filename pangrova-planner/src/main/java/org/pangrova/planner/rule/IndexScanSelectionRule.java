package org.pangrova.planner.rule;

import org.pangrova.planner.PlanTable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.TableScan;
import org.pangrova.spi.index.IndexDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Calcite planner hook that consults an {@link IndexEvaluator} to detect whether a
 * {@link TableScan} has maintained indexes (bitmap, B-tree, bloom filter, HNSW vector, inverted)
 * applicable to its columns.
 *
 * <p><b>Architecture / status (F-IDX-04):</b> index-based acceleration in this engine is performed
 * at execution time by {@code IndexHintGenerator} (predicate→applicable-index analysis over the
 * plan's RelNode) feeding {@code IndexSplitFilter} for split/file pruning — there is <em>no</em>
 * planner-side {@code IndexScan} physical operator (the executor prunes splits rather than running
 * an index operator, and the HEP optimizer is heuristic, not cost-based over an IndexScan
 * alternative). Verified wiring: {@code IndexAwareSplitFetcher.fetchAndFilterSplits} →
 * {@code IndexHintGenerator.generate} → {@code IndexSplitFilter}.</p>
 *
 * <p>This class is therefore an <em>alternative, currently-unregistered</em> planner-side hook: as
 * written {@link #onMatch} performs no plan transformation, and it is not part of
 * {@code HepRuleRegistry}. It is retained for a potential future cost-based integration (feeding
 * index selectivity into a cost model once an {@code IndexScan} operator exists); wiring it as-is
 * would duplicate {@code IndexHintGenerator} with no additional effect on query results or pruning.</p>
 */
public final class IndexScanSelectionRule extends RelOptRule {

    private static final Logger LOG = LoggerFactory.getLogger(IndexScanSelectionRule.class);

    /**
     * Singleton instance. Uses the standard operand matching any {@link TableScan}.
     */
    public static final IndexScanSelectionRule INSTANCE = new IndexScanSelectionRule(
            operand(TableScan.class, any()),
            "IndexScanSelectionRule");

    /**
     * Functional interface for the index evaluation callback.
     * Decouples this rule from the concrete {@code IndexEvaluationService} class
     * to avoid a compile-time dependency on {@code pangrova-indexing}.
     */
    @FunctionalInterface
    public interface IndexEvaluator {
        /**
         * Finds applicable indexes for the given table and filter conditions.
         *
         * @param catalogName catalog containing the table
         * @param schemaName  schema containing the table
         * @param tableName   the table name
         * @param filterColumns columns referenced in filter predicates
         * @return list of applicable index descriptors, empty if none apply
         */
        List<IndexDescriptor> findApplicableIndexes(
                String catalogName, String schemaName, String tableName,
                List<String> filterColumns);
    }

    /**
     * Thread-local evaluator binding, set during optimization context setup.
     * Uses ThreadLocal because Calcite rules are singletons and the evaluator
     * is query-context-specific.
     */
    private static final ThreadLocal<IndexEvaluator> EVALUATOR = new ThreadLocal<>();

    /**
     * Binds an index evaluator for the current optimization pass.
     * Must be called before optimization begins and cleared after.
     */
    public static void bindEvaluator(IndexEvaluator evaluator) {
        EVALUATOR.set(evaluator);
    }

    /**
     * Clears the bound evaluator after optimization completes.
     */
    public static void clearEvaluator() {
        EVALUATOR.remove();
    }

    private IndexScanSelectionRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IndexEvaluator evaluator = EVALUATOR.get();
        if (evaluator == null) {
            // No index evaluator bound — indexing module not active
            return;
        }

        TableScan scan = call.rel(0);
        List<String> qualifiedName = PlanTable.require(scan).getQualifiedName();
        if (qualifiedName.size() < 3) {
            return; // Need catalog.schema.table
        }

        String catalogName = qualifiedName.get(0);
        String schemaName = qualifiedName.get(1);
        String tableName = qualifiedName.get(2);

        // Extract filter columns from the scan's row type (the actual predicate
        // pushdown happens at a higher level; here we check if any index exists
        // for the table's columns that could benefit planning cost estimation)
        List<String> columnNames = scan.getRowType().getFieldNames();

        List<IndexDescriptor> applicableIndexes = evaluator.findApplicableIndexes(
                catalogName, schemaName, tableName, columnNames);

        if (!applicableIndexes.isEmpty()) {
            LOG.debug("event=index_scan_candidate table={}.{}.{} indexes={}",
                    catalogName, schemaName, tableName, applicableIndexes.size());
            // No plan transformation: this engine applies indexes via execution-time split pruning
            // (IndexHintGenerator → IndexSplitFilter), not a planner IndexScan operator. See the
            // class javadoc — this hook is retained only for future cost-based integration.
        }
    }
}
