package org.pangrova.planner.rule;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite RelOptRule that detects CTE-like patterns (duplicate subplans) and
 * replaces them with {@link CteTempTableScan} for materialization.
 *
 * <p>Matches on any non-leaf RelNode whose structural digest appears multiple
 * times in the plan tree (indicating an inlined CTE that was expanded by
 * SqlToRelConverter). When materialization is cheaper than re-computation,
 * calls {@code call.transformTo()} with a CteTempTableScan node, enabling
 * downstream execution to compute once and reuse.</p>
 *
 * <p>Uses structural digests (via {@link SqlExplainLevel#DIGEST_ATTRIBUTES})
 * rather than internal node IDs, ensuring correct duplicate detection across
 * HEP planner vertex wrappers.</p>
 *
 * <p>Tracks previously materialized digests so that once the first occurrence
 * is replaced, subsequent occurrences with the same structure are also replaced
 * (even though the count in the current plan may have dropped).</p>
 *
 * <p>Uses the cost model from {@link CTEMaterializationRule} to make the
 * materialization decision based on estimated row count and reference count.</p>
 */
public final class CteRelOptRule extends RelOptRule {

    private static final Logger LOG = LoggerFactory.getLogger(CteRelOptRule.class);

    /** Marker field name appended to indicate materialization decision. */
    static final String MATERIALIZATION_MARKER = "__cte_materialized";

    public static final CteRelOptRule INSTANCE = new CteRelOptRule();

    /** Tracks whether the rule has fired during optimization (for testing). */
    private static volatile boolean lastFired;

    /**
     * Tracks structural digests that have been materialized during the current
     * optimization pass. When the first occurrence is replaced, subsequent
     * occurrences with the same structure are also replaced.
     * Cleared via {@link #resetFired()} before each optimization.
     */
    private static final Set<String> MATERIALIZED_DIGESTS = ConcurrentHashMap.newKeySet();

    private CteRelOptRule() {
        super(operand(RelNode.class, any()), "CteRelOptRule");
    }

    /** Returns true if the rule fired during the last optimization pass. */
    public static boolean didFire() {
        return lastFired;
    }

    /** Marks the rule as having fired (called by {@link org.pangrova.planner.optimization.CteMaterializationPass}). */
    public static void markFired() {
        lastFired = true;
    }

    /** Resets the fired state and materialized digest tracking. */
    public static void resetFired() {
        lastFired = false;
        MATERIALIZED_DIGESTS.clear();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        RelNode node = call.rel(0);
        if (node.getInputs().isEmpty()) {
            return false;
        }
        if (node instanceof CteTempTableScan) {
            return false;
        }
        String nodeStructure = structuralDigest(node);
        if (MATERIALIZED_DIGESTS.contains(nodeStructure)) {
            return true;
        }
        RelNode root = call.getPlanner().getRoot();
        if (root == null) {
            return false;
        }
        int refCount = countStructuralOccurrences(root, nodeStructure);
        return refCount >= 2;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode node = call.rel(0);
        String nodeStructure = structuralDigest(node);

        RelMetadataQuery mq = call.getMetadataQuery();
        Double rowCount = mq.getRowCount(node);
        long estimatedRows = rowCount != null ? Math.max(rowCount.longValue(), 0) : 0;

        double cteCost = estimatedRows > 0 ? estimatedRows : 1000.0;

        if (!MATERIALIZED_DIGESTS.contains(nodeStructure)) {
            RelNode root = call.getPlanner().getRoot();
            int refCount = countStructuralOccurrences(root, nodeStructure);
            if (!CTEMaterializationRule.shouldMaterialize(cteCost, refCount, estimatedRows)) {
                return;
            }
        }

        MATERIALIZED_DIGESTS.add(nodeStructure);

        CteTempTableScan cteTempScan = new CteTempTableScan(
                node.getCluster(),
                node.getTraitSet(),
                "cte_" + Integer.toHexString(nodeStructure.hashCode()),
                nodeStructure,
                node.getRowType(),
                estimatedRows);

        call.transformTo(cteTempScan);

        lastFired = true;
        LOG.info("event=cte_materialization_applied estimatedRows={}", estimatedRows);
    }

    /**
     * Produces a structural digest of a RelNode subtree that is independent of
     * internal node IDs and HEP vertex wrappers. Two structurally identical
     * subtrees (same operators, same predicates, same tables) produce the same
     * digest regardless of their position in the plan graph.
     */
    static String structuralDigest(RelNode node) {
        StringWriter sw = new StringWriter();
        RelWriterImpl writer = new RelWriterImpl(
                new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false);
        node.explain(writer);
        return sw.toString().trim();
    }

    /**
     * Counts how many times a given structural digest appears in the plan tree.
     */
    static int countStructuralOccurrences(RelNode root, String targetStructure) {
        Map<String, Integer> digestCounts = new HashMap<>();
        collectStructuralDigests(root, digestCounts);
        return digestCounts.getOrDefault(targetStructure, 0);
    }

    private static void collectStructuralDigests(RelNode node, Map<String, Integer> counts) {
        if (!node.getInputs().isEmpty() && !(node instanceof CteTempTableScan)) {
            String digest = structuralDigest(node);
            counts.merge(digest, 1, Integer::sum);
        }
        for (RelNode child : node.getInputs()) {
            collectStructuralDigests(child, counts);
        }
    }
}
