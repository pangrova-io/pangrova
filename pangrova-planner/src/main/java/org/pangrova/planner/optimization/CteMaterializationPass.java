package org.pangrova.planner.optimization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.planner.rule.CteRelOptRule;
import org.pangrova.planner.rule.CteTempTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimization pass that detects duplicate subtrees (inlined CTEs) in the plan
 * and replaces them with {@link CteTempTableScan} references to a single
 * materialized result.
 *
 * <p>Calcite's SqlToRelConverter inlines CTE references, producing structurally
 * identical subtrees. This pass identifies them by computing a structural digest
 * (ignoring node IDs) and replaces duplicates with temp scan references.</p>
 *
 * <p>Runs before HEP/CBO passes so that downstream optimization operates on
 * the deduplicated plan.</p>
 */
public final class CteMaterializationPass implements OptimizationPass {

    private static final Logger LOG = LoggerFactory.getLogger(CteMaterializationPass.class);

    @Override
    public RelNode optimize(RelNode relNode, OptimizationContext context) {
        // Step 1: Collect structural digests for all non-leaf subtrees
        Map<String, List<RelNode>> digestToNodes = new HashMap<>();
        collectDigests(relNode, digestToNodes);

        // Step 2: Find digests that appear multiple times (CTE candidates)
        Map<String, RelNode> materializationTargets = new HashMap<>();
        for (Map.Entry<String, List<RelNode>> entry : digestToNodes.entrySet()) {
            List<RelNode> nodes = entry.getValue();
            if (nodes.size() >= 2) {
                materializationTargets.put(entry.getKey(), nodes.get(0));
            }
        }

        if (materializationTargets.isEmpty()) {
            return relNode;
        }

        // Step 3: Replace all occurrences with CteTempTableScan
        RelNode rewritten = new CteReplacementShuttle(materializationTargets).visit(relNode);

        CteRelOptRule.markFired();
        LOG.info("event=cte_materialization_pass_applied materializedCount={}",
                materializationTargets.size());

        return rewritten;
    }

    @Override
    public String name() {
        return "CteMaterialization";
    }

    @Override
    public boolean isApplicable(RelNode relNode, OptimizationContext context) {
        return countNonLeafNodes(relNode) >= 2;
    }

    /**
     * Computes a structural digest for a RelNode subtree that is independent of
     * internal node IDs and HEP vertex wrappers. Uses DIGEST_ATTRIBUTES level
     * without ID prefix, producing a canonical string based purely on operator
     * types and their attributes. Two structurally identical subtrees (same
     * operators, same predicates, same tables) produce the same digest regardless
     * of their position in the plan graph.
     */
    static String structuralDigest(RelNode node) {
        java.io.StringWriter sw = new java.io.StringWriter();
        RelWriterImpl writer = new RelWriterImpl(
                new java.io.PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false);
        node.explain(writer);
        return sw.toString().trim();
    }

    private void collectDigests(RelNode node, Map<String, List<RelNode>> digestToNodes) {
        // Skip leaf nodes (table scans are cheap to re-read)
        if (!node.getInputs().isEmpty()) {
            String digest = structuralDigest(node);
            digestToNodes.computeIfAbsent(digest, k -> new ArrayList<>()).add(node);
        }
        for (RelNode input : node.getInputs()) {
            collectDigests(input, digestToNodes);
        }
    }

    private int countNonLeafNodes(RelNode node) {
        int count = node.getInputs().isEmpty() ? 0 : 1;
        for (RelNode input : node.getInputs()) {
            count += countNonLeafNodes(input);
        }
        return count;
    }

    /**
     * Shuttle that replaces subtrees matching materialization targets with
     * {@link CteTempTableScan} nodes.
     */
    private static final class CteReplacementShuttle extends RelShuttleImpl {

        private final Map<String, RelNode> materializationTargets;

        CteReplacementShuttle(Map<String, RelNode> materializationTargets) {
            this.materializationTargets = materializationTargets;
        }

        @Override
        public RelNode visit(RelNode other) {
            if (!other.getInputs().isEmpty() && !(other instanceof CteTempTableScan)) {
                String digest = structuralDigest(other);
                if (materializationTargets.containsKey(digest)) {
                    RelNode representative = materializationTargets.get(digest);
                    RelMetadataQuery mq = other.getCluster().getMetadataQuery();
                    Double rowCount = mq.getRowCount(representative);
                    long estimatedRows = rowCount != null ? Math.max(rowCount.longValue(), 0) : 0;

                    return new CteTempTableScan(
                            other.getCluster(),
                            other.getTraitSet(),
                            "cte_" + Integer.toHexString(digest.hashCode()),
                            digest,
                            other.getRowType(),
                            estimatedRows);
                }
            }
            return super.visit(other);
        }
    }
}
