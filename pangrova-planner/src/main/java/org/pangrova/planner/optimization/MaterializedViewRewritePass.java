package org.pangrova.planner.optimization;

import org.apache.calcite.rel.RelNode;
import org.pangrova.planner.mv.CalciteMaterializedViewRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Applies materialized-view substitution to an optimized plan. */
public final class MaterializedViewRewritePass {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewRewritePass.class);

    private final CalciteMaterializedViewRewriter materializedViewRewriter;
    private volatile org.pangrova.planner.mv.MaterializedViewRewriteDecision lastDecision =
            org.pangrova.planner.mv.MaterializedViewRewriteDecision.disabled();

    public MaterializedViewRewritePass(CalciteMaterializedViewRewriter materializedViewRewriter) {
        this.materializedViewRewriter = materializedViewRewriter;
    }

    /**
     * Applies materialized view rewrite as a post-optimization pass.
     * Returns the original plan unchanged when no rewriter is wired. Any rewrite
     * failure is fail-closed: the original plan is executed rather than an
     * unproven substitution.
     */
    public RelNode tryMaterializedViewRewrite(RelNode relNode) {
        if (materializedViewRewriter == null) {
            lastDecision = org.pangrova.planner.mv.MaterializedViewRewriteDecision.disabled();
            return relNode;
        }
        try {
            CalciteMaterializedViewRewriter.RewriteResult result =
                    materializedViewRewriter.rewriteWithDecision(relNode);
            lastDecision = result.decision();
            return result.plan();
        } catch (RuntimeException e) {
            LOG.warn("event=materialized_view_rewrite_skipped reason=exception", e);
            lastDecision = org.pangrova.planner.mv.MaterializedViewRewriteDecision.skipped(
                    "exception");
            return relNode;
        }
    }

    /**
     * Returns the diagnostic decision of the most recent rewrite attempt.
     *
     * @return rewrite decision; disabled before any attempt or without a rewriter
     */
    public org.pangrova.planner.mv.MaterializedViewRewriteDecision lastDecision() {
        return lastDecision;
    }
}
