package org.pangrova.planner.optimization;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.pangrova.planner.mv.CalciteMaterializedViewRewriter;
import org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig;
import org.pangrova.planner.runtimefilter.JoinRuntimeFilterPlanner;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Post-optimization pass that applies materialized view rewrite and
 * dynamic partition pruning (DPP) analysis to an optimized RelNode plan.
 *
 * <p>MV rewrite substitutes table scans with pre-computed materialized view
 * scans when a valid, fresh MV covers the query. DPP analysis identifies
 * join patterns eligible for runtime filter injection at the worker level.</p>
 */
public final class MaterializedViewRewritePass {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewRewritePass.class);

    private final CalciteMaterializedViewRewriter materializedViewRewriter;
    private final DynamicPartitionPruningConfig dynamicPartitionPruningConfig;
    private final JoinRuntimeFilterPlanner joinRuntimeFilterPlanner;
    private final RuntimeFilterRegistry runtimeFilterRegistry;
    private volatile org.pangrova.planner.mv.MaterializedViewRewriteDecision lastDecision =
            org.pangrova.planner.mv.MaterializedViewRewriteDecision.disabled();

    /**
     * Creates a rewrite pass combining materialized view substitution and runtime filter planning.
     *
     * @param materializedViewRewriter rewriter used for materialized view substitution
     * @param dynamicPartitionPruningConfig pruning configuration, may be null for defaults
     * @param runtimeFilterRegistry registry collecting planned runtime filters
     */
    public MaterializedViewRewritePass(
            CalciteMaterializedViewRewriter materializedViewRewriter,
            DynamicPartitionPruningConfig dynamicPartitionPruningConfig,
            RuntimeFilterRegistry runtimeFilterRegistry) {
        this.materializedViewRewriter = materializedViewRewriter;
        this.dynamicPartitionPruningConfig = dynamicPartitionPruningConfig != null
                ? dynamicPartitionPruningConfig : DynamicPartitionPruningConfig.DEFAULT;
        this.joinRuntimeFilterPlanner = new JoinRuntimeFilterPlanner();
        this.runtimeFilterRegistry = runtimeFilterRegistry != null
                ? runtimeFilterRegistry : new RuntimeFilterRegistry();
    }

    /**
     * Analyzes the optimized plan for eligible joins when DPP analysis is enabled.
     *
     * Descriptors identify logical candidates only. This pass has no build-attempt values
     * or physical scan binding and never publishes a predicate to the execution registry.
     */
    public List<RuntimeFilterDescriptor> analyzeForRuntimeFilters(RelNode optimized) {
        if (!dynamicPartitionPruningConfig.enabled()) {
            return List.of();
        }
        // F-AQE-02: Do NOT clear the shared registry here. With queryId-namespaced
        // keys, each query's filters are isolated and stale entries from aborted
        // planning do not interfere. Global clear() would destroy other queries'
        // in-flight runtime filters.
        List<RuntimeFilterDescriptor> descriptors = joinRuntimeFilterPlanner.analyze(optimized);
        if (!descriptors.isEmpty()) {
            LOG.info("event=dpp_descriptors_analyzed count={}", descriptors.size());
        }
        return descriptors;
    }

    /**
     * Analyze a plan for partition-pruning-eligible joins without registering filters.
     * Useful for testing and inspection.
     */
    public List<RuntimeFilterDescriptor> analyzeJoinsForPartitionPruning(RelNode plan) {
        return joinRuntimeFilterPlanner.analyze(plan);
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

    /** Returns the runtime filter registry for DPP. */
    public RuntimeFilterRegistry getRuntimeFilterRegistry() {
        return runtimeFilterRegistry;
    }

    /** Returns the DPP configuration. */
    public DynamicPartitionPruningConfig getDynamicPartitionPruningConfig() {
        return dynamicPartitionPruningConfig;
    }
}
