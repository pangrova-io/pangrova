package org.pangrova.coordinator.query.execution;

import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.planner.core.rerank.RerankerPlanNode;
import org.pangrova.planner.core.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.indexing.planner.vector.VectorSearchNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Groups supplementary parameters for query dispatch, reducing the parameter
 * count of {@code QueryDispatcher.dispatchQuery()} from 6 to 4.
 *
 * @param snapshotOverrides time-travel snapshot overrides per table (never null, may be empty)
 * @param originalSql       the original SQL text for plan serialization
 * @param compiler          the SQL compiler for Substrait conversion
 * @param vectorSearchNode  rewritten vector search plan node (empty for non-vector queries)
 * @param rerankerPlan      reranker plan node for hybrid search reranking (empty for non-reranking queries)
 */
public record DispatchQueryContext(
        Map<String, Long> snapshotOverrides,
        String originalSql,
        SqlCompiler compiler,
        List<String> preOptColumnNames,
        Optional<VectorSearchNode> vectorSearchNode,
        Optional<RerankerPlanNode> rerankerPlan,
        Optional<DmlWriteContext> dmlWriteContext,
        List<RuntimeFilterDescriptor> runtimeFilterDescriptors) {

    public DispatchQueryContext {
        snapshotOverrides = snapshotOverrides != null ? Map.copyOf(snapshotOverrides) : Map.of();
        preOptColumnNames = preOptColumnNames != null ? List.copyOf(preOptColumnNames) : List.of();
        vectorSearchNode = vectorSearchNode != null ? vectorSearchNode : Optional.empty();
        rerankerPlan = rerankerPlan != null ? rerankerPlan : Optional.empty();
        dmlWriteContext = dmlWriteContext != null ? dmlWriteContext : Optional.empty();
        runtimeFilterDescriptors = runtimeFilterDescriptors != null
                ? List.copyOf(runtimeFilterDescriptors) : List.of();
    }

    public DispatchQueryContext(Map<String, Long> snapshotOverrides,
                                String originalSql,
                                SqlCompiler compiler) {
        this(snapshotOverrides, originalSql, compiler, List.of(),
                Optional.empty(), Optional.empty(), Optional.empty(), List.of());
    }

    public DispatchQueryContext(Map<String, Long> snapshotOverrides,
                                String originalSql,
                                SqlCompiler compiler,
                                List<String> preOptColumnNames) {
        this(snapshotOverrides, originalSql, compiler, preOptColumnNames,
                Optional.empty(), Optional.empty(), Optional.empty(), List.of());
    }

    /**
     * Returns a copy with the vector search node attached.
     */
    public DispatchQueryContext withVectorSearch(VectorSearchNode node) {
        return new DispatchQueryContext(snapshotOverrides, originalSql, compiler,
                preOptColumnNames, Optional.of(node), rerankerPlan,
                dmlWriteContext, runtimeFilterDescriptors);
    }

    /**
     * Returns a copy with reranker plan attached.
     */
    public DispatchQueryContext withRerankerPlan(RerankerPlanNode plan) {
        return new DispatchQueryContext(snapshotOverrides, originalSql, compiler,
                preOptColumnNames, vectorSearchNode, Optional.of(plan),
                dmlWriteContext, runtimeFilterDescriptors);
    }

    /**
     * Returns a copy with DML write context attached for native write dispatch.
     */
    public DispatchQueryContext withDmlWriteContext(DmlWriteContext ctx) {
        return new DispatchQueryContext(snapshotOverrides, originalSql, compiler,
                preOptColumnNames, vectorSearchNode, rerankerPlan,
                Optional.of(ctx), runtimeFilterDescriptors);
    }
}
