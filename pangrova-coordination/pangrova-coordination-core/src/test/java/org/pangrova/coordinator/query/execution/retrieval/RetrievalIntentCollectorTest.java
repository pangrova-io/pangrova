package org.pangrova.coordinator.query.execution.retrieval;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.DispatchQueryContext;
import org.pangrova.query.core.stage.binding.RetrievalMergeIntent;
import org.pangrova.planner.core.rerank.FullTextScoringNode;
import org.pangrova.planner.core.rerank.RerankerPlanNode;
import org.pangrova.indexing.adapter.planner.vector.VectorSearchNode;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.indexing.api.vector.DistanceMetric;
import org.pangrova.indexing.api.vector.VectorHybridSearchStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetrievalIntentCollectorTest {

    private final RetrievalIntentCollector collector = new RetrievalIntentCollector(null);

    @Test
    void bindsAllAnnMetricsToCanonicalNativeInvocation() {
        for (var capability : List.of(BuiltInCapabilityCatalog.hnswRetrievalPlanRef(),
                BuiltInCapabilityCatalog.ivfPqRetrievalPlanRef())) {
            for (DistanceMetric metric : DistanceMetric.values()) {
                VectorSearchNode node = new VectorSearchNode(
                        "catalog.schema.items", "embedding", new float[]{1.0f, 0.0f},
                        2, metric, "index-1", capability, 4, List.of(),
                        new VectorHybridSearchStrategy(VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, 2), 7,
                        "snapshot-7", "snapshot-7", 0);
                DispatchQueryContext context = new DispatchQueryContext(
                        Map.of(), "SELECT ...", null).withVectorSearch(node);
                var intents = collector.collectRetrievalArtifactIntents(context.vectorSearchNode(), context.rerankerPlan(), "tenant");
                assertThat(intents).hasSize(1);
                assertThat(intents.get(0).capability()).isEqualTo(capability);
                assertThat(intents.get(0).invocationOptions())
                        .filteredOn(option -> option.getName().equals("metric"))
                        .singleElement().satisfies(option ->
                                assertThat(option.getValue().getStringValue()).isEqualTo(metric.name()));
                String parameter = capability.equals(BuiltInCapabilityCatalog.hnswRetrievalPlanRef())
                        ? "ef_search" : "nprobes";
                assertThat(intents.get(0).invocationOptions())
                        .filteredOn(option -> option.getName().equals(parameter))
                        .singleElement().satisfies(option ->
                                assertThat(option.getValue().getInt64Value()).isEqualTo(4));
            }
        }
    }

    @Test
    void bindsHybridRrfSourcesToNativeMergeIntent() {
        RerankerPlanNode plan = new RerankerPlanNode(
                new RerankerPlanNode.RrfStrategy(60),
                List.of(new RerankerPlanNode.VectorSource(
                                "embedding", new float[]{1.0f}, "l2_distance"),
                        new RerankerPlanNode.FullTextSource(
                                new FullTextScoringNode(
                                        "catalog.schema.docs", "body", "lakehouse", List.of()))),
                10, "lakehouse", List.of());
        DispatchQueryContext context = new DispatchQueryContext(
                Map.of(), "SELECT ...", null).withRerankerPlan(plan);

        RetrievalMergeIntent intent = collector.collectRetrievalMergeIntent(context.vectorSearchNode(), context.rerankerPlan()).orElseThrow();

        assertThat(intent.strategy()).isEqualTo(RetrievalMergeIntent.Strategy.RRF);
        assertThat(intent.rrfK()).isEqualTo(60);
        assertThat(intent.outputLimit()).isEqualTo(10);
        assertThat(intent.sources()).extracting(RetrievalMergeIntent.Source::sourceId)
                .containsExactly("VECTOR:embedding", "FULL_TEXT:body");
        assertThat(intent.sources()).allSatisfy(source ->
                assertThat(source.candidateLimit()).isGreaterThanOrEqualTo(10));
    }

    @Test
    void rejectsAmbiguousMultiVectorBinding() {
        RerankerPlanNode plan = new RerankerPlanNode(
                new RerankerPlanNode.RrfStrategy(60),
                List.of(new RerankerPlanNode.VectorSource(
                                "vec1", new float[]{1.0f}, "l2_distance"),
                        new RerankerPlanNode.VectorSource(
                                "vec2", new float[]{2.0f}, "l2_distance")),
                10, null, List.of("vec1", "vec2"));
        DispatchQueryContext context = new DispatchQueryContext(
                Map.of(), "SELECT ...", null).withRerankerPlan(plan);

        assertThatThrownBy(() -> collector.collectRetrievalMergeIntent(context.vectorSearchNode(), context.rerankerPlan()))
                .hasMessageContaining("Multi-vector retrieval requires");
    }
}
