package org.pangrova.planner.core.rerank;

import org.pangrova.planner.core.rerank.RerankerPlanNode.CascadeStrategy;
import org.pangrova.planner.core.rerank.RerankerPlanNode.CrossEncoderStrategy;
import org.pangrova.planner.core.rerank.RerankerPlanNode.FullTextSource;
import org.pangrova.planner.core.rerank.RerankerPlanNode.LinearCombinationStrategy;
import org.pangrova.planner.core.rerank.RerankerPlanNode.RetrievalSource;
import org.pangrova.planner.core.rerank.RerankerPlanNode.RrfStrategy;
import org.pangrova.planner.core.rerank.RerankerPlanNode.VectorSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RerankerPlanNodeTest {

    @Test
    void rrfStrategyValidatesPositiveK() {
        assertThatThrownBy(() -> new RrfStrategy(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("k must be positive");

        assertThatThrownBy(() -> new RrfStrategy(-1))
                .isInstanceOf(IllegalArgumentException.class);

        var strategy = new RrfStrategy(60);
        assertThat(strategy.k()).isEqualTo(60);
    }

    @Test
    void linearCombinationValidatesFinitePositiveWeights() {
        assertThatThrownBy(() -> new LinearCombinationStrategy(-0.1, 0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finite and positive");
        assertThatThrownBy(() -> new LinearCombinationStrategy(0.0, 0.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LinearCombinationStrategy(Double.NaN, 0.5))
                .isInstanceOf(IllegalArgumentException.class);

        var strategy = new LinearCombinationStrategy(0.7, 0.3);
        assertThat(strategy.weight1()).isEqualTo(0.7);
        assertThat(strategy.weight2()).isEqualTo(0.3);
    }

    @Test
    void crossEncoderValidatesParameters() {
        assertThatThrownBy(() -> new CrossEncoderStrategy("", 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");

        assertThatThrownBy(() -> new CrossEncoderStrategy("model", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topN must be positive");

        var strategy = new CrossEncoderStrategy("ms_marco", 20);
        assertThat(strategy.modelName()).isEqualTo("ms_marco");
        assertThat(strategy.topN()).isEqualTo(20);
    }

    @Test
    void cascadeRequiresAtLeastTwoStages() {
        assertThatThrownBy(() -> new CascadeStrategy(List.of(new RrfStrategy(60))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 2 stages");

        var cascade = new CascadeStrategy(List.of(
                new RrfStrategy(60),
                new CrossEncoderStrategy("model", 5)));
        assertThat(cascade.stages()).hasSize(2);
    }

    @Test
    void vectorSourceDefensivelyCopiesQueryVector() {
        float[] original = {1.0f, 2.0f, 3.0f};
        var source = new VectorSource("embedding", original, "cosine_distance");

        original[0] = 99.0f;
        source.queryVector()[0] = 88.0f;
        assertThat(source.queryVector()[0]).isEqualTo(1.0f);
    }

    @Test
    void planNodeRequiresNonEmptySources() {
        assertThatThrownBy(() -> new RerankerPlanNode(
                new RrfStrategy(60), List.of(), 10, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sources must not be empty");
    }

    @Test
    void planNodeRequiresPositiveTopK() {
        List<RetrievalSource> sources = List.of(
                new VectorSource("vec", new float[]{1.0f}, "cosine_distance"));

        assertThatThrownBy(() -> new RerankerPlanNode(
                new RrfStrategy(60), sources, 0, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topK must be positive");
    }

    @Test
    void isHybridReturnsTrueForMultipleSources() {
        var vectorSource = new VectorSource("vec", new float[]{1.0f}, "cosine_distance");
        var ftsSource = new FullTextSource(
                new FullTextScoringNode("table", "content", "query", List.of()));

        var hybrid = new RerankerPlanNode(
                new RrfStrategy(60), List.of(vectorSource, ftsSource), 10, "query", List.of());
        assertThat(hybrid.isHybrid()).isTrue();

        var single = new RerankerPlanNode(
                new RrfStrategy(60), List.of(vectorSource), 10, null, List.of());
        assertThat(single.isHybrid()).isFalse();
    }

    @Test
    void isMultiVectorReturnsTrueForMultipleTargetColumns() {
        var source = new VectorSource("vec1", new float[]{1.0f}, "cosine_distance");

        var multiVec = new RerankerPlanNode(
                new RrfStrategy(60), List.of(source), 10, null, List.of("vec1", "vec2"));
        assertThat(multiVec.isMultiVector()).isTrue();

        var singleVec = new RerankerPlanNode(
                new RrfStrategy(60), List.of(source), 10, null, List.of("vec1"));
        assertThat(singleVec.isMultiVector()).isFalse();
    }

    @Test
    void targetColumnsDefaultsToEmptyList() {
        var source = new VectorSource("vec", new float[]{1.0f}, "cosine_distance");
        var node = new RerankerPlanNode(
                new RrfStrategy(60), List.of(source), 10, null, null);
        assertThat(node.targetColumns()).isEmpty();
    }
}
