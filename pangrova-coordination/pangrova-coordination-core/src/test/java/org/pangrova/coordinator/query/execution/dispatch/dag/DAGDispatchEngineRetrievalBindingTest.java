package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.query.core.stage.binding.RetrievalMergeIntent;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalPlanBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalPlanBinder.DistributedRetrieval;

import java.util.List;
import java.util.Map;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.protocol.v2.ScoreDirection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DAGDispatchEngineRetrievalBindingTest {

    @Test
    void selectsExactOutputTopKForNativeGlobalMerge() {
        SerializedPlanFragment output = fragment(RetrievalTopKPlanHint.Kind.EXACT, "");

        assertThat(RetrievalPlanBinder.shouldBindDistributed(
                output, true, true)).isTrue();
        assertThat(RetrievalPlanBinder.shouldBindDistributed(
                output, false, false)).isFalse();
    }

    @Test
    void rejectsSplitBearingDistributedRetrievalOutput() {
        SerializedPlanFragment output = fragment(RetrievalTopKPlanHint.Kind.EXACT, "");

        assertThatThrownBy(() -> RetrievalPlanBinder.shouldBindDistributed(
                output, true, false))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("split-free singleton Native stage");
    }

    @Test
    void refusesHybridLeafWhenBm25ArtifactIsUnavailable() {
        SerializedPlanFragment leaf = fragment(
                RetrievalTopKPlanHint.Kind.VECTOR, "source-a");
        RetrievalMergeIntent merge = new RetrievalMergeIntent(
                RetrievalMergeIntent.Strategy.RRF,
                10,
                60,
                List.of(
                        new RetrievalMergeIntent.Source(
                                "source-a",
                                RetrievalMergeIntent.SourceKind.VECTOR,
                                ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                                1.0,
                                40),
                        new RetrievalMergeIntent.Source(
                                "source-b",
                                RetrievalMergeIntent.SourceKind.FULL_TEXT,
                                ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                                1.0,
                                40)));

        assertThatThrownBy(() -> RetrievalPlanBinder.bindDistributed(
                new DistributedRetrieval(
                        leaf, "query-1", 1, false, false,
                        java.util.Optional.of(merge),
                        java.util.Optional.empty(), Map.of())))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("BM25 retrieval has no immutable artifact binding");
    }

    private static SerializedPlanFragment fragment(
            RetrievalTopKPlanHint.Kind kind, String sourceKey) {
        NamedStruct schema = NamedStruct.newBuilder()
                .addNames("id")
                .addNames("score")
                .setStruct(Type.Struct.newBuilder()
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()))
                        .addTypes(Type.newBuilder().setFp64(Type.FP64.newBuilder())))
                .build();
        RetrievalTopKPlanHint hint = new RetrievalTopKPlanHint(
                kind,
                sourceKey,
                1,
                10,
                RetrievalTopKPlanHint.ScoreDirection.LOWER_IS_BETTER,
                List.of(0),
                schema.toByteArray());
        return new SerializedPlanFragment(new byte[0], List.of(hint));
    }
}
