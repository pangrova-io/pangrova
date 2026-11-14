package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import org.pangrova.execution.stage.binding.RetrievalGenerationIntent;


import org.junit.jupiter.api.Test;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.query.api.task.QuerySplit;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pre-I/O validation/fallback contracts; published generation resolution has real store/E2E coverage. */
class RetrievalGenerationResolverTest {
    private final RetrievalGenerationResolver resolver = new RetrievalGenerationResolver(null);
    // If any path unexpectedly tries to read an artifact, decoding this sentinel fails.
    private static final List<QuerySplit> SPLITS = List.of(new QuerySplit("must-not-decode", List.of()));

    @Test
    void exactRetrievalAndEmptyStageNeedNoArtifactOwner() {
        assertThat(resolver.resolve(fragment(RetrievalTopKPlanHint.Kind.EXACT, ""),
                List.of(), Optional.empty(), SPLITS)).isEmpty();
        assertThat(resolver.resolve(fragment(RetrievalTopKPlanHint.Kind.FULL_TEXT, "FULL_TEXT:body"),
                List.of(), Optional.empty(), List.of())).isEmpty();
    }

    @Test
    void missingFullTextArtifactCannotBecomeVectorExactFallback() {
        var fullText = fragment(RetrievalTopKPlanHint.Kind.FULL_TEXT, "FULL_TEXT:body");
        assertThatThrownBy(() -> resolver.resolve(fullText, List.of(), Optional.empty(), SPLITS))
                .hasMessageContaining("no immutable artifact binding");
        assertThatThrownBy(() -> resolver.resolve(fullText,
                List.of(intent("FULL_TEXT:body", true)), Optional.empty(), SPLITS))
                .hasMessageContaining("complete immutable BM25 artifact");
        assertThat(resolver.resolve(fragment(RetrievalTopKPlanHint.Kind.VECTOR, "VECTOR:embedding"),
                List.of(intent("VECTOR:embedding", false)), Optional.empty(), SPLITS)).isEmpty();
    }

    @Test
    void duplicatedOrUnconsumedBindingsAreRejected() {
        var vector = intent("VECTOR:embedding", false);
        var plan = fragment(RetrievalTopKPlanHint.Kind.VECTOR, "VECTOR:embedding");
        assertThatThrownBy(() -> resolver.resolve(plan, List.of(vector, vector), Optional.empty(), SPLITS))
                .hasMessageContaining("Duplicate retrieval source binding");
        assertThatThrownBy(() -> resolver.resolve(plan,
                List.of(vector, intent("VECTOR:other", false)), Optional.empty(), SPLITS))
                .hasMessageContaining("not represented in the physical Native stage");
    }

    private static SerializedPlanFragment fragment(RetrievalTopKPlanHint.Kind kind, String source) {
        return new SerializedPlanFragment(new byte[0], List.of(new RetrievalTopKPlanHint(kind, source,
                0, 10, RetrievalTopKPlanHint.ScoreDirection.LOWER_IS_BETTER, List.of(1), new byte[0])));
    }

    private static RetrievalGenerationIntent intent(String source, boolean fullText) {
        return new RetrievalGenerationIntent("tenant", source, "artifact", "format", "1", "table", "snapshot",
                fullText ? BuiltInCapabilityCatalog.bm25RetrievalPlanRef() : BuiltInCapabilityCatalog.hnswRetrievalPlanRef(),
                List.of(), 10);
    }
}
