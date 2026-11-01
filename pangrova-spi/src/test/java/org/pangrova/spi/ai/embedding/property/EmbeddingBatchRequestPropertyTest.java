package org.pangrova.spi.ai.embedding.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.spi.ai.embedding.EmbeddingBatchRequest;
import org.pangrova.spi.ai.embedding.EmbeddingPurpose;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EmbeddingBatchRequestPropertyTest {
    @Property(tries = 100, seed = "20261002")
    void physicalRebatchingPreservesPurposeAndEveryInput(
            @ForAll("texts") List<String> texts,
            @ForAll EmbeddingPurpose purpose,
            @ForAll("chunkSize") int chunkSize) {
        List<EmbeddingBatchRequest.Input> inputs = texts.stream()
                .map(text -> (EmbeddingBatchRequest.Input) new EmbeddingBatchRequest.TextInput(text)).toList();
        var whole = new EmbeddingBatchRequest(purpose, inputs);
        var reconstructed = new ArrayList<EmbeddingBatchRequest.Input>();
        for (int offset = 0; offset < inputs.size();) {
            int end = offset + Math.min(chunkSize, inputs.size() - offset);
            var chunk = new EmbeddingBatchRequest(purpose, inputs.subList(offset, end));
            assertThat(chunk.purpose()).isEqualTo(whole.purpose());
            reconstructed.addAll(chunk.inputs());
            offset = end;
        }
        assertThat(reconstructed).containsExactlyElementsOf(whole.inputs());
    }

    @Provide
    Arbitrary<List<String>> texts() {
        return Arbitraries.strings().all().ofMaxLength(40).list().ofMinSize(1).ofMaxSize(31);
    }

    @Provide
    Arbitrary<Integer> chunkSize() {
        return Arbitraries.integers().between(1, 8);
    }
}
