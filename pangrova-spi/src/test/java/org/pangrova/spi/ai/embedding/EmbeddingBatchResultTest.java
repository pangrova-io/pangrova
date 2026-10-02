package org.pangrova.spi.ai.embedding;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingBatchResultTest {

    @Test
    void vectorsAreDefensivelyCopiedAndProviderIdentityIsRetained() {
        float[] source = {0.25f, -0.5f};
        var result = new EmbeddingBatchResult(
                List.of(source), "provider-7");
        source[0] = 10;
        result.vectors().get(0)[1] = 10;

        assertThat(result.vectors().get(0)).containsExactly(0.25f, -0.5f);
        assertThat(result.providerRequestId()).isEqualTo("provider-7");
    }

    @Test
    void missingVectorsOrRequestIdentityCannotBeSilentlyInvented() {
        assertThatThrownBy(() -> new EmbeddingBatchResult(
                null, ""))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new EmbeddingBatchResult(
                List.of(new float[]{1}), null))
                .isInstanceOf(NullPointerException.class);
    }
}
