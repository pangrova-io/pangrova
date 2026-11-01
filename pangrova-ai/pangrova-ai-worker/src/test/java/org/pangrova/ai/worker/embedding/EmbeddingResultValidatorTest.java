/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.worker.embedding;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.invocation.EmbeddingProvider;
import org.pangrova.ai.worker.embedding.openai.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.worker.embedding.openai.OpenAiEmbeddingConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingResultValidatorTest {

    private final EmbeddingProvider provider = new OpenAiCompatibleEmbeddingProvider(
            new OpenAiEmbeddingConfig("http://localhost:11434/v1/embeddings", "embed", "",
                    2, 8, 1000.0, "", "", 30));

    @Test
    void validatesAndDefensivelyCopiesOrderedBatch() {
        float[] first = new float[]{1.0f, 2.0f};
        List<float[]> validated = EmbeddingResultValidator.validateBatch(
                provider, List.of(first, new float[]{3.0f, 4.0f}), 2, "test");

        first[0] = 99.0f;
        assertThat(validated.get(0)).containsExactly(1.0f, 2.0f);
        assertThat(validated.get(1)).containsExactly(3.0f, 4.0f);
    }

    @Test
    void rejectsNullAndNonFiniteProviderVectors() {
        assertInvalid(List.of(new float[]{1.0f}));
        assertInvalid(java.util.Collections.singletonList(null));
        assertInvalid(List.of(new float[]{Float.NaN, 1.0f}));
        assertInvalid(List.of(new float[]{Float.POSITIVE_INFINITY, 1.0f}));
        assertInvalid(List.of(new float[]{Float.NEGATIVE_INFINITY, 1.0f}));
    }

    @Test
    void batchRejectsCardinalityMismatchAtomically() {
        assertThatThrownBy(() -> EmbeddingResultValidator.validateBatch(
                provider, List.of(new float[]{1, 2}), 2, "test"))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("cardinality mismatch")
                .hasMessageContaining("expected=2 actual=1");
        assertThatThrownBy(() -> EmbeddingResultValidator.validateBatch(provider, null, 2, "test"))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("actual=null");
    }

    private void assertInvalid(List<float[]> vectors) {
        assertThatThrownBy(() -> EmbeddingResultValidator.validateBatch(
                provider, vectors, 1, "test"))
                .isInstanceOf(PangrovaException.class)
                .extracting("errorCode")
                .isEqualTo(PangrovaErrorCode.AI_EMBEDDING_DIMENSION_MISMATCH);
    }
}
