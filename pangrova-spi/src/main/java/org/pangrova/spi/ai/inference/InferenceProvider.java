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
package org.pangrova.spi.ai.inference;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.annotation.ThreadSafe;

import java.util.List;
import java.util.Set;

/**
 * SPI contract for generative AI / LLM inference providers.
 *
 * <p>Implementations call external model endpoints (OpenAI-compatible, vLLM,
 * Ollama, etc.) to produce text completions from prompts. The engine invokes
 * this interface on the Worker data plane during scalar and table function
 * evaluation (ai_complete, ai_summarize, ai_classify, ai_filter).</p>
 *
 * <p>Implementations must handle rate limiting, retry with exponential backoff,
 * and request timeout internally (matching the pattern established by
 * {@link org.pangrova.spi.ai.embedding.EmbeddingProvider}).</p>
 */
@Experimental
@ThreadSafe
public interface InferenceProvider {

    /**
     * Generates a text completion for the given prompt.
     *
     * @param prompt     the prompt text (must not be null)
     * @param parameters generation parameters controlling output behavior
     * @return inference result including generated text and token usage (never null)
     * @throws IllegalArgumentException if prompt is null
     */
    InferenceResult complete(String prompt, GenerationParameters parameters);

    /**
     * Generates completions for a batch of prompts.
     *
     * <p>Batch size must not exceed {@link #maxBatchSize()}. Implementations
     * should optimize for throughput.</p>
     *
     * @param prompts    the prompts to complete (must not be null or empty; size &le; maxBatchSize)
     * @param parameters shared generation parameters
     * @return list of inference results, one per input prompt, in order (never null)
     * @throws IllegalArgumentException if prompts is null, empty, or exceeds maxBatchSize
     */
    List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters);

    /**
     * Returns the model name this provider serves.
     *
     * @return the model name (never null or blank)
     */
    String modelName();

    /**
     * Returns the maximum number of prompts per batch request.
     *
     * @return positive integer representing the max batch size
     */
    int maxBatchSize();

    // ---- Multimodal extensions (default methods for backward compatibility) ----

    /**
     * Generates a completion for a multimodal prompt containing mixed content (text + images/files).
     * Default implementation falls back to text-only completion by extracting text content.
     *
     * @param prompt     the multimodal prompt (must not be null)
     * @param parameters generation parameters
     * @return inference result (never null)
     */
    default InferenceResult completeMultimodal(MultimodalPrompt prompt, GenerationParameters parameters) {
        return complete(prompt.textContent(), parameters);
    }

    /**
     * Returns the set of input modalities supported by this provider.
     * Default returns TEXT only. Override to advertise multimodal capabilities.
     *
     * @return set of supported modalities (never null or empty)
     */
    default Set<Modality> supportedModalities() {
        return Set.of(Modality.TEXT);
    }
}
