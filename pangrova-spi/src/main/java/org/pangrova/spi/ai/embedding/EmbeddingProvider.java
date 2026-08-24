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
package org.pangrova.spi.ai.embedding;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.annotation.ThreadSafe;

/**
 * SPI contract for embedding model providers.
 *
 * <p>Implementations produce dense vector embeddings from text or binary content.
 * The engine invokes this interface during DML execution (SYNC mode) and
 * background drain (ASYNC mode). Implementations are discovered via
 * {@link java.util.ServiceLoader}.</p>
 */
@Experimental
@ThreadSafe
public interface EmbeddingProvider {

    /**
     * Embeds a single text input.
     *
     * @param text the text to embed (must not be null)
     * @return dense embedding vector of length {@link #dimensions()}
     * @throws IllegalArgumentException if text is null
     */
    float[] embed(String text);

    /**
     * Embeds a batch of text inputs in a single request.
     *
     * <p>Batch size must not exceed {@link #maxBatchSize()}. Implementations
     * should optimize for throughput (e.g., single HTTP round-trip).</p>
     *
     * @param texts the texts to embed (must not be null or empty; size &le; maxBatchSize)
     * @return list of dense embedding vectors, one per input text, in order
     * @throws IllegalArgumentException if texts is null, empty, or exceeds maxBatchSize
     */
    List<float[]> embedBatch(List<String> texts);

    /**
     * Embeds binary content referenced by blob descriptors.
     *
     * @param refs blob references to embed (must not be null or empty)
     * @return list of dense embedding vectors, one per blob reference, in order
     * @throws IllegalArgumentException if refs is null or empty
     */
    List<float[]> embedBlob(List<BlobReference> refs);

    /**
     * Returns the output vector dimensionality of this provider.
     *
     * @return positive integer representing the embedding dimension
     */
    int dimensions();

    /**
     * Returns the model name this provider serves.
     *
     * @return the model name (never null or blank)
     */
    String modelName();

    /**
     * Returns the maximum number of inputs per batch request.
     *
     * @return positive integer representing the max batch size
     */
    int maxBatchSize();
}
