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
package org.pangrova.spi.ai.rerank;

import java.util.Map;

import org.pangrova.spi.annotation.Experimental;

/**
 * A reranked result row with its fused relevance score and metadata.
 *
 * <p>Produced by a {@link Reranker} after scoring and reordering candidates.
 * The {@code metadata} carries additional information from the reranking process
 * (e.g., per-source scores, explanation data). The metadata map is defensively
 * copied to ensure immutability.</p>
 *
 * @param rowOffset byte offset of the row in the data file (must be non-negative)
 * @param score     fused relevance score after reranking (higher = more relevant)
 * @param metadata  additional information from the reranking process (immutable)
 */
@Experimental
public record RankedResult(long rowOffset, double score, Map<String, String> metadata) {

    public RankedResult {
        if (rowOffset < 0) {
            throw new IllegalArgumentException("rowOffset must be non-negative");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        metadata = Map.copyOf(metadata);
    }
}
