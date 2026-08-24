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

import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.annotation.ThreadSafe;

/**
 * Reorders candidate results from one or more retrieval sources using a relevance scoring strategy.
 *
 * <p>Implementations fuse and reorder {@link ScoredCandidate} entries based on their scoring
 * algorithm (e.g., Reciprocal Rank Fusion, weighted linear combination, cross-encoder model).
 * The reranker returns at most {@code topK} results sorted by descending fused score.</p>
 *
 * @see RerankerFactory
 * @see RankedResult
 * @see ScoredCandidate
 */
@Experimental
@ThreadSafe
public interface Reranker {

    /**
     * Reranks the given candidates and returns the top-K results.
     *
     * @param query      the original query text used for relevance scoring (may be null for
     *                   strategies like RRF that do not use query text)
     * @param candidates scored candidates from retrieval sources (must not be null)
     * @param topK       maximum number of results to return (must be positive)
     * @return ranked results sorted by descending fused score, at most {@code topK} entries
     * @throws IllegalArgumentException if candidates is null or topK is not positive
     */
    List<RankedResult> rerank(String query, List<ScoredCandidate> candidates, int topK);

    /**
     * Returns the name of this reranker instance.
     *
     * @return the reranker name, never null
     */
    String name();
}
