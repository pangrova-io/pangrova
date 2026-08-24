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

import org.pangrova.spi.annotation.Experimental;

/**
 * A candidate row from a retrieval source with its relevance score.
 *
 * <p>Represents a single result from a retrieval method (vector search, full-text search,
 * or other scoring mechanism) before reranking. The {@code source} field identifies which
 * retrieval method produced this candidate (e.g., "vector", "fts", "bm25").</p>
 *
 * @param rowOffset byte offset of the row in the data file (must be non-negative)
 * @param score     relevance score from the retrieval source (higher = more relevant)
 * @param source    identifier of the retrieval method that produced this candidate
 */
@Experimental
public record ScoredCandidate(long rowOffset, double score, String source) {

    public ScoredCandidate {
        if (rowOffset < 0) {
            throw new IllegalArgumentException("rowOffset must be non-negative");
        }
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source must not be null or blank");
        }
    }
}
