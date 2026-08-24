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
package org.pangrova.coordinator.ai.cache;

import java.util.Optional;

/**
 * Contract for semantic query caching.
 *
 * <p>Implementations store query results keyed by embedding similarity,
 * enabling cache hits for semantically equivalent queries without requiring
 * exact SQL text matching.</p>
 */
public interface SemanticCache {

    Optional<SemanticCacheHit> findSimilar(
            float[] queryEmbedding, String tenantId, String principal, double threshold);

    void put(SemanticCacheEntry entry);

    void invalidateByTable(String tableName);

    long size();
}
