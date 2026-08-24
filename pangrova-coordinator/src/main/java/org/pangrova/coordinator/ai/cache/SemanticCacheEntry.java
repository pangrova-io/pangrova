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

import java.time.Instant;
import java.util.Set;

/**
 * An entry stored in a {@link SemanticCache}: the original SQL, its embedding
 * vector, tenant/principal scope, referenced tables, and the serialized result.
 *
 * <p>Immutable value type. The {@code queryEmbedding} array is defensively copied
 * on construction.</p>
 */
public record SemanticCacheEntry(
        String originalSql,
        float[] queryEmbedding,
        String tenantId,
        String principal,
        Set<String> referencedTables,
        byte[] cachedResult,
        Instant cachedAt) {

    public SemanticCacheEntry {
        if (originalSql == null || originalSql.isBlank()) {
            throw new IllegalArgumentException("originalSql required");
        }
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            throw new IllegalArgumentException("queryEmbedding required");
        }
        queryEmbedding = queryEmbedding.clone();
        referencedTables = referencedTables != null ? Set.copyOf(referencedTables) : Set.of();
        if (cachedAt == null) {
            throw new IllegalArgumentException("cachedAt required");
        }
    }
}
