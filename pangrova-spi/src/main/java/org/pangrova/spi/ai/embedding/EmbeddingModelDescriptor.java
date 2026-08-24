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

import org.pangrova.spi.annotation.Experimental;

/**
 * Immutable descriptor for a registered embedding model configuration.
 *
 * <p>Stored in the catalog's embedding model registry. The {@code apiKeySecret}
 * field holds an opaque reference resolved at runtime via {@link SecretResolver};
 * actual credentials are never persisted in catalog metadata.</p>
 *
 * @param name                 unique model name (must not be null or blank)
 * @param provider             provider type identifier (e.g., "openai-compatible"; must not be null or blank)
 * @param endpoint             model service endpoint URL (must not be null or blank)
 * @param dimension            output vector dimensionality (must be positive)
 * @param apiKeySecret         opaque secret reference for authentication (must not be null or blank)
 * @param maxBatchSize         maximum inputs per batch request (must be positive)
 * @param maxRequestsPerSecond rate limit for embedding requests (must be positive)
 * @param queryPrefix          prefix prepended to query-time text (may be empty, must not be null)
 * @param documentPrefix       prefix prepended to document-time text (may be empty, must not be null)
 * @param version              configuration version, incremented on ALTER (must be non-negative)
 */
@Experimental
public record EmbeddingModelDescriptor(
        String name,
        String provider,
        String endpoint,
        int dimension,
        String apiKeySecret,
        int maxBatchSize,
        int maxRequestsPerSecond,
        String queryPrefix,
        String documentPrefix,
        int version) {

    public EmbeddingModelDescriptor {
        requireText(name, "name");
        requireText(provider, "provider");
        requireText(endpoint, "endpoint");
        requirePositive(dimension, "dimension");
        requireText(apiKeySecret, "apiKeySecret");
        requirePositive(maxBatchSize, "maxBatchSize");
        requirePositive(maxRequestsPerSecond, "maxRequestsPerSecond");
        requireNonNull(queryPrefix, "queryPrefix");
        requireNonNull(documentPrefix, "documentPrefix");
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative");
        }
    }

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " required");
        }
    }

    private static void requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static void requireNonNull(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
    }
}
