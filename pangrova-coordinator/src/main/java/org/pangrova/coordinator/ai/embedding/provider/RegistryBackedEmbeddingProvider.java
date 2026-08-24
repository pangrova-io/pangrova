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
package org.pangrova.coordinator.ai.embedding.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.pangrova.common.annotation.Experimental;
import org.pangrova.coordinator.ai.embedding.EmbeddingModelNotReadyException;
import org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.security.UserIdentity;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coordinator-side embedding provider that populates the semantic query cache.
 *
 * <p>The catalog's {@link EmbeddingModelRegistry} is resolved lazily on every call
 * so that models registered at runtime via {@code CREATE MODEL} activate the
 * semantic cache without a coordinator restart. The "default" model is the single
 * registered model when exactly one exists; when none (or more than one, with no
 * configured default) is present the provider raises
 * {@link EmbeddingModelNotReadyException} so best-effort callers skip caching
 * without polluting the cache with zero vectors.</p>
 *
 * <p>This is deliberately separate from the worker-side OpenAI-compatible provider:
 * the coordinator module cannot depend on {@code pangrova-worker} (only its transport
 * package), and the semantic-cache path needs only text embedding, not the worker's
 * batching/blob/rate-limit machinery. The on-the-wire request/response format is kept
 * compatible with any OpenAI-style {@code /v1/embeddings} endpoint.</p>
 */
@Experimental(trackingIssue = "PANGROVA-P1-1-semantic-cache-embedding")
public final class RegistryBackedEmbeddingProvider implements EmbeddingProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int HTTP_STATUS_OK = 200;

    // Privileged identity used only to read model configuration from the registry.
    // Carries both admin and usage groups so it passes RBAC regardless of how the
    // registry's group names were configured (createEmbeddingModelRegistry uses
    // admin="admin", usage="users").
    private static final UserIdentity SYSTEM_IDENTITY = new UserIdentity(
            "pangrova-system", Set.of("admin", "users"), Map.of(), Instant.EPOCH);

    private final EmbeddingModelRegistry registry;
    private final SecretResolver secretResolver;
    private final HttpClient httpClient;
    private final Duration timeout;

    public RegistryBackedEmbeddingProvider(EmbeddingModelRegistry registry, SecretResolver secretResolver) {
        this(registry, secretResolver,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(),
                DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * Constructor with HttpClient injection for testing.
     */
    RegistryBackedEmbeddingProvider(EmbeddingModelRegistry registry, SecretResolver secretResolver,
                                    HttpClient httpClient, int timeoutSeconds) {
        if (registry == null) {
            throw new IllegalArgumentException("registry required");
        }
        if (secretResolver == null) {
            throw new IllegalArgumentException("secretResolver required");
        }
        if (httpClient == null) {
            throw new IllegalArgumentException("httpClient required");
        }
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("timeoutSeconds must be positive");
        }
        this.registry = registry;
        this.secretResolver = secretResolver;
        this.httpClient = httpClient;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
    }

    @Override
    public float[] embed(String text) {
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
        EmbeddingModelDescriptor model = resolveDefaultModel();
        return call(model, List.of(model.queryPrefix() + text)).get(0);
    }

    @Override
    public List<float[]> embedBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            throw new IllegalArgumentException("texts must not be null or empty");
        }
        EmbeddingModelDescriptor model = resolveDefaultModel();
        if (texts.size() > model.maxBatchSize()) {
            throw new IllegalArgumentException(
                    "batch size " + texts.size() + " exceeds maxBatchSize " + model.maxBatchSize());
        }
        List<String> prefixed = new ArrayList<>(texts.size());
        for (String text : texts) {
            prefixed.add(model.documentPrefix() + text);
        }
        return call(model, prefixed);
    }

    @Override
    public List<float[]> embedBlob(List<BlobReference> refs) {
        throw new UnsupportedOperationException(
                "blob embedding is not supported by the coordinator semantic-cache provider; "
                        + "blob embedding runs worker-side");
    }

    @Override
    public int dimensions() {
        return resolveDefaultModel().dimension();
    }

    @Override
    public String modelName() {
        return resolveDefaultModel().name();
    }

    @Override
    public int maxBatchSize() {
        return resolveDefaultModel().maxBatchSize();
    }

    private EmbeddingModelDescriptor resolveDefaultModel() {
        List<EmbeddingModelDescriptor> models = registry.listAll(SYSTEM_IDENTITY);
        if (models.isEmpty()) {
            throw new EmbeddingModelNotReadyException("no embedding model registered");
        }
        if (models.size() > 1) {
            throw new EmbeddingModelNotReadyException(
                    "multiple embedding models registered (" + models.size()
                            + ") with no configured default; register exactly one to enable semantic cache");
        }
        return models.get(0);
    }

    private List<float[]> call(EmbeddingModelDescriptor model, List<String> inputs) {
        String apiKey = secretResolver.resolve(model.apiKeySecret());
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("resolved API key is empty for model: " + model.name());
        }
        String requestBody = buildRequestJson(model.name(), inputs);
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(model.endpoint()))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HTTP_STATUS_OK) {
                throw new IllegalStateException("embedding request failed with HTTP "
                        + response.statusCode() + ": " + response.body());
            }
            return parseResponse(response.body(), inputs.size(), model.dimension());
        } catch (java.io.IOException e) {
            throw new IllegalStateException("embedding request failed for model: " + model.name(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("embedding request interrupted for model: " + model.name(), e);
        }
    }

    private static String buildRequestJson(String modelName, List<String> inputs) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode inputArray = root.putArray("input");
        for (String input : inputs) {
            inputArray.add(input);
        }
        root.put("model", modelName);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize embedding request", e);
        }
    }

    private static List<float[]> parseResponse(String responseBody, int expectedCount, int dimension) {
        try {
            JsonNode data = MAPPER.readTree(responseBody).path("data");
            validateDataArray(data, expectedCount);
            List<float[]> results = new ArrayList<>(expectedCount);
            for (int i = 0; i < expectedCount; i++) {
                results.add(parseSingleEmbedding(data.get(i).path("embedding"), i, dimension));
            }
            return results;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("invalid embedding response: failed to parse JSON", e);
        }
    }

    private static void validateDataArray(JsonNode data, int expectedCount) {
        if (!data.isArray() || data.size() < expectedCount) {
            throw new IllegalStateException("invalid embedding response: expected " + expectedCount
                    + " embeddings, found " + (data.isArray() ? data.size() : 0));
        }
    }

    private static float[] parseSingleEmbedding(JsonNode embeddingNode, int index, int dimension) {
        if (!embeddingNode.isArray() || embeddingNode.size() != dimension) {
            throw new IllegalStateException("invalid embedding at index " + index
                    + ": expected dimension " + dimension
                    + ", got " + (embeddingNode.isArray() ? embeddingNode.size() : -1));
        }
        float[] embedding = new float[dimension];
        for (int d = 0; d < dimension; d++) {
            embedding[d] = (float) embeddingNode.get(d).asDouble();
        }
        return embedding;
    }
}
