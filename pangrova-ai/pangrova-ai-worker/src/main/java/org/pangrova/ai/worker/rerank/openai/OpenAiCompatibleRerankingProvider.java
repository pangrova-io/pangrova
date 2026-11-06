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
package org.pangrova.ai.worker.rerank.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.worker.context.AiHttpTransport;
import org.pangrova.ai.api.invocation.RerankingBatchResult;
import org.pangrova.ai.spi.reranking.RerankingProvider;
import org.pangrova.ai.worker.context.AiProviderCallContext;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** OpenAI-compatible {@code /v1/rerank} provider with ordered result validation. */
public final class OpenAiCompatibleRerankingProvider implements RerankingProvider, Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int HTTP_OK = 200;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    private final URI endpoint;
    private final String modelName;
    private final String apiKey;
    private final int maxBatchSize;
    private final Duration timeout;
    private final HttpClient httpClient;
    private final Semaphore rateLimiter;
    private final ScheduledExecutorService rateReplenisher;

    /** Creates a reranking provider using the configured endpoint and limits. */
    public OpenAiCompatibleRerankingProvider(
            URI endpoint,
            String modelName,
            String apiKey,
            int maxBatchSize,
            double maxRequestsPerSecond,
            int timeoutSeconds) {
        this(
                endpoint,
                modelName,
                apiKey,
                maxBatchSize,
                maxRequestsPerSecond,
                timeoutSeconds,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build());
    }

    OpenAiCompatibleRerankingProvider(
            URI endpoint,
            String modelName,
            String apiKey,
            int maxBatchSize,
            double maxRequestsPerSecond,
            int timeoutSeconds,
            HttpClient httpClient) {
        this.endpoint = java.util.Objects.requireNonNull(endpoint, "endpoint required");
        requireText(modelName, "modelName");
        if (apiKey == null) {
            throw new IllegalArgumentException("apiKey must not be null");
        }
        if (maxBatchSize <= 0
                || !Double.isFinite(maxRequestsPerSecond)
                || maxRequestsPerSecond <= 0
                || timeoutSeconds <= 0) {
            throw new IllegalArgumentException("reranking limits must be positive and finite");
        }
        this.modelName = modelName;
        this.apiKey = apiKey;
        this.maxBatchSize = maxBatchSize;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.httpClient = java.util.Objects.requireNonNull(httpClient, "httpClient required");
        int permits = Math.max(1, (int) Math.ceil(maxRequestsPerSecond));
        this.rateLimiter = new Semaphore(permits, true);
        this.rateReplenisher =
                ManagedExecutorFactory.newSingleThreadScheduledExecutor("ai-rerank-rate-replenish");
        long intervalNanos = Math.max(1L,
                (long) Math.ceil((double) TimeUnit.SECONDS.toNanos(1) / maxRequestsPerSecond));
        rateReplenisher.scheduleAtFixedRate(
                () -> {
                    int missing = permits - rateLimiter.availablePermits();
                    if (missing > 0) {
                        rateLimiter.release();
                    }
                },
                intervalNanos,
                intervalNanos,
                TimeUnit.NANOSECONDS);
    }

    @Override
    public RerankingBatchResult score(String query, List<String> documents) {
        requireText(query, "query");
        requireDocuments(documents);
        String body = requestBody(query, documents);
        acquirePermit();
        try {
            HttpResponse<String> response = send(body);
            if (response.statusCode() == HTTP_OK) {
                return parseResponse(response, documents.size());
            }
            throw responseFailure(response.statusCode());
        } catch (IOException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "Reranking provider POST outcome is unknown",
                    failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "Reranking provider POST was interrupted with unknown outcome",
                    failure);
        }
    }

    private void requireDocuments(List<String> documents) {
        if (documents == null
                || documents.isEmpty()
                || documents.size() > maxBatchSize
                || documents.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException(
                    "documents must contain 1.." + maxBatchSize + " non-null values");
        }
    }

    private HttpResponse<String> send(String body) throws IOException, InterruptedException {
        HttpRequest.Builder request =
                HttpRequest.newBuilder(endpoint)
                        .timeout(AiProviderCallContext.remainingTimeout(timeout))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body));
        if (!apiKey.isBlank()) {
            request.header("Authorization", "Bearer " + apiKey);
        }
        AiProviderCallContext.currentIdempotencyKey()
                .ifPresent(value -> request.header("Idempotency-Key", value));
        return AiHttpTransport.send(httpClient, request.build());
    }

    private static AiInvocationException responseFailure(int statusCode) {
        AiInvocationException.Category category =
                statusCode == HTTP_TOO_MANY_REQUESTS
                        ? AiInvocationException.Category.RATE_LIMIT
                        : AiInvocationException.Category.PROVIDER_UNAVAILABLE;
        return new AiInvocationException(
                category, false, "Reranking provider returned HTTP " + statusCode);
    }

    private String requestBody(String query, List<String> documents) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", modelName);
        root.put("query", query);
        root.put("top_n", documents.size());
        root.put("return_documents", false);
        ArrayNode values = root.putArray("documents");
        documents.forEach(values::add);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException impossible) {
            throw new IllegalStateException("Failed to serialize reranking request", impossible);
        }
    }

    private static RerankingBatchResult parseResponse(
            HttpResponse<String> response, int expectedCount) {
        try {
            JsonNode root = MAPPER.readTree(response.body());
            if (root == null || !root.isObject()) {
                throw invalidResponse("reranking response must be a JSON object");
            }
            JsonNode results = root.path("results");
            if (!results.isArray() || results.size() != expectedCount) {
                throw invalidResponse("expected " + expectedCount + " reranking results");
            }
            Double[] scores = parseScores(results, expectedCount);
            String providerRequestId =
                    root.path("id")
                            .asText(response.headers().firstValue("x-request-id").orElse(""));
            return new RerankingBatchResult(
                    new ArrayList<>(Arrays.asList(scores)), providerRequestId);
        } catch (JsonProcessingException failure) {
            throw invalidResponse("reranking response is not valid JSON", failure);
        }
    }

    private static Double[] parseScores(JsonNode results, int expectedCount) {
        Double[] scores = new Double[expectedCount];
        for (JsonNode result : results) {
            JsonNode indexNode = result.path("index");
            if (!indexNode.isIntegralNumber() || !indexNode.canConvertToInt()) {
                throw invalidResponse("reranking index must be an integral value");
            }
            int index = indexNode.intValue();
            JsonNode score =
                    result.has("relevance_score")
                            ? result.get("relevance_score")
                            : result.get("score");
            requireValidScore(index, score, scores);
            scores[index] = score.doubleValue();
        }
        if (Arrays.stream(scores).anyMatch(java.util.Objects::isNull)) {
            throw invalidResponse("reranking results omitted an input index");
        }
        return scores;
    }

    private static void requireValidScore(int index, JsonNode score, Double[] scores) {
        if (index < 0
                || index >= scores.length
                || scores[index] != null
                || score == null
                || !score.isNumber()
                || !Double.isFinite(score.doubleValue())) {
            throw invalidResponse("reranking result indexes/scores are invalid");
        }
    }

    private void acquirePermit() {
        try {
            long waitMillis = AiProviderCallContext.remainingTimeout(timeout).toMillis();
            if (!rateLimiter.tryAcquire(Math.max(1L, waitMillis), TimeUnit.MILLISECONDS)) {
                throw new AiInvocationException(
                        AiInvocationException.Category.RATE_LIMIT,
                        true,
                        "Timed out waiting for reranking provider admission");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    false,
                    "Reranking provider admission was interrupted",
                    failure);
        }
    }

    private static AiInvocationException invalidResponse(String message) {
        return invalidResponse(message, null);
    }

    private static AiInvocationException invalidResponse(String message, Throwable cause) {
        return new AiInvocationException(
                AiInvocationException.Category.PROVIDER_RESPONSE_INVALID, false, message, cause);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    @Override
    public String modelName() {
        return modelName;
    }

    @Override
    public int maxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public void close() {
        rateReplenisher.shutdownNow();
    }
}
