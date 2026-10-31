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
package org.pangrova.ai.worker.inference.provider.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.ai.api.invocation.InferenceResult;
import org.pangrova.ai.api.capability.Modality;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.ai.worker.context.AiProviderCallContext;
import org.pangrova.ai.worker.context.AiHttpTransport;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Inference provider that calls any OpenAI-compatible /v1/chat/completions endpoint.
 *
 * <p>Handles local rate limiting, configurable timeout, and batch splitting. A provider POST is
 * sent once unless a separately verified provider idempotency contract permits replay.
 *
 * <p>Thread-safe: uses java.net.http.HttpClient with no mutable shared state beyond the atomic
 * token bucket.
 */
public final class OpenAiCompatibleInferenceProvider implements InferenceProvider, Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int HTTP_OK = 200;
    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_REQUEST_TIMEOUT = 408;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_GATEWAY_TIMEOUT = 504;

    private final String endpoint;
    private final String modelName;
    private final String apiKey;
    private final int maxBatchSize;
    private final Duration timeout;
    private final OpenAiChatRequestEncoder requestEncoder;
    private final HttpClient httpClient;
    private final Semaphore rateLimiter;
    private final ScheduledExecutorService rateReplenisher;
    private final ExecutorService batchExecutor;

    /** Creates a provider using the configured endpoint and admission limits. */
    public OpenAiCompatibleInferenceProvider(
            OpenAiInferenceConfig config, Optional<Function<BlobReference, byte[]>> blobLoader) {
        java.util.Objects.requireNonNull(config, "config must not be null");
        requireText(config.endpoint(), "endpoint");
        requireText(config.modelName(), "modelName");
        if (config.apiKey() == null) {
            throw new IllegalArgumentException("apiKey must not be null");
        }
        validatePositiveValues(
                config.maxBatchSize(), config.maxRequestsPerSecond(),
                config.maxConcurrency(), config.timeoutSeconds());
        this.endpoint = config.endpoint();
        this.modelName = config.modelName();
        this.apiKey = config.apiKey();
        this.maxBatchSize = config.maxBatchSize();
        this.timeout = Duration.ofSeconds(config.timeoutSeconds());
        this.requestEncoder = new OpenAiChatRequestEncoder(modelName, blobLoader);
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        int permits = Math.max(1, (int) Math.ceil(config.maxRequestsPerSecond()));
        this.rateLimiter = new Semaphore(permits, true);
        long intervalNanos =
                Math.max(1L, (long) Math.ceil((double) TimeUnit.SECONDS.toNanos(1) / config.maxRequestsPerSecond()));
        this.rateReplenisher =
                ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                        "ai-inference-rate-replenish");
        this.rateReplenisher.scheduleAtFixedRate(
                () -> {
                    int drain = permits - rateLimiter.availablePermits();
                    if (drain > 0) {
                        rateLimiter.release();
                    }
                },
                intervalNanos,
                intervalNanos,
                TimeUnit.NANOSECONDS);
        this.batchExecutor =
                ManagedExecutorFactory.newFixedThreadPool(
                        "ai-inference-batch",
                        Math.min(config.maxBatchSize(), config.maxConcurrency()));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " required");
        }
    }

    private static void validatePositiveValues(
            int maxBatchSize, double maxRequestsPerSecond, int maxConcurrency, int timeoutSeconds) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        if (!Double.isFinite(maxRequestsPerSecond) || maxRequestsPerSecond <= 0) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be positive");
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("timeoutSeconds must be positive");
        }
    }

    @Override
    public InferenceResult complete(String prompt, GenerationParameters parameters) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt must not be null");
        }
        if (parameters == null) {
            parameters = GenerationParameters.DEFAULTS;
        }
        return callOnce(prompt, parameters);
    }

    @Override
    public List<InferenceResult> completeBatch(
            List<String> prompts, GenerationParameters parameters) {
        if (prompts == null
                || prompts.isEmpty()
                || prompts.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException(
                    "prompts must not be null or empty and must contain only non-null values");
        }
        if (prompts.size() > maxBatchSize) {
            throw new IllegalArgumentException(
                    "batch size " + prompts.size() + " exceeds maxBatchSize " + maxBatchSize);
        }
        if (parameters == null) {
            parameters = GenerationParameters.DEFAULTS;
        }
        GenerationParameters params = parameters;
        String invocationKey = AiProviderCallContext.currentIdempotencyKey().orElse(null);
        Instant invocationDeadline = AiProviderCallContext.currentDeadline().orElse(null);
        AiProviderCallContext.AttemptAdmission attemptAdmission =
                AiProviderCallContext.currentAttemptAdmission().orElse(null);
        return InferenceBatch.execute(prompts.size(), batchExecutor, index -> invocationKey == null
                ? callOnce(prompts.get(index), params)
                : AiProviderCallContext.callWithOrdinal(invocationKey, invocationDeadline, index,
                        attemptAdmission, () -> callOnce(prompts.get(index), params)));
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
    public InferenceResult completeMultimodal(
            MultimodalPrompt prompt, GenerationParameters parameters) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt must not be null");
        }
        if (parameters == null) {
            parameters = GenerationParameters.DEFAULTS;
        }
        String requestBody = requestEncoder.encodeMultimodal(prompt, parameters);
        return callOnce(requestBody);
    }

    @Override
    public Set<Modality> supportedModalities() {
        return requestEncoder.supportsImages()
                ? Set.of(Modality.TEXT, Modality.IMAGE)
                : Set.of(Modality.TEXT);
    }

    @Override
    public void close() {
        rateReplenisher.shutdown();
        batchExecutor.shutdown();
    }

    private InferenceResult callOnce(String prompt, GenerationParameters parameters) {
        String requestBody = requestEncoder.encodeText(prompt, parameters);
        return callOnce(requestBody);
    }

    private InferenceResult callOnce(String requestBody) {
        acquireRatePermit();
        try {
            return attemptOnce(requestBody);
        } catch (IOException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "Inference provider POST outcome is unknown",
                    failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "Inference provider POST was interrupted with unknown outcome",
                    failure);
        }
    }

    private InferenceResult attemptOnce(String requestBody)
            throws IOException, InterruptedException {
        HttpResponse<String> response = sendRequest(requestBody);
        int statusCode = response.statusCode();

        if (statusCode == HTTP_OK) {
            return parseResponse(response.body());
        }
        throw responseFailure(statusCode);
    }

    private HttpResponse<String> sendRequest(String requestBody)
            throws IOException, InterruptedException {
        HttpRequest.Builder request =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .timeout(AiProviderCallContext.remainingTimeout(timeout))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody));
        if (!apiKey.isBlank()) {
            request.header("Authorization", "Bearer " + apiKey);
        }
        AiProviderCallContext.currentIdempotencyKey()
                .ifPresent(value -> request.header("Idempotency-Key", value));
        return AiHttpTransport.send(httpClient, request.build());
    }

    private void acquireRatePermit() {
        try {
            long waitMillis = AiProviderCallContext.remainingTimeout(timeout).toMillis();
            if (!rateLimiter.tryAcquire(Math.max(1L, waitMillis), TimeUnit.MILLISECONDS)) {
                throw new AiInvocationException(
                        AiInvocationException.Category.RATE_LIMIT,
                        true,
                        "Timed out waiting for inference provider admission");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    false,
                    "Inference provider admission was interrupted",
                    e);
        }
    }

    private InferenceResult parseResponse(String responseBody) {
        try {
            JsonNode root = MAPPER.readTree(responseBody);
            if (root == null || !root.isObject()) {
                throw invalidResponse("inference response must be a JSON object");
            }
            String text = requireResponseText(root);
            return new InferenceResult(text, root.path("id").asText(""));
        } catch (JsonProcessingException e) {
            throw invalidResponse("inference response is not valid JSON", e);
        }
    }

    private static String requireResponseText(JsonNode root) {
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            throw invalidResponse("inference response is missing its choices array");
        }
        JsonNode content = choices.get(0).path("message").path("content");
        if (content.isMissingNode() || !content.isTextual()) {
            throw invalidResponse("inference response is missing its text content");
        }
        return content.asText();
    }

    private static AiInvocationException responseFailure(int statusCode) {
        AiInvocationException.Category category = switch (statusCode) {
            case HTTP_UNAUTHORIZED -> AiInvocationException.Category.AUTHENTICATION;
            case HTTP_FORBIDDEN -> AiInvocationException.Category.AUTHORIZATION;
            case HTTP_REQUEST_TIMEOUT, HTTP_GATEWAY_TIMEOUT ->
                            AiInvocationException.Category.TIMEOUT;
            case HTTP_TOO_MANY_REQUESTS -> AiInvocationException.Category.RATE_LIMIT;
            default -> AiInvocationException.Category.PROVIDER_UNAVAILABLE;
        };
        return new AiInvocationException(
                category, false, "Inference provider returned HTTP " + statusCode);
    }

    private static AiInvocationException invalidResponse(String message) {
        return invalidResponse(message, null);
    }

    private static AiInvocationException invalidResponse(String message, Throwable cause) {
        return new AiInvocationException(
                AiInvocationException.Category.PROVIDER_RESPONSE_INVALID, false, message, cause);
    }
}
