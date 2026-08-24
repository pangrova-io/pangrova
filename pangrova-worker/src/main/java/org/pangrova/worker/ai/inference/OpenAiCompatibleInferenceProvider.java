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
package org.pangrova.worker.ai.inference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.ai.inference.ContentPart;
import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.ai.inference.Modality;
import org.pangrova.spi.ai.inference.MultimodalPrompt;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Inference provider that calls any OpenAI-compatible /v1/chat/completions endpoint.
 *
 * <p>Handles rate limiting via token bucket, exponential backoff with jitter on 429/503,
 * configurable timeout, and batch splitting. Reuses the same HTTP client
 * and retry strategy as {@code OpenAiCompatibleEmbeddingProvider}.</p>
 *
 * <p>Thread-safe: uses java.net.http.HttpClient with no mutable shared state
 * beyond the atomic token bucket.</p>
 */
public final class OpenAiCompatibleInferenceProvider implements InferenceProvider, Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final int HTTP_OK = 200;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private final String endpoint;
    private final String modelName;
    private final String apiKey;
    private final int maxBatchSize;
    private final Duration timeout;
    private final HttpClient httpClient;
    private final Semaphore rateLimiter;
    private final ScheduledExecutorService rateReplenisher;
    private final ExecutorService batchExecutor;

    public OpenAiCompatibleInferenceProvider(
            String endpoint,
            String modelName,
            String apiKey,
            int maxBatchSize,
            double maxRequestsPerSecond,
            int timeoutSeconds) {
        this(endpoint, modelName, apiKey, maxBatchSize, maxRequestsPerSecond,
                timeoutSeconds,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build());
    }

    OpenAiCompatibleInferenceProvider(
            String endpoint,
            String modelName,
            String apiKey,
            int maxBatchSize,
            double maxRequestsPerSecond,
            int timeoutSeconds,
            HttpClient httpClient) {
        requireText(endpoint, "endpoint");
        requireText(modelName, "modelName");
        requireText(apiKey, "apiKey");
        validatePositiveValues(maxBatchSize, maxRequestsPerSecond, timeoutSeconds);
        this.endpoint = endpoint;
        this.modelName = modelName;
        this.apiKey = apiKey;
        this.maxBatchSize = maxBatchSize;
        this.timeout = Duration.ofSeconds(timeoutSeconds);
        this.httpClient = httpClient;

        int permits = Math.max(1, (int) maxRequestsPerSecond);
        this.rateLimiter = new Semaphore(permits, true);
        long intervalMs = 1000 / Math.max(1, (long) maxRequestsPerSecond);
        this.rateReplenisher = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                "ai-inference-rate-replenish");
        this.rateReplenisher.scheduleAtFixedRate(
                () -> {
                    int drain = permits - rateLimiter.availablePermits();
                    if (drain > 0) {
                        rateLimiter.release(Math.min(drain, permits));
                    }
                },
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        this.batchExecutor = ManagedExecutorFactory.newFixedThreadPool(
                "ai-inference-batch",
                Math.min(maxBatchSize, Runtime.getRuntime().availableProcessors()));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " required");
        }
    }

    private static void validatePositiveValues(int maxBatchSize, double maxRequestsPerSecond,
                                               int timeoutSeconds) {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        if (maxRequestsPerSecond <= 0) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be positive");
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
        return callWithRetry(prompt, parameters);
    }

    @Override
    public List<InferenceResult> completeBatch(List<String> prompts, GenerationParameters parameters) {
        if (prompts == null || prompts.isEmpty()) {
            throw new IllegalArgumentException("prompts must not be null or empty");
        }
        if (prompts.size() > maxBatchSize) {
            throw new IllegalArgumentException(
                    "batch size " + prompts.size() + " exceeds maxBatchSize " + maxBatchSize);
        }
        if (parameters == null) {
            parameters = GenerationParameters.DEFAULTS;
        }
        GenerationParameters params = parameters;
        List<CompletableFuture<InferenceResult>> futures = prompts.stream()
                .map(p -> CompletableFuture.supplyAsync(
                        () -> callWithRetry(p, params), batchExecutor))
                .toList();
        return futures.stream().map(CompletableFuture::join).toList();
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
    public InferenceResult completeMultimodal(MultimodalPrompt prompt, GenerationParameters parameters) {
        if (prompt == null) {
            throw new IllegalArgumentException("prompt must not be null");
        }
        if (parameters == null) {
            parameters = GenerationParameters.DEFAULTS;
        }
        String requestBody = buildMultimodalRequestJson(prompt, parameters);
        return callWithRetryRaw(requestBody, parameters);
    }

    @Override
    public Set<Modality> supportedModalities() {
        return Set.of(Modality.TEXT, Modality.IMAGE, Modality.DOCUMENT);
    }

    @Override
    public void close() {
        rateReplenisher.shutdown();
        batchExecutor.shutdown();
    }

    private InferenceResult callWithRetry(String prompt, GenerationParameters parameters) {
        String requestBody = buildRequestJson(prompt, parameters);
        return callWithRetryRaw(requestBody, parameters);
    }

    private InferenceResult callWithRetryRaw(String requestBody, GenerationParameters parameters) {
        long backoffMs = INITIAL_BACKOFF_MS;
        Exception lastException = null;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                long jitter = ThreadLocalRandom.current().nextLong(backoffMs / 2);
                sleep(backoffMs + jitter);
                backoffMs *= 2;
            }
            acquireRatePermit();

            try {
                return attemptOnce(requestBody, attempt);
            } catch (RetryableStatusException e) {
                lastException = e;
            } catch (IOException e) {
                lastException = e;
                if (attempt >= MAX_RETRIES) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Inference request interrupted", e);
            }
        }
        throw new RuntimeException(
                "Inference request failed after " + (MAX_RETRIES + 1) + " attempts",
                lastException);
    }

    private InferenceResult attemptOnce(String requestBody, int attempt)
            throws IOException, InterruptedException {
        HttpResponse<String> response = sendRequest(requestBody);
        int statusCode = response.statusCode();

        if (statusCode == HTTP_OK) {
            return parseResponseWithUsage(response.body());
        }
        if (isRetryable(statusCode) && attempt < MAX_RETRIES) {
            throw new RetryableStatusException(statusCode);
        }
        throw new RuntimeException(
                "Inference request failed with HTTP " + statusCode
                        + ": " + response.body());
    }

    private HttpResponse<String> sendRequest(String requestBody) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static boolean isRetryable(int statusCode) {
        return statusCode == HTTP_TOO_MANY_REQUESTS || statusCode == HTTP_SERVICE_UNAVAILABLE;
    }

    private void acquireRatePermit() {
        try {
            if (!rateLimiter.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("Rate limit timeout: unable to acquire permit");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for rate limit permit", e);
        }
    }

    private String buildRequestJson(String prompt, GenerationParameters parameters) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", modelName);
        ArrayNode messages = root.putArray("messages");
        messages.addObject().put("role", "user").put("content", prompt);

        applyGenerationParameters(root, parameters);

        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize inference request", e);
        }
    }

    private String buildMultimodalRequestJson(MultimodalPrompt prompt, GenerationParameters parameters) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", modelName);
        ArrayNode messages = root.putArray("messages");

        ObjectNode userMsg = messages.addObject();
        userMsg.put("role", "user");
        ArrayNode contentArray = userMsg.putArray("content");

        for (ContentPart part : prompt.parts()) {
            if (part instanceof ContentPart.Text textPart) {
                contentArray.addObject()
                        .put("type", "text")
                        .put("text", textPart.text());
            } else if (part instanceof ContentPart.InlineImage imagePart) {
                ObjectNode imageNode = contentArray.addObject();
                imageNode.put("type", "image_url");
                ObjectNode imageUrl = imageNode.putObject("image_url");
                String base64Data = Base64.getEncoder().encodeToString(imagePart.data());
                imageUrl.put("url", "data:" + imagePart.mimeType() + ";base64," + base64Data);
            } else if (part instanceof ContentPart.FileRef filePart) {
                // For file references, pass the URI as an image_url for Vision API
                // The provider endpoint handles URI resolution (presigned URLs, etc.)
                ObjectNode imageNode = contentArray.addObject();
                imageNode.put("type", "image_url");
                ObjectNode imageUrl = imageNode.putObject("image_url");
                imageUrl.put("url", filePart.reference().uri());
            }
        }

        applyGenerationParameters(root, parameters);

        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize multimodal inference request", e);
        }
    }

    private void applyGenerationParameters(ObjectNode root, GenerationParameters parameters) {
        parameters.temperature().ifPresent(t -> root.put("temperature", t));
        parameters.maxTokens().ifPresent(m -> root.put("max_tokens", m));
        parameters.responseFormat().ifPresent(fmt -> {
            ObjectNode formatNode = MAPPER.createObjectNode();
            formatNode.put("type", fmt);
            root.set("response_format", formatNode);
        });
    }

    private InferenceResult parseResponseWithUsage(String responseBody) {
        try {
            JsonNode root = MAPPER.readTree(responseBody);
            JsonNode choices = root.path("choices");
            if (!choices.isArray() || choices.isEmpty()) {
                throw new RuntimeException("Invalid response: missing 'choices' array");
            }
            JsonNode content = choices.get(0).path("message").path("content");
            if (content.isMissingNode() || !content.isTextual()) {
                throw new RuntimeException("Invalid response: missing 'content' field");
            }
            String text = content.asText();

            JsonNode usage = root.path("usage");
            if (!usage.isMissingNode()) {
                long inputTokens = usage.path("prompt_tokens").asLong(0);
                long outputTokens = usage.path("completion_tokens").asLong(0);
                return InferenceResult.of(text, inputTokens, outputTokens);
            }
            return InferenceResult.of(text);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Invalid response: failed to parse JSON", e);
        }
    }

    private static void sleep(long millis) {
        ThreadUtils.sleepQuietly(Math.max(1, millis));
        if (Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("Interrupted during backoff");
        }
    }

    /** Signals a retryable HTTP status so the retry loop can distinguish it from I/O errors. */
    private static final class RetryableStatusException extends IOException {
        RetryableStatusException(int statusCode) {
            super("HTTP " + statusCode + " from inference endpoint");
        }
    }
}
