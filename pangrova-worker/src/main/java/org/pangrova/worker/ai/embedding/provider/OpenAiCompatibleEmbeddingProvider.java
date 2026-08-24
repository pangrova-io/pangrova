package org.pangrova.worker.ai.embedding.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Embedding provider that calls any OpenAI-compatible /v1/embeddings endpoint.
 *
 * <p>Handles rate limiting via token bucket, exponential backoff on 429/503,
 * configurable timeout, and batch splitting for large input sets.</p>
 *
 * <p>Thread-safe: uses java.net.http.HttpClient with no mutable shared state
 * beyond the atomic token bucket.</p>
 */
public final class OpenAiCompatibleEmbeddingProvider implements EmbeddingProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final int HTTP_OK = 200;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final long NANOS_PER_MILLI = 1_000_000;

    private final String endpoint;
    private final String modelName;
    private final String apiKey;
    private final int dimension;
    private final int maxBatchSize;
    private final String queryPrefix;
    private final String documentPrefix;
    private final Duration timeout;
    private final HttpClient httpClient;

    // Token bucket for rate limiting
    private final AtomicLong lastRequestNanos;
    private final long minIntervalNanos;

    /**
     * Creates a provider with a default HTTP client.
     *
     * @param config endpoint, model and tuning configuration
     */
    public OpenAiCompatibleEmbeddingProvider(OpenAiEmbeddingConfig config) {
        this(config, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build());
    }

    /**
     * Constructor with HttpClient injection for testing.
     *
     * @param config     endpoint, model and tuning configuration
     * @param httpClient HTTP client used for endpoint calls
     */
    OpenAiCompatibleEmbeddingProvider(OpenAiEmbeddingConfig config, HttpClient httpClient) {
        if (config == null) {
            throw new IllegalArgumentException("config required");
        }
        requireText(config.endpoint(), "endpoint");
        requireText(config.modelName(), "modelName");
        requireText(config.apiKey(), "apiKey");
        validatePositiveValues(config);
        if (httpClient == null) {
            throw new IllegalArgumentException("httpClient required");
        }

        this.endpoint = config.endpoint();
        this.modelName = config.modelName();
        this.apiKey = config.apiKey();
        this.dimension = config.dimension();
        this.maxBatchSize = config.maxBatchSize();
        this.queryPrefix = config.queryPrefix() != null ? config.queryPrefix() : "";
        this.documentPrefix = config.documentPrefix() != null ? config.documentPrefix() : "";
        this.timeout = Duration.ofSeconds(config.timeoutSeconds());
        this.httpClient = httpClient;
        this.lastRequestNanos = new AtomicLong(0);
        this.minIntervalNanos = (long) (NANOS_PER_SECOND / config.maxRequestsPerSecond());
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " required");
        }
    }

    private static void validatePositiveValues(OpenAiEmbeddingConfig config) {
        if (config.dimension() <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (config.maxBatchSize() <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        if (config.maxRequestsPerSecond() <= 0) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be positive");
        }
        if (config.timeoutSeconds() <= 0) {
            throw new IllegalArgumentException("timeoutSeconds must be positive");
        }
    }

    @Override
    public float[] embed(String text) {
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
        String prefixed = queryPrefix + text;
        List<float[]> results = callEmbeddingEndpoint(List.of(prefixed));
        return results.get(0);
    }

    @Override
    public List<float[]> embedBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) {
            throw new IllegalArgumentException("texts must not be null or empty");
        }
        if (texts.size() > maxBatchSize) {
            throw new IllegalArgumentException(
                    "batch size " + texts.size() + " exceeds maxBatchSize " + maxBatchSize);
        }
        List<String> prefixed = new ArrayList<>(texts.size());
        for (String text : texts) {
            prefixed.add(documentPrefix + text);
        }
        return callEmbeddingEndpoint(prefixed);
    }

    @Override
    public List<float[]> embedBlob(List<BlobReference> refs) {
        if (refs == null || refs.isEmpty()) {
            throw new IllegalArgumentException("refs must not be null or empty");
        }
        List<String> base64Inputs = new ArrayList<>(refs.size());
        for (BlobReference ref : refs) {
            base64Inputs.add(downloadAndEncode(ref));
        }
        return callEmbeddingEndpoint(base64Inputs);
    }

    @Override
    public int dimensions() {
        return dimension;
    }

    @Override
    public String modelName() {
        return modelName;
    }

    @Override
    public int maxBatchSize() {
        return maxBatchSize;
    }

    private List<float[]> callEmbeddingEndpoint(List<String> inputs) {
        List<float[]> allResults = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i += maxBatchSize) {
            int end = Math.min(i + maxBatchSize, inputs.size());
            List<String> chunk = inputs.subList(i, end);
            List<float[]> chunkResults = callWithRetry(chunk);
            allResults.addAll(chunkResults);
        }
        return allResults;
    }

    private List<float[]> callWithRetry(List<String> inputs) {
        long backoffMs = INITIAL_BACKOFF_MS;
        Exception lastException = null;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                sleep(backoffMs);
                backoffMs *= 2;
            }
            waitForRateLimit();

            try {
                return sendOnce(inputs, attempt);
            } catch (IOException e) {
                lastException = e;
                if (attempt >= MAX_RETRIES) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Embedding request interrupted", e);
            }
        }
        throw new RuntimeException(
                "Embedding request failed after " + (MAX_RETRIES + 1) + " attempts",
                lastException);
    }

    /**
     * Sends a single embedding request. Throws {@link IOException} for
     * retryable HTTP failures and {@link RuntimeException} for terminal ones.
     */
    private List<float[]> sendOnce(List<String> inputs, int attempt)
            throws IOException, InterruptedException {
        String requestBody = buildRequestJson(inputs);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString());
        int statusCode = response.statusCode();

        if (statusCode == HTTP_OK) {
            return parseResponse(response.body(), inputs.size());
        }
        if (isRetryable(statusCode) && attempt < MAX_RETRIES) {
            throw new IOException("HTTP " + statusCode + " from embedding endpoint");
        }
        throw new RuntimeException(
                "Embedding request failed with HTTP " + statusCode
                        + ": " + response.body());
    }

    private void waitForRateLimit() {
        while (true) {
            long now = System.nanoTime();
            long last = lastRequestNanos.get();
            long elapsed = now - last;
            if (elapsed >= minIntervalNanos) {
                if (lastRequestNanos.compareAndSet(last, now)) {
                    return;
                }
            } else {
                long waitMs = (minIntervalNanos - elapsed) / NANOS_PER_MILLI + 1;
                sleep(waitMs);
            }
        }
    }

    private String buildRequestJson(List<String> inputs) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode inputArray = root.putArray("input");
        for (String input : inputs) {
            inputArray.add(input);
        }
        root.put("model", modelName);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize embedding request", e);
        }
    }

    private List<float[]> parseResponse(String responseBody, int expectedCount) {
        try {
            JsonNode root = MAPPER.readTree(responseBody);
            JsonNode data = root.path("data");
            if (!data.isArray()) {
                throw new RuntimeException("Invalid response: missing 'data' array");
            }
            List<float[]> results = new ArrayList<>(expectedCount);
            for (int i = 0; i < expectedCount; i++) {
                if (i >= data.size()) {
                    throw new RuntimeException(
                            "Invalid response: expected " + expectedCount
                                    + " embeddings, found " + data.size());
                }
                JsonNode embeddingNode = data.get(i).path("embedding");
                if (!embeddingNode.isArray()) {
                    throw new RuntimeException(
                            "Invalid response: missing embedding array at index " + i);
                }
                float[] embedding = new float[embeddingNode.size()];
                for (int d = 0; d < embeddingNode.size(); d++) {
                    embedding[d] = (float) embeddingNode.get(d).asDouble();
                }
                if (embedding.length != dimension) {
                    throw new RuntimeException(
                            "Dimension mismatch: expected " + dimension
                                    + ", got " + embedding.length);
                }
                results.add(embedding);
            }
            return results;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Invalid response: failed to parse JSON", e);
        }
    }

    private String downloadAndEncode(BlobReference ref) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ref.uri()))
                    .timeout(timeout)
                    .GET()
                    .build();
            HttpResponse<InputStream> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != HTTP_OK) {
                throw new IOException(
                        "Failed to download blob " + ref.uri()
                                + ": HTTP " + response.statusCode());
            }
            try (InputStream is = response.body()) {
                byte[] bytes = is.readAllBytes();
                return Base64.getEncoder().encodeToString(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to download blob: " + ref.uri(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Blob download interrupted: " + ref.uri(), e);
        }
    }

    private static boolean isRetryable(int statusCode) {
        return statusCode == HTTP_TOO_MANY_REQUESTS || statusCode == HTTP_SERVICE_UNAVAILABLE;
    }

    private static void sleep(long millis) {
        ThreadUtils.sleepQuietly(Math.max(1, millis));
        if (Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("Interrupted during backoff");
        }
    }
}
