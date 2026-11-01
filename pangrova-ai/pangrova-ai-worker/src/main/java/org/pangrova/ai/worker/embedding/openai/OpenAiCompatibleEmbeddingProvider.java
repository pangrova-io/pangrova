package org.pangrova.ai.worker.embedding.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.spi.ai.embedding.EmbeddingBatchResult;
import org.pangrova.spi.ai.invocation.EmbeddingProvider;
import org.pangrova.spi.ai.embedding.EmbeddingBatchRequest;
import org.pangrova.spi.ai.embedding.EmbeddingPurpose;
import org.pangrova.ai.worker.context.AiHttpTransport;
import org.pangrova.ai.worker.context.AiProviderCallContext;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Embedding provider that calls any OpenAI-compatible /v1/embeddings endpoint.
 *
 * <p>Handles send-rate limiting and an absolute task deadline. Each provider POST is sent once;
 * batch splitting belongs to the invoking operator, and semantic purpose is explicit.
 *
 * <p>Thread-safe: uses java.net.http.HttpClient with no mutable shared state beyond the atomic
 * token bucket.
 */
public final class OpenAiCompatibleEmbeddingProvider implements EmbeddingProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int HTTP_OK = 200;
    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_REQUEST_TIMEOUT = 408;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_GATEWAY_TIMEOUT = 504;
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
     * @param config endpoint, model and tuning configuration
     * @param httpClient HTTP client used for endpoint calls
     */
    OpenAiCompatibleEmbeddingProvider(OpenAiEmbeddingConfig config, HttpClient httpClient) {
        validateDependencies(config, httpClient);
        validateEndpoint(config.endpoint());

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

    private static void validateDependencies(OpenAiEmbeddingConfig config, HttpClient httpClient) {
        if (config == null) {
            throw new IllegalArgumentException("config required");
        }
        requireText(config.endpoint(), "endpoint");
        requireText(config.modelName(), "modelName");
        if (config.apiKey() == null) {
            throw new IllegalArgumentException("apiKey must not be null");
        }
        validatePositiveValues(config);
        if (httpClient == null) {
            throw new IllegalArgumentException("httpClient required");
        }
    }

    private static void validateEndpoint(String endpoint) {
        URI endpointUri = URI.create(endpoint);
        String scheme = endpointUri.getScheme();
        boolean httpScheme = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
        if (!httpScheme || endpointUri.getHost() == null || endpointUri.getUserInfo() != null) {
            throw new IllegalArgumentException(
                    "endpoint must be an absolute HTTP(S) URI without user-info");
        }
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
        if (!Double.isFinite(config.maxRequestsPerSecond()) || config.maxRequestsPerSecond() <= 0) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be positive");
        }
        if (config.timeoutSeconds() <= 0) {
            throw new IllegalArgumentException("timeoutSeconds must be positive");
        }
    }

    @Override
    public EmbeddingBatchResult embed(EmbeddingBatchRequest request) {
        return callEmbeddingEndpoint(prepareInputs(request));
    }

    List<String> prepareInputs(EmbeddingBatchRequest request) {
        java.util.Objects.requireNonNull(request, "request required");
        if (request.inputs().size() > maxBatchSize) {
            throw new IllegalArgumentException(
                    "batch size "
                            + request.inputs().size()
                            + " exceeds maxBatchSize "
                            + maxBatchSize);
        }
        if (!(request.inputs().get(0) instanceof EmbeddingBatchRequest.TextInput)) {
            throw new AiInvocationException(
                    AiInvocationException.Category.CAPABILITY_MISMATCH,
                    false,
                    "OpenAI-compatible /v1/embeddings accepts only text input");
        }
        String prefix = switch (request.purpose()) {
            case RAW -> "";
            case QUERY -> queryPrefix;
            case DOCUMENT -> documentPrefix;
        };
        return request.inputs().stream()
                .map(EmbeddingBatchRequest.TextInput.class::cast)
                .map(input -> prefix + input.text())
                .toList();
    }

    @Override
    public Set<EmbeddingPurpose> supportedPurposes() {
        return Set.of(EmbeddingPurpose.RAW, EmbeddingPurpose.QUERY, EmbeddingPurpose.DOCUMENT);
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

    private EmbeddingBatchResult callEmbeddingEndpoint(List<String> inputs) {
        waitForRateLimit();
        try {
            return sendOnce(inputs);
        } catch (IOException failure) {
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "Embedding provider POST outcome is unknown",
                    failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AiInvocationException(
                    AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN,
                    false,
                    "Embedding provider POST was interrupted with unknown outcome",
                    failure);
        }
    }

    /** Sends a single embedding request and classifies the provider's response. */
    private EmbeddingBatchResult sendOnce(List<String> inputs)
            throws IOException, InterruptedException {
        HttpRequest request = prepareHttpRequest(inputs);
        HttpResponse<String> response = AiHttpTransport.send(httpClient, request);
        int statusCode = response.statusCode();
        if (statusCode == HTTP_OK) {
            return parseResponse(
                    response.body(),
                    inputs.size(),
                    response.headers().firstValue("x-request-id").orElse(""));
        }
        throw responseFailure(statusCode);
    }

    HttpRequest prepareHttpRequest(List<String> inputs) {
        String requestBody = buildRequestJson(inputs);
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

        return request.build();
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

    String buildRequestJson(List<String> inputs) {
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

    EmbeddingBatchResult parseResponse(
            String responseBody, int expectedCount, String providerRequestId) {
        try {
            JsonNode root = MAPPER.readTree(responseBody);
            if (root == null || !root.isObject()) {
                throw invalidResponse("embedding response must be a JSON object");
            }
            JsonNode data = root.path("data");
            if (!data.isArray() || data.size() != expectedCount) {
                throw invalidResponse(
                        "expected "
                                + expectedCount
                                + " embeddings, found "
                                + (data.isArray() ? data.size() : "no data array"));
            }
            float[][] results = new float[expectedCount][];
            for (JsonNode item : data) {
                parseEmbeddingItem(item, expectedCount, results);
            }
            if (Arrays.stream(results).anyMatch(java.util.Objects::isNull)) {
                throw invalidResponse("embedding response omitted an input index");
            }
            return new EmbeddingBatchResult(List.copyOf(Arrays.asList(results)), providerRequestId);
        } catch (JsonProcessingException e) {
            throw invalidResponse("embedding response is not valid JSON", e);
        }
    }

    private void parseEmbeddingItem(JsonNode item, int expectedCount, float[][] results) {
        int index = requireEmbeddingIndex(item, expectedCount, results);
        float[] embedding = parseCoordinates(item.path("embedding"), index);
        requireDimension(embedding);
        results[index] = embedding;
    }

    private static int requireEmbeddingIndex(JsonNode item, int expectedCount, float[][] results) {
        JsonNode indexNode = item.path("index");
        if (!indexNode.isIntegralNumber() || !indexNode.canConvertToInt()) {
            throw invalidResponse("embedding index is missing or non-integral");
        }
        int index = indexNode.intValue();
        if (index < 0 || index >= expectedCount || results[index] != null) {
            throw invalidResponse("embedding indexes are duplicated or out of range");
        }
        return index;
    }

    private static float[] parseCoordinates(JsonNode embeddingNode, int index) {
        if (!embeddingNode.isArray()) {
            throw invalidResponse("missing embedding array at index " + index);
        }
        float[] embedding = new float[embeddingNode.size()];
        for (int d = 0; d < embeddingNode.size(); d++) {
            JsonNode coordinate = embeddingNode.get(d);
            if (!coordinate.isNumber()) {
                throw invalidResponse("embedding coordinate is not numeric");
            }
            embedding[d] = (float) coordinate.doubleValue();
            if (!Float.isFinite(embedding[d])) {
                throw invalidResponse("embedding coordinate is not finite");
            }
        }
        return embedding;
    }

    private void requireDimension(float[] embedding) {
        if (embedding.length != dimension) {
            throw invalidResponse(
                    "Dimension mismatch: expected " + dimension + ", got " + embedding.length);
        }
    }

    static AiInvocationException responseFailure(int statusCode) {
        AiInvocationException.Category category = switch (statusCode) {
            case HTTP_UNAUTHORIZED -> AiInvocationException.Category.AUTHENTICATION;
            case HTTP_FORBIDDEN -> AiInvocationException.Category.AUTHORIZATION;
            case HTTP_REQUEST_TIMEOUT, HTTP_GATEWAY_TIMEOUT ->
                            AiInvocationException.Category.TIMEOUT;
            case HTTP_TOO_MANY_REQUESTS -> AiInvocationException.Category.RATE_LIMIT;
            default -> AiInvocationException.Category.PROVIDER_UNAVAILABLE;
        };
        return new AiInvocationException(
                category, false, "Embedding provider returned HTTP " + statusCode);
    }

    private static AiInvocationException invalidResponse(String message) {
        return invalidResponse(message, null);
    }

    private static AiInvocationException invalidResponse(String message, Throwable cause) {
        return new AiInvocationException(
                AiInvocationException.Category.PROVIDER_RESPONSE_INVALID, false, message, cause);
    }

    private static void sleep(long millis) {
        ThreadUtils.sleepQuietly(AiProviderCallContext.remainingWaitMillis(Math.max(1, millis)));
        if (Thread.currentThread().isInterrupted()) {
            throw new AiInvocationException(
                    AiInvocationException.Category.TIMEOUT,
                    false,
                    "Embedding rate admission was interrupted");
        }
    }
}
