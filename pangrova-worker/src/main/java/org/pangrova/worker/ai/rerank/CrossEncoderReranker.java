package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cross-encoder reranker that scores query-document pairs via a model endpoint.
 *
 * <p>Sends candidate documents to an external cross-encoder model for joint relevance scoring.
 * Respects the configured query timeout: if the model endpoint does not respond within the
 * timeout, falls back to returning the input candidates ranked by their original scores
 * and emits a WARN log with timeout details.</p>
 */
final class CrossEncoderReranker implements Reranker {

    private static final Logger LOG = LoggerFactory.getLogger(CrossEncoderReranker.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int HTTP_OK = 200;

    private final String modelName;
    private final URI endpoint;
    private final Duration timeout;
    private final HttpClient httpClient;

    CrossEncoderReranker(String modelName, URI endpoint, Duration timeout) {
        this.modelName = Objects.requireNonNull(modelName, "modelName must not be null");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive, got: " + timeout);
        }
        this.timeout = timeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
    }

    @Override
    public List<RankedResult> rerank(String query, List<ScoredCandidate> candidates, int topK) {
        Objects.requireNonNull(query, "query must not be null");
        Objects.requireNonNull(candidates, "candidates must not be null");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got: " + topK);
        }
        if (candidates.isEmpty()) {
            return List.of();
        }

        try {
            return executeRerank(query, candidates, topK);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Cross-encoder model={} interrupted, falling back to preceding stage results",
                    modelName);
            return fallbackToOriginalScores(candidates, topK);
        } catch (Exception e) {
            LOG.warn("Cross-encoder model={} failed with error={}, "
                    + "falling back to preceding stage results", modelName, e.getMessage());
            return fallbackToOriginalScores(candidates, topK);
        }
    }

    private List<RankedResult> executeRerank(String query, List<ScoredCandidate> candidates, int topK)
            throws Exception {
        String requestBody = buildRequestBody(query, candidates);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != HTTP_OK) {
            LOG.warn("Cross-encoder model={} returned status={}, falling back to original scores",
                    modelName, response.statusCode());
            return fallbackToOriginalScores(candidates, topK);
        }

        return parseScores(response.body(), candidates, topK);
    }

    @Override
    public String name() {
        return "cross_encoder";
    }

    private List<RankedResult> fallbackToOriginalScores(List<ScoredCandidate> candidates, int topK) {
        return candidates.stream()
                .sorted(Comparator.comparingDouble(ScoredCandidate::score).reversed())
                .limit(topK)
                .map(c -> new RankedResult(c.rowOffset(), c.score(), Map.of("fallback", "true")))
                .toList();
    }

    private String buildRequestBody(String query, List<ScoredCandidate> candidates) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("query", query);
        ArrayNode docs = root.putArray("documents");
        for (ScoredCandidate c : candidates) {
            docs.addObject().put("id", c.rowOffset());
        }
        root.put("model", modelName);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize rerank request", e);
        }
    }

    private List<RankedResult> parseScores(String responseBody, List<ScoredCandidate> candidates, int topK) {
        List<Double> scores = parseScoreArray(responseBody, candidates.size());

        if (scores.size() != candidates.size()) {
            LOG.warn("Cross-encoder model={} returned {} scores for {} candidates, "
                    + "falling back to original scores", modelName, scores.size(), candidates.size());
            return fallbackToOriginalScores(candidates, topK);
        }

        record ScoredEntry(long rowOffset, double score) {}

        List<ScoredEntry> scored = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            scored.add(new ScoredEntry(candidates.get(i).rowOffset(), scores.get(i)));
        }

        return scored.stream()
                .sorted(Comparator.comparingDouble(ScoredEntry::score).reversed())
                .limit(topK)
                .map(e -> new RankedResult(e.rowOffset(), e.score(), Map.of("model", modelName)))
                .toList();
    }

    private List<Double> parseScoreArray(String json, int expectedSize) {
        try {
            JsonNode root = MAPPER.readTree(json);
            JsonNode results = root.path("results");
            if (!results.isArray()) {
                results = root.path("data");
            }
            List<Double> scores = new ArrayList<>(expectedSize);
            if (results.isArray()) {
                for (JsonNode item : results) {
                    JsonNode scoreNode = item.path("score");
                    if (scoreNode.isNumber()) {
                        scores.add(scoreNode.asDouble());
                    }
                }
            }
            return scores;
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to parse cross-encoder response JSON", e);
            return List.of();
        }
    }
}
