package org.pangrova.ai.worker.embedding.provider.openai;

/**
 * Configuration for {@link OpenAiCompatibleEmbeddingProvider}.
 *
 * <p>Groups the endpoint, model, credentials and tuning knobs needed to call an
 * OpenAI-compatible /v1/embeddings endpoint, keeping constructor signatures small.</p>
 *
 * @param endpoint           absolute URL of the embeddings endpoint
 * @param modelName          model identifier sent in the request body
 * @param apiKey             bearer token, or an empty string for an explicitly anonymous endpoint
 * @param dimension          expected output vector dimensionality
 * @param maxBatchSize       maximum number of inputs per HTTP request
 * @param maxRequestsPerSecond rate limit enforced between requests
 * @param queryPrefix        prefix prepended to query texts (may be null)
 * @param documentPrefix     prefix prepended to document texts (may be null)
 * @param timeoutSeconds     per-request HTTP timeout in seconds
 */
public record OpenAiEmbeddingConfig(
        String endpoint,
        String modelName,
        String apiKey,
        int dimension,
        int maxBatchSize,
        double maxRequestsPerSecond,
        String queryPrefix,
        String documentPrefix,
        int timeoutSeconds) {
}
