package org.pangrova.ai.control.embedding;

import java.util.Optional;

/**
 * Immutable request carrying fields to update on an existing embedding model.
 * Only non-empty Optional fields are applied; absent fields retain their current value.
 *
 * @param endpoint             new endpoint URL (if present, overrides existing)
 * @param apiKeySecret         new secret reference (if present, overrides existing)
 * @param maxBatchSize         new max batch size (if present, overrides existing)
 * @param maxRequestsPerSecond new rate limit (if present, overrides existing)
 * @param queryPrefix          new query prefix (if present, overrides existing)
 * @param documentPrefix       new document prefix (if present, overrides existing)
 */
public record ModelUpdateRequest(
        Optional<String> endpoint,
        Optional<String> apiKeySecret,
        Optional<Integer> maxBatchSize,
        Optional<Integer> maxRequestsPerSecond,
        Optional<String> queryPrefix,
        Optional<String> documentPrefix) {

    public ModelUpdateRequest {
        endpoint = defaultEmpty(endpoint);
        apiKeySecret = defaultEmpty(apiKeySecret);
        maxBatchSize = defaultEmpty(maxBatchSize);
        maxRequestsPerSecond = defaultEmpty(maxRequestsPerSecond);
        queryPrefix = defaultEmpty(queryPrefix);
        documentPrefix = defaultEmpty(documentPrefix);

        validatePositiveValues(maxBatchSize, maxRequestsPerSecond);
        validateNonBlankStrings(endpoint, apiKeySecret);
    }

    private static <T> Optional<T> defaultEmpty(Optional<T> value) {
        return value != null ? value : Optional.empty();
    }

    private static void validatePositiveValues(Optional<Integer> maxBatchSize,
                                               Optional<Integer> maxRequestsPerSecond) {
        if (maxBatchSize.isPresent() && maxBatchSize.get() <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        if (maxRequestsPerSecond.isPresent() && maxRequestsPerSecond.get() <= 0) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be positive");
        }
    }

    private static void validateNonBlankStrings(Optional<String> endpoint,
                                                Optional<String> apiKeySecret) {
        if (endpoint.isPresent() && endpoint.get().isBlank()) {
            throw new IllegalArgumentException("endpoint must not be blank");
        }
        if (apiKeySecret.isPresent() && apiKeySecret.get().isBlank()) {
            throw new IllegalArgumentException("apiKeySecret must not be blank");
        }
    }

    /**
     * Returns true if at least one field is present for update.
     */
    public boolean hasUpdates() {
        return endpoint.isPresent()
                || apiKeySecret.isPresent()
                || maxBatchSize.isPresent()
                || maxRequestsPerSecond.isPresent()
                || queryPrefix.isPresent()
                || documentPrefix.isPresent();
    }
}
