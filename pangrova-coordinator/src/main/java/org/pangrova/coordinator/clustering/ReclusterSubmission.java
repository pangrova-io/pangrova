package org.pangrova.coordinator.clustering;

import org.pangrova.spi.connector.clustering.ClusteringResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Result of submitting an asynchronous {@code RECLUSTER TABLE} request.
 *
 * <p>{@code taskId} is the handle returned to the SQL caller. The same id
 * resolves to a {@link ReclusterJob} via {@link ReclusterJobTracker#find}
 * for progress queries. {@code completion} fires when the connector returns
 * (success) or the job propagates a failure.</p>
 */
public record ReclusterSubmission(
        String taskId,
        CompletableFuture<ClusteringResult> completion) {

    public ReclusterSubmission {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(completion, "completion");
    }
}
