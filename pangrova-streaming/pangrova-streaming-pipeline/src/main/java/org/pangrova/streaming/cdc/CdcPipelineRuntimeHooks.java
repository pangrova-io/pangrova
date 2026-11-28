package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrency.BackoffStrategy;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.streaming.commit.CdcCommitCoordinator;

import java.util.Objects;

/**
 * Hooks provided to one pipeline attempt by the scheduler.
 *
 * @param metricsProvider      process metrics backend, borrowed for this attempt
 * @param listener          callback for pipeline failure events
 * @param retryBackoff      exponential backoff for transient failures
 * @param commitCoordinator optional durable checkpoint/publication coordinator;
 *                          when null, the caller selects at-least-once delivery
 */
public record CdcPipelineRuntimeHooks(
    MetricsProvider metricsProvider,
    CdcPipelineListener listener,
    BackoffStrategy retryBackoff,
    CdcCommitCoordinator commitCoordinator
) {

    /** Validates the required hooks; commitCoordinator is nullable by design. */
    public CdcPipelineRuntimeHooks {
        Objects.requireNonNull(metricsProvider, "metricsProvider");
        Objects.requireNonNull(listener, "listener must not be null");
        Objects.requireNonNull(retryBackoff, "retryBackoff must not be null");
        // commitCoordinator is nullable — null means at-least-once fallback
    }
}
