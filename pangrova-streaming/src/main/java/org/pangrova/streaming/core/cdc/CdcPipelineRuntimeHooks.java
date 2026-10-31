package org.pangrova.streaming.core.cdc;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.streaming.core.commit.CdcCommitCoordinator;

import java.util.Objects;

/**
 * Groups the runtime hooks consumed by {@link CdcPipelineLoop}: the failure
 * listener, the retry backoff strategy, and the optional 2PC commit coordinator.
 *
 * <p>Extracted as a record to keep {@link CdcPipelineLoopContext} within the
 * constructor-parameter governance limit.</p>
 *
 * @param listener          callback for pipeline failure events
 * @param retryBackoff      exponential backoff for transient failures
 * @param commitCoordinator optional durable checkpoint/publication coordinator;
 *                          when null, the caller selects at-least-once delivery
 */
public record CdcPipelineRuntimeHooks(
    CdcPipelineListener listener,
    BackoffStrategy retryBackoff,
    CdcCommitCoordinator commitCoordinator
) {

    /** Validates the required hooks; commitCoordinator is nullable by design. */
    public CdcPipelineRuntimeHooks {
        Objects.requireNonNull(listener, "listener must not be null");
        Objects.requireNonNull(retryBackoff, "retryBackoff must not be null");
        // commitCoordinator is nullable — null means at-least-once fallback
    }
}
