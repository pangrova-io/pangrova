package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.streaming.cdc.commit.CdcCommitCoordinator;

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
 * @param commitCoordinator optional 2PC coordinator for exactly-once semantics;
 *                          when null, the pipeline falls back to at-least-once
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
