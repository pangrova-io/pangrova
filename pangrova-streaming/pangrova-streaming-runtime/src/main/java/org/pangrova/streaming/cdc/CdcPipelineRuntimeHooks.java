package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.streaming.cdc.commit.CdcCommitCoordinator;

import java.util.Objects;

/**
 * Groups the runtime hooks consumed by {@link CdcPipelineLoop}: failure
 * listener, retry backoff, optional 2PC coordinator, security context,
 * write authorization, fencing, dead-letter handling, and metrics.
 *
 * @param listener           callback for pipeline failure events
 * @param retryBackoff       exponential backoff for transient failures
 * @param commitCoordinator  optional durable checkpoint/publication coordinator;
 *                           when null, the caller selects at-least-once delivery
 * @param securityContext    definer identity propagated to the write sink
 * @param writePolicyEnforcer write authorization callback
 * @param fencingToken       HA epoch validator
 * @param deadLetterHandler  payload sink for {@link DeadLetterPolicy#SKIP_AND_RECORD}
 * @param metrics            pipeline metrics recorder
 */
public record CdcPipelineRuntimeHooks(
    CdcPipelineListener listener,
    BackoffStrategy retryBackoff,
    CdcCommitCoordinator commitCoordinator,
    PipelineSecurityContext securityContext,
    WritePolicyEnforcer writePolicyEnforcer,
    CdcFencingToken fencingToken,
    CdcDeadLetterHandler deadLetterHandler,
    CdcPipelineMetrics metrics
) {

    public CdcPipelineRuntimeHooks {
        Objects.requireNonNull(listener, "listener must not be null");
        Objects.requireNonNull(retryBackoff, "retryBackoff must not be null");
        Objects.requireNonNull(securityContext, "securityContext must not be null");
        Objects.requireNonNull(writePolicyEnforcer, "writePolicyEnforcer must not be null");
        Objects.requireNonNull(fencingToken, "fencingToken must not be null");
        Objects.requireNonNull(deadLetterHandler, "deadLetterHandler must not be null");
        Objects.requireNonNull(metrics, "metrics must not be null");
    }

    /**
     * Test-only hooks: system identity, no-op enforcement, no fencing, no-op
     * dead-letter handling, and no metrics. Production code must use the full
     * constructor so security and observability are actually wired; this
     * factory exists solely so tests do not have to spell out every no-op.
     */
    public static CdcPipelineRuntimeHooks forTests(CdcPipelineListener listener,
                                                   BackoffStrategy retryBackoff,
                                                   CdcCommitCoordinator commitCoordinator) {
        return new CdcPipelineRuntimeHooks(listener, retryBackoff, commitCoordinator,
                PipelineSecurityContext.SYSTEM,
                (context, table, operations) -> { },
                CdcFencingToken.NOOP,
                (stream, payload, cause) -> { },
                CdcPipelineMetrics.noop());
    }
}
