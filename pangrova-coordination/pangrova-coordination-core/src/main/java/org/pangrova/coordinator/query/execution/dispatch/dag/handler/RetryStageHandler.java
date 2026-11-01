package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import io.github.resilience4j.retry.Retry;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/** Classifies stage failures and advances the retry budget of the actual query stage owner. */
public final class RetryStageHandler implements StageLifecycleHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RetryStageHandler.class);
    private static final String RETRY_SERVICE_NAME = "coordinator-to-worker";
    private static final Set<PangrovaErrorCode> RETRYABLE_ERRORS = Set.of(
            PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE,
            PangrovaErrorCode.RUNTIME_TASK_FAILED);
    private final int maxRetries;

    /** Uses the real Resilience4j attempt limit without retaining a second shared stage counter. */
    public RetryStageHandler(ResilienceRegistry resilienceRegistry) {
        checkNotNull(resilienceRegistry, "resilienceRegistry must not be null");
        Retry retry = resilienceRegistry.getRetry(RETRY_SERVICE_NAME);
        maxRetries = retry.getRetryConfig().getMaxAttempts() - 1;
    }

    /** Checks the actual execution owner without creating a parallel retry ledger. */
    @Override
    public void onScheduling(StageLifecycleEvent event) {
        requireOwner(event);
    }

    /** A duplicate failure cannot clear or advance the already prepared next attempt. */
    @Override
    public void onFailed(StageLifecycleEvent event) {
        Stage failed = requireOwner(event);
        synchronized (failed) {
            retryFailedOwner(event, failed);
        }
    }

    private static Stage requireOwner(StageLifecycleEvent event) {
        Stage failed = event.stage();
        if (!event.queryId().equals(failed.stateMachine().getQueryId())
                || event.dag().findStage(failed.stageId()) != failed) {
            throw new IllegalArgumentException("Retry event differs from its actual query stage owner");
        }
        return failed;
    }

    private void retryFailedOwner(StageLifecycleEvent event, Stage failed) {
        if (failed.stateMachine().getState() != StageStateMachine.State.FAILED) return;
        var classified = resolveErrorCode(failed.lastError());
        if (classified.isEmpty()) {
            LOG.info("event=stage_retry_declined queryId={} stageId={} reason=unclassified_error",
                    event.queryId(), failed.stageId());
            return;
        }
        PangrovaErrorCode code = classified.orElseThrow();
        if (!RETRYABLE_ERRORS.contains(code)) {
            LOG.info("event=stage_retry_declined queryId={} stageId={} errorCode={} reason=non_retryable",
                    event.queryId(), failed.stageId(), code.getCode());
            return;
        }
        if (failed.retryCount() >= maxRetries || !failed.tryPrepareRetry()) {
            LOG.info("event=stage_retry_declined queryId={} stageId={} reason=max_retries_exhausted",
                    event.queryId(), failed.stageId());
            return;
        }
        if (!failed.stateMachine().reset()) {
            throw new IllegalStateException("Prepared stage retry lost its terminal state owner");
        }
        LOG.info("event=stage_retry_initiated queryId={} stageId={} errorCode={} retryCount={}",
                event.queryId(), failed.stageId(), code.getCode(), failed.retryCount());
    }

    private static Optional<PangrovaErrorCode> resolveErrorCode(ErrorDetail error) {
        if (error == null || error.errorCode() == null) return Optional.empty();
        for (PangrovaErrorCode code : PangrovaErrorCode.values()) {
            if (code.getCode().equals(error.errorCode())) return Optional.of(code);
        }
        return Optional.empty();
    }
}
