package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import io.github.resilience4j.retry.Retry;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.resilience.ResilienceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Registers stages with the retry policy on scheduling and evaluates
 * retry eligibility on failure. When a stage qualifies for retry, resets
 * its state machine to PLANNED for re-scheduling.
 *
 * <p>Uses {@link ResilienceRegistry} to obtain retry configuration (max attempts)
 * and tracks per-stage retry counts locally.</p>
 */
public final class RetryStageHandler implements StageLifecycleHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryStageHandler.class);
    private static final String RETRY_SERVICE_NAME = "coordinator-to-worker";
    private static final int DEFAULT_MAX_RETRIES = 2;

    private static final Set<PangrovaErrorCode> RETRYABLE_ERRORS = Set.of(
            PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE,
            PangrovaErrorCode.RUNTIME_TASK_FAILED);

    private final ResilienceRegistry resilienceRegistry;
    private final ConcurrentHashMap<String, AtomicInteger> retryCounters = new ConcurrentHashMap<>();
    private final int maxRetries;

    public RetryStageHandler(ResilienceRegistry resilienceRegistry) {
        this.resilienceRegistry = checkNotNull(
                resilienceRegistry, "resilienceRegistry must not be null");
        Retry retry = resilienceRegistry.getRetry(RETRY_SERVICE_NAME);
        this.maxRetries = retry.getRetryConfig().getMaxAttempts() - 1;
    }

    @Override
    public void onScheduling(StageLifecycleEvent event) {
        Stage stage = event.stage();
        retryCounters.putIfAbsent(
                String.valueOf(stage.stageId()), new AtomicInteger(0));
    }

    /**
     * Evaluates whether a failed stage should be retried.
     * If retry is initiated, resets the stage state for re-scheduling.
     * The DAG engine detects the reset and re-schedules the stage.
     */
    @Override
    public void onFailed(StageLifecycleEvent event) {
        Stage failed = event.stage();
        String stageId = String.valueOf(failed.stageId());
        PangrovaErrorCode errorCode = resolveErrorCode(failed.lastError());

        if (!isRetryable(errorCode)) {
            LOG.info("event=stage_retry_declined queryId={} stageId={} errorCode={} reason=non_retryable",
                    event.queryId(), stageId, errorCode.getCode());
            return;
        }

        AtomicInteger counter = retryCounters.computeIfAbsent(stageId, k -> new AtomicInteger(0));
        int currentCount = counter.get();
        if (currentCount >= maxRetries) {
            LOG.info("event=stage_retry_declined queryId={} stageId={} errorCode={} reason=max_retries_exhausted",
                    event.queryId(), stageId, errorCode.getCode());
            return;
        }

        counter.incrementAndGet();
        LOG.info("event=stage_retry_initiated queryId={} stageId={} errorCode={} retryCount={}",
                event.queryId(), stageId, errorCode.getCode(), counter.get());

        if (!failed.tryPrepareRetry()) {
            LOG.info("event=stage_retry_exhausted queryId={} stageId={}", event.queryId(), stageId);
            return;
        }
        failed.stateMachine().reset();
    }

    /** Returns the current retry count for a stage. */
    public int getRetryCount(String stageId) {
        AtomicInteger counter = retryCounters.get(stageId);
        return counter != null ? counter.get() : 0;
    }

    private static boolean isRetryable(PangrovaErrorCode errorCode) {
        return RETRYABLE_ERRORS.contains(errorCode);
    }

    private static PangrovaErrorCode resolveErrorCode(ErrorDetail err) {
        if (err == null || err.errorCode() == null) {
            return PangrovaErrorCode.RUNTIME_TASK_FAILED;
        }
        for (PangrovaErrorCode code : PangrovaErrorCode.values()) {
            if (code.getCode().equals(err.errorCode())) {
                return code;
            }
        }
        return PangrovaErrorCode.RUNTIME_TASK_FAILED;
    }
}
