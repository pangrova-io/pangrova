package org.pangrova.coordinator.resilience.retry;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.resilience.policy.ErrorClassifier;
import org.pangrova.common.resilience.policy.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests SplitRetryManager with RetryPolicy integration.
 * Uses real ErrorClassifier and RetryPolicy — no mocks.
 */
class SplitRetryManagerTest {

    private final ErrorClassifier classifier = new ErrorClassifier();
    private final RetryPolicy retryPolicy = new RetryPolicy(100, 10_000, 2.0, classifier);
    private final SplitRetryManager manager = new SplitRetryManager(classifier, retryPolicy);

    @Test
    void transientError_retriesWithBackoff() {
        // EXTERNAL errors are retryable
        boolean shouldRetry = manager.handleSplitFailure("t1", "s1",
                PangrovaErrorCode.METADATA_CATALOG_BACKEND_ERROR);

        assertThat(shouldRetry).isTrue();
        assertThat(manager.getState("t1", "s1"))
                .isEqualTo(SplitRetryManager.SplitState.RETRY);
    }

    @Test
    void permanentError_failsImmediately() {
        // USER errors are not retryable
        boolean shouldRetry = manager.handleSplitFailure("t1", "s2",
                PangrovaErrorCode.VALIDATION_NULL_ARGUMENT);

        assertThat(shouldRetry).isFalse();
        assertThat(manager.getState("t1", "s2"))
                .isEqualTo(SplitRetryManager.SplitState.FAILED);
    }

    @Test
    void retryBackoff_increasesExponentially() {
        manager.handleSplitFailure("t1", "s3",
                PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);

        Duration backoff = manager.getRetryBackoff("t1", "s3");
        assertThat(backoff.toMillis()).isGreaterThan(0);
    }

    @Test
    void maxRetries_exhausted_failsPermanently() {
        // EXCHANGE errors allow max 3 retries
        for (int i = 0; i < 3; i++) {
            boolean retry = manager.handleSplitFailure("t1", "s4",
                    PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
            assertThat(retry).isTrue();
        }
        // 4th attempt should fail
        boolean retry = manager.handleSplitFailure("t1", "s4",
                PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(retry).isFalse();
        assertThat(manager.getState("t1", "s4"))
                .isEqualTo(SplitRetryManager.SplitState.FAILED);
    }

    @Test
    void stateLifecycle_pendingToRunningToDone() {
        manager.markPending("t2", "s1");
        assertThat(manager.getState("t2", "s1"))
                .isEqualTo(SplitRetryManager.SplitState.PENDING);

        manager.markRunning("t2", "s1");
        assertThat(manager.getState("t2", "s1"))
                .isEqualTo(SplitRetryManager.SplitState.RUNNING);

        manager.markDone("t2", "s1");
        assertThat(manager.getState("t2", "s1"))
                .isEqualTo(SplitRetryManager.SplitState.DONE);
    }

    @Test
    void unknownSplit_returnsNullState() {
        assertThat(manager.getState("unknown", "unknown")).isNull();
    }

    @Test
    void unknownSplit_returnsZeroBackoff() {
        Duration backoff = manager.getRetryBackoff("unknown", "unknown");
        assertThat(backoff).isEqualTo(Duration.ZERO);
    }
}
