package org.pangrova.coordinator.resilience.retry;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.resilience.policy.ErrorClassifier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests StageRetryManager stage-level retry decisions based on task failure ratio.
 */
class StageRetryManagerTest {

    private final ErrorClassifier classifier = new ErrorClassifier();
    private final StageRetryManager manager = new StageRetryManager(classifier);

    @Test
    void nonRetryableError_doesNotTriggerRetry() {
        manager.registerStage("stage-1", 10);
        boolean retry = manager.onTaskFailure("stage-1", "task-1",
                PangrovaErrorCode.VALIDATION_NULL_ARGUMENT);
        assertThat(retry).isFalse();
    }

    @Test
    void retryableError_belowThreshold_doesNotTriggerRetry() {
        manager.registerStage("stage-1", 10);
        // 1 failure out of 10 = 10% < 30% threshold
        boolean retry = manager.onTaskFailure("stage-1", "task-1",
                PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(retry).isFalse();
    }

    @Test
    void retryableError_aboveThreshold_triggersRetry() {
        manager.registerStage("stage-1", 10);
        // 3 failures out of 10 = 30% >= threshold
        manager.onTaskFailure("stage-1", "task-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        manager.onTaskFailure("stage-1", "task-2", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        boolean retry = manager.onTaskFailure("stage-1", "task-3",
                PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(retry).isTrue();
    }

    @Test
    void initiateStageRetry_incrementsRetryCount() {
        manager.registerStage("stage-1", 10);
        assertThat(manager.getRetryCount("stage-1")).isEqualTo(0);

        manager.initiateStageRetry("stage-1");
        assertThat(manager.getRetryCount("stage-1")).isEqualTo(1);
    }

    @Test
    void maxRetries_exhausted_doesNotTriggerRetry() {
        manager.registerStage("stage-1", 3);
        // Exhaust 2 retries (MAX_STAGE_RETRIES = 2)
        manager.initiateStageRetry("stage-1");
        manager.initiateStageRetry("stage-1");

        // Even with all tasks failing, should not trigger retry
        manager.onTaskFailure("stage-1", "task-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        manager.onTaskFailure("stage-1", "task-2", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        boolean retry = manager.onTaskFailure("stage-1", "task-3",
                PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(retry).isFalse();
    }

    @Test
    void getRetryCount_unknownStage_returnsZero() {
        assertThat(manager.getRetryCount("unknown")).isEqualTo(0);
    }

    @Test
    void initiateStageRetry_unknownStage_doesNotThrow() {
        manager.initiateStageRetry("unknown");
        // No exception expected
    }

    @Test
    void duplicateTaskFailure_countedOnce() {
        manager.registerStage("stage-1", 10);
        manager.onTaskFailure("stage-1", "task-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        manager.onTaskFailure("stage-1", "task-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        // Same task failing twice should count as 1 failure (10% < 30%)
        boolean retry = manager.onTaskFailure("stage-1", "task-1",
                PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        assertThat(retry).isFalse();
    }
}
