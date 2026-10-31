package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.coordinator.resilience.retry.StageRetryManager;
import org.junit.jupiter.api.Test;

/**
 * Verifies StageRetryManager retry decisions when wired into query dispatcher dependencies.
 * Tests retry on retryable errors and fail-fast on non-retryable errors.
 */
class QueryDispatcherStageRetryTest {

    @Test
    void retryableErrorTriggersStageRetry() {
        StageRetryManager retryManager = new StageRetryManager();
        retryManager.registerStage("stage-1", 3);

        boolean shouldRetry = retryManager.onTaskFailure(
                "stage-1", "task-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);

        assertThat(shouldRetry).isTrue();
        retryManager.initiateStageRetry("stage-1");
        assertThat(retryManager.getRetryCount("stage-1")).isEqualTo(1);
    }

    @Test
    void nonRetryableErrorDoesNotTriggerRetry() {
        StageRetryManager retryManager = new StageRetryManager();
        retryManager.registerStage("stage-2", 3);

        boolean shouldRetry = retryManager.onTaskFailure(
                "stage-2", "task-1", PangrovaErrorCode.PARSER_SYNTAX_ERROR);

        assertThat(shouldRetry).isFalse();
        assertThat(retryManager.getRetryCount("stage-2")).isZero();
    }

    @Test
    void retryManagerDefaultConstructorWorks() {
        StageRetryManager retryManager = new StageRetryManager();
        // registering and querying a stage should work
        retryManager.registerStage("stage-x", 5);
        assertThat(retryManager.getRetryCount("stage-x")).isZero();
        assertThat(retryManager.getGeneration("stage-x")).isZero();
    }

    @Test
    void maxRetriesExhaustedStopsRetrying() {
        StageRetryManager retryManager = new StageRetryManager();
        retryManager.registerStage("stage-3", 3);

        retryManager.onTaskFailure("stage-3", "t-1", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        retryManager.initiateStageRetry("stage-3");
        retryManager.onTaskFailure("stage-3", "t-2", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);
        retryManager.initiateStageRetry("stage-3");

        boolean shouldRetry = retryManager.onTaskFailure(
                "stage-3", "t-3", PangrovaErrorCode.EXCHANGE_SOURCE_UNAVAILABLE);

        assertThat(shouldRetry).isFalse();
        assertThat(retryManager.getRetryCount("stage-3")).isEqualTo(2);
    }
}
