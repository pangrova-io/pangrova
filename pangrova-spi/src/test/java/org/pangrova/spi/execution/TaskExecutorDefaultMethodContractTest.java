package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies the default-method contract of {@link TaskExecutor}.
 *
 * <p>A bare implementation that only implements the four required abstract
 * methods should get safe, well-documented default behavior from the
 * optional methods: EOF for batch retrieval, false for isFinished,
 * unlimited capabilities, and UOE for executeCommand.</p>
 */
@DisplayName("TaskExecutor SPI default method contract")
class TaskExecutorDefaultMethodContractTest {

    /**
     * A bare implementation that only implements the four required abstract
     * methods. All default methods should return their documented defaults.
     */
    private static class BareTaskExecutor implements TaskExecutor {
        @Override
        public long createTask(byte[] taskRequestBytes) {
            return 1L;
        }

        @Override
        public void startTask(long executorHandle) {}

        @Override
        public void releaseTask(long executorHandle) {}

        @Override
        public void cancelTask(long executorHandle) {}
    }

    private final TaskExecutor bare = new BareTaskExecutor();

    @Test
    @DisplayName("getNextBatchResult defaults to EOF")
    void getNextBatchResultDefaultsToEof() {
        BatchResult result = bare.getNextBatchResult(1L);
        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
    }

    @Test
    @DisplayName("getShuffleBatchResult defaults to EOF")
    void getShuffleBatchResultDefaultsToEof() {
        BatchResult result = bare.getShuffleBatchResult(1L);
        assertThat(result.status()).isEqualTo(BatchResult.Status.EOF);
    }

    @Test
    @DisplayName("isFinished defaults to false")
    void isFinishedDefaultsToFalse() {
        assertThat(bare.isFinished(1L)).isFalse();
    }

    @Test
    @DisplayName("capabilities defaults to unlimited")
    void capabilitiesDefaultsToUnlimited() {
        ExecutionCapabilities caps = bare.capabilities();
        assertThat(caps).isEqualTo(ExecutionCapabilities.unlimited());
    }

    @Test
    @DisplayName("executeCommand throws UOE by default")
    void executeCommandThrowsUoe() {
        assertThatThrownBy(() -> bare.executeCommand(1L, new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("executeCommand");
    }

    @Test
    @DisplayName("implementation that overrides getNextBatchResult can return data")
    void overriddenGetNextBatchResultReturnsData() {
        TaskExecutor withData = new BareTaskExecutor() {
            @Override
            public BatchResult getNextBatchResult(long h) {
                return BatchResult.data(new byte[]{1, 2, 3});
            }
        };
        BatchResult result = withData.getNextBatchResult(1L);
        assertThat(result.status()).isEqualTo(BatchResult.Status.DATA);
    }

    @Test
    @DisplayName("implementation that overrides isFinished can return true")
    void overriddenIsFinishedReturnsTrue() {
        TaskExecutor withFinished = new BareTaskExecutor() {
            @Override
            public boolean isFinished(long h) {
                return true;
            }
        };
        assertThat(withFinished.isFinished(1L)).isTrue();
    }
}
