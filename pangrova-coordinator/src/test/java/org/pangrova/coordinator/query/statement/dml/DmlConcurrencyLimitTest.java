package org.pangrova.coordinator.query.statement.dml;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates BUG-D48 fix: DML concurrency limit error message reports the
 * configured max permits, not {@code availablePermits() + getQueueLength()}.
 *
 * <p>Uses package-private access to {@link DmlExecutor#acquireDmlPermit()} and
 * {@link DmlExecutor#dmlConcurrency} instead of reflection.</p>
 */
class DmlConcurrencyLimitTest {

    @Test
    void errorMessageReportsConfiguredMaxNotDerivedValue() throws Exception {
        int maxDml = 2;
        long timeoutMs = 1;
        DmlExecutor executor = new DmlExecutor(null,
                new DmlExecutorConfig(null, null, null, maxDml, timeoutMs));

        executor.dmlConcurrency.acquire(maxDml);

        assertThatThrownBy(executor::acquireDmlPermit)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max=" + maxDml)
                .hasMessageContaining("timed out after " + timeoutMs + "ms");

        executor.dmlConcurrency.release(maxDml);
    }

    @Test
    void errorMessageDoesNotContainZeroMax() throws Exception {
        int maxDml = 4;
        long timeoutMs = 1;
        DmlExecutor executor = new DmlExecutor(null,
                new DmlExecutorConfig(null, null, null, maxDml, timeoutMs));

        executor.dmlConcurrency.acquire(maxDml);

        try {
            executor.acquireDmlPermit();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).contains("max=" + maxDml);
            assertThat(e.getMessage()).doesNotContain("max=0");
        } finally {
            executor.dmlConcurrency.release(maxDml);
        }
    }

    @Test
    void permitAcquireSucceedsWhenAvailable() {
        DmlExecutor executor = new DmlExecutor(null,
                new DmlExecutorConfig(null, null, null, 2, 1000));
        executor.acquireDmlPermit();
        executor.dmlConcurrency.release();
    }
}
