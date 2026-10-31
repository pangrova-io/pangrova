package org.pangrova.worker.resource.memory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InsufficientMemoryException} context fields.
 */
class InsufficientMemoryExceptionTest {

    @Test
    void exceptionCarriesFullContext() {
        var ex = new InsufficientMemoryException(
                1024,
                new InsufficientMemoryException.PoolSnapshot("task-pool", 200, 800, 1000));

        assertThat(ex.getPoolName()).isEqualTo("task-pool");
        assertThat(ex.getRequestedBytes()).isEqualTo(1024);
        assertThat(ex.getAvailableBytes()).isEqualTo(200);
        assertThat(ex.getUsedBytes()).isEqualTo(800);
        assertThat(ex.getMaxBytes()).isEqualTo(1000);
        assertThat(ex.getMessage())
                .contains("task-pool")
                .contains("requested=1024")
                .contains("available=200")
                .contains("used=800")
                .contains("max=1000");
    }
}
