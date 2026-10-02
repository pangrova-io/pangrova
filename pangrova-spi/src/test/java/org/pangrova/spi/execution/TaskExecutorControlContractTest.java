package org.pangrova.spi.execution;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Structural control-plane boundary checks; real task behavior is tested in Worker JNI tests. */
class TaskExecutorControlContractTest {
    @Test
    void limitsAreNotSynthesizedByTheExecutionSpi() {
        assertThat(Arrays.stream(TaskExecutor.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName))
                .doesNotContain("capabilities");
    }

    @Test
    void controlsDoNotExposeLocalMemoryAddresses() {
        assertThat(Arrays.stream(TaskExecutor.class.getDeclaredMethods()))
                .allSatisfy(method -> assertThat(method.getReturnType())
                        .isIn(long.class, void.class, boolean.class, byte[].class,
                                java.util.Optional.class, BatchResult.class));
    }
}
