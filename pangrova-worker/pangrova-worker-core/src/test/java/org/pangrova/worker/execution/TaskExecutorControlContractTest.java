package org.pangrova.worker.execution;

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
                                java.util.Optional.class));
    }

    @Test
    void allEightControlsRequireActualImplementationAndExposeNoDataPull() {
        var methods = TaskExecutor.class.getDeclaredMethods();
        assertThat(methods).hasSize(8);
        assertThat(Arrays.stream(methods).map(java.lang.reflect.Method::getName))
                .containsExactlyInAnyOrder("createTask", "startTask", "releaseTask", "cancelTask",
                        "executeCommand", "isFinished", "getTaskStatus", "getTaskStats")
                .doesNotContain("getNextBatchResult", "getShuffleBatchResult");
        assertThat(methods).allSatisfy(method -> {
            assertThat(java.lang.reflect.Modifier.isAbstract(method.getModifiers())).isTrue();
            assertThat(method.isDefault()).isFalse();
        });
    }
}
