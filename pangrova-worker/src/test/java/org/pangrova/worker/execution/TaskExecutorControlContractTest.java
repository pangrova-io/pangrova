package org.pangrova.worker.execution;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

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
                                java.util.Optional.class, java.util.List.class));
    }

    @Test
    void allControlsRequireActualImplementationAndExposeNoDataPull() {
        var methods = TaskExecutor.class.getDeclaredMethods();
        var defaults = Set.of("createAdmittedTask", "isWorkerExecuted", "getTaskCommitFragments");
        assertThat(Arrays.stream(methods).map(java.lang.reflect.Method::getName))
                .containsExactlyInAnyOrder("createTask", "createAdmittedTask", "startTask", "startResultTask",
                        "releaseTask", "cancelTask", "executeCommand", "pushExchangeChunk", "isFinished",
                        "awaitFinished", "getTaskStatus", "getTaskStats", "isWorkerExecuted", "getTaskCommitFragments")
                .doesNotContain("getNextBatchResult", "getShuffleBatchResult");
        assertThat(methods).allSatisfy(method -> {
            if (defaults.contains(method.getName())) {
                assertThat(method.isDefault()).isTrue();
            } else {
                assertThat(java.lang.reflect.Modifier.isAbstract(method.getModifiers())).isTrue();
                assertThat(method.isDefault()).isFalse();
            }
        });
    }
}
