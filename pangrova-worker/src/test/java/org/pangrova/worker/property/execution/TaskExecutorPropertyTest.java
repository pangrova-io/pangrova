package org.pangrova.worker.property.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.exception.JniCallException;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.exception.NativeRuntimeException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Actual JNI rejection/registry properties and the sole control-plane SPI structure. */
@Tag("native-required")
@org.junit.jupiter.api.Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskExecutorPropertyTest {
    private static final Map<String, String> SPI_TO_NATIVE_RUNTIME =
            Map.ofEntries(
                    Map.entry("createTask", "nativeCreateTaskExecutor"),
                    Map.entry("startTask", "nativeStartTask"),
                    Map.entry("releaseTask", "nativeReleaseExecutor"),
                    Map.entry("cancelTask", "nativeCancelTask"),
                    Map.entry("executeCommand", "nativeCommand"),
                    Map.entry("isFinished", "nativeGetTaskStatus"),
                    Map.entry("getTaskStatus", "nativeGetTaskStatus"),
                    Map.entry("getTaskStats", "nativeGetTaskStats"));

    @Property(tries = 10)
    void createTaskEitherReturnsHandleOrThrows(@ForAll("taskRequestBytes") byte[] input) {
        try (var runtime = new ActualNativeTaskFixture()) {
            assertThatThrownBy(() -> runtime.executor().createTask(input))
                    .isInstanceOf(org.pangrova.worker.exception.NativeTaskCreationFailure.class)
                    .hasCauseInstanceOf(NativeRuntimeException.class)
                    .satisfies(
                            failure -> {
                                var creation =
                                        (org.pangrova.worker.exception.NativeTaskCreationFailure)
                                                failure;
                                assertThat(creation.actualHandle()).isZero();
                                assertThat(creation.withoutNativeOwner()).isTrue();
                            });
            long valid = runtime.createPending(runtime.valuesRequest(false));
            assertThat(valid).isPositive();
            runtime.executor().releaseTask(valid);
            assertNativeHandleGone(valid);
        }
    }

    @Property(tries = 10)
    void releaseTaskEitherSucceedsOrThrows(@ForAll("executorCounts") int count) throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var handles = new java.util.ArrayList<Long>();
            for (int index = 0; index < count; index++) {
                var request = runtime.valuesRequest(false);
                long handle = runtime.createPending(request);
                handles.add(handle);
                assertThat(
                                TaskReport.parseFrom(NativeRuntime.nativeGetTaskStatus(handle))
                                        .getAttempt())
                        .isEqualTo(request.getAttempt());
            }
            for (long handle : handles) {
                runtime.executor().releaseTask(handle);
                runtime.executor().releaseTask(handle);
                assertNativeHandleGone(handle);
                assertThatThrownBy(() -> runtime.executor().getTaskStatus(handle))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("not tracked");
            }
        }
    }

    @Property(tries = 10)
    void nativeTaskExecutorImplementsTaskExecutorSpi(@ForAll("executorCounts") int count)
            throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            assertThat(runtime.executor()).isInstanceOf(TaskExecutor.class);
            for (int index = 0; index < count; index++) {
                long handle = runtime.createStarted(runtime.valuesRequest(false));
                assertThat(runtime.awaitFinished(handle).getState())
                        .isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
                assertThat(runtime.executor().getTaskStats(handle)).isNotEmpty();
                runtime.executor().releaseTask(handle);
                assertNativeHandleGone(handle);
            }
        }
    }

    @Property(tries = 10)
    void everyTaskExecutorMethodHasNativeRuntimeDelegate(@ForAll("spiMethodNames") String name) {
        String expected = SPI_TO_NATIVE_RUNTIME.get(name);
        Set<String> methods =
                Arrays.stream(NativeRuntime.class.getDeclaredMethods())
                        .filter(
                                method ->
                                        Modifier.isStatic(method.getModifiers())
                                                && Modifier.isPublic(method.getModifiers()))
                        .map(Method::getName)
                        .collect(Collectors.toSet());
        assertThat(methods).contains(expected);
    }

    @Property(tries = 10)
    void taskExecutorSpiDeclaresExactlyRequiredMethods(@ForAll("spiMethodNames") String name)
            throws Exception {
        assertThat(TaskExecutor.class.getMethod(name, parameters(name)).isDefault()).isFalse();
        assertThat(Arrays.stream(TaskExecutor.class.getDeclaredMethods()).map(Method::getName))
                .containsExactlyInAnyOrderElementsOf(SPI_TO_NATIVE_RUNTIME.keySet());
    }

    @Property(tries = 10)
    void nativeTaskExecutorIsFinal(@ForAll("spiMethodNames") String name) throws Exception {
        assertThat(Modifier.isFinal(NativeTaskExecutor.class.getModifiers())).isTrue();
        assertThat(NativeTaskExecutor.class.getMethod(name, parameters(name)).getDeclaringClass())
                .isEqualTo(NativeTaskExecutor.class);
    }

    private static Class<?>[] parameters(String name) {
        return name.equals("createTask")
                ? new Class<?>[] {byte[].class}
                : name.equals("executeCommand")
                        ? new Class<?>[] {long.class, byte[].class}
                        : new Class<?>[] {long.class};
    }

    private static void assertNativeHandleGone(long handle) {
        assertThatThrownBy(() -> NativeRuntime.nativeGetTaskStatus(handle))
                .isInstanceOf(JniCallException.class)
                .hasMessageContaining("not found");
    }

    @Provide
    Arbitrary<byte[]> taskRequestBytes() {
        // An unterminated protobuf varint is malformed for every generated size.
        return Arbitraries.integers()
                .between(1, 512)
                .map(
                        size -> {
                            byte[] input = new byte[size];
                            Arrays.fill(input, (byte) 0x80);
                            return input;
                        });
    }

    @Provide
    Arbitrary<Integer> executorCounts() {
        return Arbitraries.integers().between(1, 5);
    }

    @Provide
    Arbitrary<String> spiMethodNames() {
        return Arbitraries.of(SPI_TO_NATIVE_RUNTIME.keySet().toArray(String[]::new));
    }
}
