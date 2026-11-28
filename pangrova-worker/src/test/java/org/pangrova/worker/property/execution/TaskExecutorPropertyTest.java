package org.pangrova.worker.property.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.execution.NativeTaskExecutor;
import org.pangrova.worker.exception.JniCallException;
import org.pangrova.worker.jni.NativeRuntime;

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
                    Map.entry("startTask", "nativeStartTaskWithTrace"),
                    Map.entry("startResultTask", "nativeStartTaskWithTrace"),
                    Map.entry("releaseTask", "nativeReleaseExecutor"),
                    Map.entry("cancelTask", "nativeCancelTask"),
                    Map.entry("executeCommand", "nativeCommand"),
                    Map.entry("pushExchangeChunk", "nativePushExchangeChunk"),
                    Map.entry("isFinished", "nativeReadTaskState"),
                    Map.entry("awaitFinished", "nativeReadTaskState"),
                    Map.entry("getTaskStatus", "nativeGetTaskStatus"),
                    Map.entry("getTaskStats", "nativeGetTaskStats"));

    @Property(tries = 10)
    void invalidCanonicalRequestsDoNotAcquireNativeOwnership(
            @ForAll("missingAdmissionFields") String field) {
        try (var runtime = new ActualNativeTaskFixture()) {
            var input = runtime.valuesRequest(false).toBuilder();
            switch (field) {
                case "attempt" -> input.clearAttempt();
                case "plan" -> input.clearSubstraitPlan();
                case "checksum" -> input.clearSubstraitPlanChecksum();
                case "grant" -> input.clearResourceGrant();
                default -> throw new AssertionError(field);
            }
            assertThatThrownBy(() -> runtime.executor().createTask(input.build()))
                    .isInstanceOf(org.pangrova.worker.exception.NativeTaskCreationFailure.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
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
                var report = TaskReport.parseFrom(NativeRuntime.nativeGetTaskStatus(handle));
                assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
                assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_PENDING);
                assertThat(NativeRuntime.nativeReadTaskState(handle, 0)).isEqualTo(report.getStateValue());
                assertThat(runtime.executor().isFinished(handle)).isFalse();
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
                var report = runtime.awaitFinished(handle);
                assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
                assertThat(NativeRuntime.nativeReadTaskState(handle, 0)).isEqualTo(report.getStateValue());
                assertThat(runtime.executor().isFinished(handle)).isTrue();
                assertThat(runtime.executor().getTaskStats(handle)).isNotEmpty();
                runtime.executor().releaseTask(handle);
                assertNativeHandleGone(handle);
            }
        }
    }

    @Test
    void nativeStateProbeObservesCancellationAndRejectsReleasedHandles() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            long handle = runtime.createPending(runtime.valuesRequest(false));
            runtime.executor().cancelTask(handle);
            var report = TaskReport.parseFrom(runtime.executor().getTaskStatus(handle));
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(NativeRuntime.nativeReadTaskState(handle, 50)).isEqualTo(report.getStateValue());
            assertThat(runtime.executor().awaitFinished(handle, 50)).isTrue();
            runtime.executor().releaseTask(handle);
            assertThatThrownBy(() -> NativeRuntime.nativeReadTaskState(handle, 0))
                    .isInstanceOf(JniCallException.class)
                    .hasMessageContaining("not found");
        }
    }

    @Property
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

    @Property
    void taskExecutorSpiDeclaresExactlyRequiredMethods(@ForAll("spiMethodNames") String name)
            throws Exception {
        assertThat(TaskExecutor.class.getMethod(name, parameters(name)).isDefault()).isFalse();
        assertThat(Arrays.stream(TaskExecutor.class.getDeclaredMethods())
                .filter(method -> !method.isDefault()).map(Method::getName))
                .containsExactlyInAnyOrderElementsOf(SPI_TO_NATIVE_RUNTIME.keySet());
        assertThat(Arrays.stream(TaskExecutor.class.getDeclaredMethods())
                .filter(Method::isDefault).map(Method::getName))
                .containsExactlyInAnyOrder("createAdmittedTask", "isWorkerExecuted", "getTaskCommitFragments");
    }

    @Property
    void nativeTaskExecutorIsFinal(@ForAll("spiMethodNames") String name) throws Exception {
        assertThat(Modifier.isFinal(NativeTaskExecutor.class.getModifiers())).isTrue();
        assertThat(NativeTaskExecutor.class.getMethod(name, parameters(name)).getDeclaringClass())
                .isEqualTo(NativeTaskExecutor.class);
    }

    private static Class<?>[] parameters(String name) {
        return switch (name) {
            case "createTask" -> new Class<?>[] {org.pangrova.protocol.v2.TaskRequest.class};
            case "startResultTask" -> new Class<?>[] {long.class, Runnable.class};
            case "executeCommand" -> new Class<?>[] {long.class, byte[].class};
            case "pushExchangeChunk" -> new Class<?>[] {long.class, byte[].class, byte[].class};
            case "awaitFinished" -> new Class<?>[] {long.class, int.class};
            default -> new Class<?>[] {long.class};
        };
    }

    private static void assertNativeHandleGone(long handle) {
        assertThatThrownBy(() -> NativeRuntime.nativeGetTaskStatus(handle))
                .isInstanceOf(JniCallException.class)
                .hasMessageContaining("not found");
    }

    @Provide
    Arbitrary<String> missingAdmissionFields() {
        return Arbitraries.of("attempt", "plan", "checksum", "grant");
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
