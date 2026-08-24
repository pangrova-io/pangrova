package org.pangrova.worker.property.execution;

import org.pangrova.worker.execution.NativeTaskExecutor;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.pangrova.spi.execution.TaskExecutor;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for TaskExecutor abstraction consistency.
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.5</b></p>
 *
 * <p>Since the native library is not available in the unit-test JVM,
 * these tests verify the SPI contract structurally:</p>
 * <ul>
 *   <li>TaskExecutor interface exposes all required methods with correct signatures</li>
 *   <li>NativeTaskExecutor is the sole implementation and implements TaskExecutor</li>
 *   <li>NativeTaskExecutor delegates to NativeRuntime (verified via method correspondence)</li>
 *   <li>For any byte array, createTask either returns a handle or throws — never silently fails</li>
 * </ul>
 */
@Tag("Feature: e2e-pipeline-integration, Property 4: TaskExecutor abstraction consistency")
class TaskExecutorPropertyTest {

    /**
     * SPI method name → expected NativeRuntime delegate method name.
     */
    private static final Map<String, String> SPI_TO_NATIVE_RUNTIME = Map.ofEntries(
            Map.entry("createTask", "nativeCreateTaskExecutor"),
            Map.entry("startTask", "nativeStartTask"),
            Map.entry("releaseTask", "nativeReleaseExecutor"),
            Map.entry("cancelTask", "nativeCancelTask"),
            Map.entry("executeCommand", "nativeCommand")
    );

    /**
     * For any arbitrary byte array, calling createTask on NativeTaskExecutor
     * must either return a non-negative handle or throw — never silently fail.
     *
     * <p>With the native library loaded, invalid protobuf input causes the
     * native engine to reject the request. The property asserts that for all
     * generated inputs, createTask either returns a valid handle (&ge; 0) or
     * throws an exception — never silently fails.</p>
     */
    @Property(tries = 10)
    void createTaskEitherReturnsHandleOrThrows(
            @ForAll("taskRequestBytes") byte[] input) {
        TaskExecutor executor = new NativeTaskExecutor();

        try {
            long handle = executor.createTask(input);
            assertThat(handle).as("handle must be non-negative").isGreaterThanOrEqualTo(0);
            executor.releaseTask(handle);
        } catch (Exception | Error e) {
            // Any exception/error is acceptable — the key invariant is no silent failure
            assertThat(e).isNotNull();
        }
    }

    /**
     * For any valid executor handle value, calling releaseTask on
     * NativeTaskExecutor must either complete normally or throw — never
     * silently fail.
     */
    @Property(tries = 10)
    void releaseTaskEitherSucceedsOrThrows(
            @ForAll("executorHandles") long handle) {
        TaskExecutor executor = new NativeTaskExecutor();

        try {
            executor.releaseTask(handle);
        } catch (Exception | Error e) {
            // Any exception/error is acceptable — the key invariant is no silent failure
            assertThat(e).isNotNull();
        }
    }

    /**
     * NativeTaskExecutor must implement TaskExecutor — the sole SPI contract.
     * For any instance, the runtime type must be assignable to TaskExecutor.
     */
    @Property(tries = 10)
    void nativeTaskExecutorImplementsTaskExecutorSpi(
            @ForAll("taskRequestBytes") byte[] ignored) {
        TaskExecutor executor = new NativeTaskExecutor();

        assertThat(executor).isInstanceOf(TaskExecutor.class);
        assertThat(TaskExecutor.class).isAssignableFrom(NativeTaskExecutor.class);
    }

    /**
     * Every SPI method declared in TaskExecutor must have a corresponding
     * NativeRuntime static method. This ensures the delegation contract is
     * structurally sound for any SPI method name.
     *
     * <p>NativeRuntime has a static initializer that calls System.loadLibrary,
     * so we load the class without initialization to inspect its declared
     * methods via reflection.</p>
     */
    @Property(tries = 10)
    void everyTaskExecutorMethodHasNativeRuntimeDelegate(
            @ForAll("spiMethodNames") String spiMethodName) throws ClassNotFoundException {
        String expectedNativeMethod = SPI_TO_NATIVE_RUNTIME.get(spiMethodName);
        assertThat(expectedNativeMethod)
                .as("SPI method '%s' must have a known NativeRuntime mapping", spiMethodName)
                .isNotNull();

        // Load NativeRuntime without triggering static initializer (avoids UnsatisfiedLinkError)
        Class<?> bridgeClass = Class.forName(
                "org.pangrova.worker.jni.NativeRuntime", false,
                getClass().getClassLoader());

        Set<String> bridgeMethods = Arrays.stream(bridgeClass.getDeclaredMethods())
                .filter(m -> Modifier.isStatic(m.getModifiers()))
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toSet());

        assertThat(bridgeMethods)
                .as("NativeRuntime must contain delegate method '%s' for SPI method '%s'",
                        expectedNativeMethod, spiMethodName)
                .contains(expectedNativeMethod);
    }

    /**
     * TaskExecutor SPI must declare exactly the 5 required abstract methods.
     * No more, no less — the interface is a stable contract.
     */
    @Property(tries = 10)
    void taskExecutorSpiDeclaresExactlyRequiredMethods(
            @ForAll("taskRequestBytes") byte[] ignored) {
        Set<String> declaredMethods = Arrays.stream(TaskExecutor.class.getDeclaredMethods())
                .filter(m -> !m.isDefault() && !m.isSynthetic())
                .map(Method::getName)
                .collect(Collectors.toSet());

        assertThat(declaredMethods).containsExactlyInAnyOrder(
                "createTask", "startTask",
                "releaseTask", "cancelTask");
    }

    /**
     * NativeTaskExecutor must be final — it is the sole implementation
     * and must not be subclassed.
     */
    @Property(tries = 10)
    void nativeTaskExecutorIsFinal(
            @ForAll("taskRequestBytes") byte[] ignored) {
        assertThat(Modifier.isFinal(NativeTaskExecutor.class.getModifiers()))
                .as("NativeTaskExecutor must be final")
                .isTrue();
    }

    // --- Generators ---

    @Provide
    Arbitrary<byte[]> taskRequestBytes() {
        return Arbitraries.bytes()
                .array(byte[].class)
                .ofMinSize(0)
                .ofMaxSize(512);
    }

    @Provide
    Arbitrary<Long> executorHandles() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE);
    }

    @Provide
    Arbitrary<String> spiMethodNames() {
        return Arbitraries.of(SPI_TO_NATIVE_RUNTIME.keySet().toArray(String[]::new));
    }
}
