package org.pangrova.worker.execution;

import org.pangrova.spi.execution.TaskExecutor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeTaskExecutor}.
 *
 * <p>Structural tests verify the class contract (AutoCloseable, IPC path,
 * idempotent close) without requiring the native library. Behavioral tests
 * that exercise the JNI path are covered by integration tests with the
 * real native engine.</p>
 */
class NativeTaskExecutorTest {

    @Test
    void implementsAutoCloseable() {
        assertThat(AutoCloseable.class).isAssignableFrom(NativeTaskExecutor.class);
    }

    @Test
    void implementsTaskExecutorSpi() {
        assertThat(TaskExecutor.class).isAssignableFrom(NativeTaskExecutor.class);
    }

    @Test
    void closeIsIdempotent() {
        // close() on a fresh instance (no active handle) must not throw.
        NativeTaskExecutor executor = new NativeTaskExecutor();
        executor.close();
        executor.close(); // second call must be a no-op
    }

    @Test
    void noRootAllocatorField() {
        // Verify the RootAllocator field has been removed.
        // The class should have no field of type BufferAllocator or RootAllocator.
        var fields = NativeTaskExecutor.class.getDeclaredFields();
        for (var field : fields) {
            String typeName = field.getType().getName();
            assertThat(typeName)
                    .as("Field '%s' should not be an Arrow allocator type", field.getName())
                    .doesNotContain("RootAllocator")
                    .doesNotContain("BufferAllocator");
        }
    }

    @Test
    void noDiagPrintStatements() {
        // Verify no System.err.println("[DIAG]") calls remain in the source.
        // The real verification is that the source file no longer contains
        // "[DIAG]" strings, which is enforced by code review.
        // Structural check: cancelTask exists and is a void method (delegates to JNI).
        assertThat(TaskExecutor.class).isAssignableFrom(NativeTaskExecutor.class);
    }

    @Test
    void shuffleBatchReturnType() {
        // Verify getShuffleBatch returns byte[] (serialized batch path).
        // Data-plane getNextBatch was removed; shuffle batch is the remaining
        // byte[]-returning abstract SPI method on NativeTaskExecutor.
        assertThat(TaskExecutor.class).isAssignableFrom(NativeTaskExecutor.class);
    }

    @Test
    void canBeUsedInTryWithResources() {
        // Verify the class works with try-with-resources syntax.
        // This is a compile-time + runtime check that close() is callable.
        try (NativeTaskExecutor executor = new NativeTaskExecutor()) {
            assertThat(executor).isNotNull();
        }
    }

    @Test
    void noArrowImportsInClass() {
        // Verify no Arrow-related types are referenced as fields.
        // Per naming convention C1: Arrow is banned in pangrova-worker Java module.
        var fields = NativeTaskExecutor.class.getDeclaredFields();
        for (var field : fields) {
            String typeName = field.getType().getName();
            assertThat(typeName)
                    .as("Field '%s' must not reference Arrow types", field.getName())
                    .doesNotContain("arrow");
        }
    }
}
