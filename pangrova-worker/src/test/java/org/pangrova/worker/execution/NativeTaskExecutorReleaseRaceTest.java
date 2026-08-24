package org.pangrova.worker.execution;

import org.pangrova.worker.jni.NativeBridgeException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * F-WORKER-EXEC-08: Verifies that releaseTask() properly serializes with
 * concurrent read-locked JNI operations (startTask, isFinished, cancelTask,
 * executeCommand) via per-handle read-write locks.
 *
 * <p>These are structural concurrency tests that verify the locking protocol
 * without requiring the native library. They exercise the lock acquisition
 * and handle tracking logic to confirm no race window exists between
 * read-locked operations and releaseTask's native release call.</p>
 */
class NativeTaskExecutorReleaseRaceTest {

    /**
     * Verifies that per-handle locks are created alongside handle entries
     * in activeHandles, and that releaseTask removes both atomically
     * (from the perspective of other threads holding the global read lock).
     */
    @Test
    void releaseTaskRemovesHandleLockEntry() throws Exception {
        NativeTaskExecutor executor = new NativeTaskExecutor();

        // Manually inject a handle + lock (simulating createTask without JNI)
        injectHandle(executor, 42L);

        assertThat(getHandleLocks(executor)).containsKey(42L);
        assertThat(getActiveHandles(executor)).containsKey(42L);

        // releaseTask should remove from both maps.
        // The nativeReleaseExecutor JNI call will throw because the handle
        // was never created in the native registry -- catch and verify cleanup.
        try {
            executor.releaseTask(42L);
        } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
            // Expected: native unavailable (UnsatisfiedLinkError / class-init
            // failure) OR handle not found in native registry
        }

        assertThat(getHandleLocks(executor)).doesNotContainKey(42L);
        assertThat(getActiveHandles(executor)).doesNotContainKey(42L);
    }

    /**
     * Verifies that operations on a released handle throw IllegalStateException
     * (handle not tracked) rather than proceeding with a stale handle.
     */
    @Test
    void operationOnReleasedHandleThrows() throws Exception {
        NativeTaskExecutor executor = new NativeTaskExecutor();
        injectHandle(executor, 99L);

        // Release the handle
        try {
            executor.releaseTask(99L);
        } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
            // Expected: native unavailable (UnsatisfiedLinkError / class-init
            // failure) OR handle not in native registry
        }

        // Subsequent operations should fail with "not tracked"
        assertThatThrownBy(() -> executor.startTask(99L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not tracked");

        assertThatThrownBy(() -> executor.cancelTask(99L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not tracked");

        // isFinished() on a released handle returns true (handle is effectively finished)
        // rather than throwing — this is by design for the completion monitor
        assertThat(executor.isFinished(99L)).isTrue();

        assertThatThrownBy(() -> executor.executeCommand(99L, new byte[]{1}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not tracked");
    }

    /**
     * Verifies that releaseTask on a closed executor is a no-op
     * (close() handles all remaining releases).
     */
    @Test
    void releaseTaskAfterCloseIsNoOp() throws Exception {
        NativeTaskExecutor executor = new NativeTaskExecutor();
        injectHandle(executor, 7L);

        // Close releases all handles
        try {
            executor.close();
        } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
            // Expected: native unavailable (UnsatisfiedLinkError / class-init
            // failure) OR handle not in native registry
        }

        // releaseTask after close should not throw
        executor.releaseTask(7L);  // no-op, returns early because closed=true
    }

    /**
     * Verifies that double-release is safe (idempotent).
     */
    @Test
    void doubleReleaseIsIdempotent() throws Exception {
        NativeTaskExecutor executor = new NativeTaskExecutor();
        injectHandle(executor, 55L);

        try {
            executor.releaseTask(55L);
        } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
            // Expected: native unavailable (UnsatisfiedLinkError / class-init
            // failure) OR handle not in native registry
        }

        // Second release should be a no-op (handleLock == null path)
        executor.releaseTask(55L);
    }

    /**
     * Verifies that concurrent releaseTask calls on the same handle
     * don't cause double-free. Only one thread should execute
     * nativeReleaseExecutor; the other should find the handle already removed.
     *
     * <p>Since the handle was never created in the native registry (only injected
     * into Java maps), nativeReleaseExecutor throws NativeBridgeException.
     * We count those exceptions: exactly one thread should reach the JNI call.</p>
     */
    @Test
    void concurrentReleaseOnSameHandleIsSafe() throws Exception {
        int iterations = 100;

        for (int iter = 0; iter < iterations; iter++) {
            NativeTaskExecutor executor = new NativeTaskExecutor();
            injectHandle(executor, 123L);

            AtomicInteger jniCallCount = new AtomicInteger(0);
            AtomicReference<Throwable> unexpectedError = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(2);

            Thread t1 = new Thread(() -> {
                try {
                    barrier.await();
                    executor.releaseTask(123L);
                    // Normal return means shouldRelease was false (handle already
                    // released by the other thread) -- no JNI call was made.
                } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
                    // Reached nativeReleaseExecutor JNI call (handle not in native registry)
                    // or native runtime failed to initialize (class-init failure)
                    jniCallCount.incrementAndGet();
                } catch (Throwable e) {
                    unexpectedError.compareAndSet(null, e);
                }
            }, "release-1");

            Thread t2 = new Thread(() -> {
                try {
                    barrier.await();
                    executor.releaseTask(123L);
                } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
                    jniCallCount.incrementAndGet();
                } catch (Throwable e) {
                    unexpectedError.compareAndSet(null, e);
                }
            }, "release-2");

            t1.start();
            t2.start();
            t1.join(5000);
            t2.join(5000);

            assertThat(unexpectedError.get())
                    .as("No unexpected exceptions on iteration %d", iter)
                    .isNull();

            // Exactly one thread should have reached the JNI call;
            // the other should have found the handle already removed.
            assertThat(jniCallCount.get())
                    .as("Only one thread should call nativeReleaseExecutor on iteration %d", iter)
                    .isEqualTo(1);
        }
    }

    /**
     * Verifies that close() acquires per-handle write locks, ensuring
     * it waits for in-flight operations before releasing handles.
     * Structural check: close() iterates handleLocks (not just activeHandles).
     */
    @Test
    void closeAcquiresPerHandleLocks() throws Exception {
        NativeTaskExecutor executor = new NativeTaskExecutor();
        injectHandle(executor, 200L);
        injectHandle(executor, 201L);

        ConcurrentHashMap<Long, ReentrantReadWriteLock> locks = getHandleLocks(executor);
        assertThat(locks).hasSize(3);

        // Acquire per-handle read lock on handle 200 (simulating in-flight op)
        ReentrantReadWriteLock lock200 = locks.get(200L);
        lock200.readLock().lock();

        AtomicBoolean closeCompleted = new AtomicBoolean(false);
        AtomicReference<Throwable> closeError = new AtomicReference<>();

        Thread closeThread = new Thread(() -> {
            try {
                executor.close();
            } catch (LinkageError | NativeBridgeException | IllegalStateException e) {
                // Expected: native lib not loaded OR handle not in native registry
            } catch (Throwable e) {
                closeError.set(e);
            }
            closeCompleted.set(true);
        }, "close-thread");

        closeThread.start();

        // Give close() time to acquire the global write lock and start
        // iterating handles. It should block on handle 200's per-handle write lock.
        Thread.sleep(100);

        // close() should NOT have completed yet (blocked on handle 200's write lock)
        // Note: this is a probabilistic check; on a very slow machine it might
        // pass spuriously, but it demonstrates the blocking behavior.
        assertThat(closeCompleted.get())
                .as("close() should block while per-handle read lock is held")
                .isFalse();

        // Release the per-handle read lock
        lock200.readLock().unlock();

        // close() should now complete
        closeThread.join(5000);
        assertThat(closeCompleted.get()).isTrue();
        assertThat(closeError.get()).isNull();

        // Both maps should be empty
        assertThat(getActiveHandles(executor)).isEmpty();
        assertThat(getHandleLocks(executor)).isEmpty();
    }

    // ---- Reflective helpers to inject handles without JNI ----

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Long, Boolean> getActiveHandles(NativeTaskExecutor executor)
            throws Exception {
        Field f = NativeTaskExecutor.class.getDeclaredField("activeHandles");
        f.setAccessible(true);
        return (ConcurrentHashMap<Long, Boolean>) f.get(executor);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Long, ReentrantReadWriteLock> getHandleLocks(
            NativeTaskExecutor executor) throws Exception {
        Field f = NativeTaskExecutor.class.getDeclaredField("handleLocks");
        f.setAccessible(true);
        return (ConcurrentHashMap<Long, ReentrantReadWriteLock>) f.get(executor);
    }

    /**
     * Injects a handle into both activeHandles and handleLocks,
     * simulating what createTask() does without calling JNI.
     */
    private static void injectHandle(NativeTaskExecutor executor, long handle) throws Exception {
        getActiveHandles(executor).put(handle, Boolean.TRUE);
        getHandleLocks(executor).put(handle, new ReentrantReadWriteLock());
    }
}
