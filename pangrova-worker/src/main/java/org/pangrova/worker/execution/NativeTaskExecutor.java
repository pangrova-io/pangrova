package org.pangrova.worker.execution;

import org.pangrova.observability.tracing.NativeTraceExporter;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.jni.NativeBridgeException;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.NativeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Sole {@link TaskExecutor} implementation -- delegates every call to
 * {@link NativeRuntime} static native methods.
 *
 * <p>Data-plane operations (result batches, shuffle batches, exchange batches)
 * are handled entirely by C++ data exchange servers. Java only issues
 * control-plane commands via {@link #executeCommand}.</p>
 *
 * <p>Implements {@link AutoCloseable} to ensure native resources are
 * released. The {@link #close()} method calls
 * {@link NativeRuntime#nativeReleaseExecutor} if a task handle is active.</p>
 *
 * <p>Requires {@link NativeRuntime#initNativeRuntime} to have been called before
 * any JNI method is invoked. In production, this is guaranteed by
 * {@code NativeRuntimeBootstrap} running before {@code WorkerTaskBootstrap}.</p>
 *
 * <p>Concurrency strategy: per-handle {@link ReentrantReadWriteLock} entries
 * in {@link #handleLocks} serialize read-locked JNI operations (startTask,
 * isFinished, cancelTask, executeCommand) against write-locked release
 * (releaseTask, close). A global {@link #lifecycleLock} protects the
 * {@link #closed} flag and serializes createTask with close.</p>
 */
public final class NativeTaskExecutor implements TaskExecutor, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeTaskExecutor.class);

    /**
     * Tracks all active executor handles for close().
     * ConcurrentHashMap allows concurrent handle operations while
     * close() can iterate and release all handles atomically under write lock.
     */
    private final ConcurrentHashMap<Long, Boolean> activeHandles =
            new ConcurrentHashMap<>();

    /**
     * Per-handle read-write locks: read lock = handle operations (concurrent),
     * write lock = release/close (exclusive per handle). Prevents TOCTOU race
     * between JNI operations and native handle release.
     */
    private final ConcurrentHashMap<Long, ReentrantReadWriteLock> handleLocks =
            new ConcurrentHashMap<>();

    /**
     * Global lifecycle lock: protects the closed flag and serializes
     * createTask with close. Not used for per-handle operations.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private boolean closed = false;

    {
        // Handle 0 is a global (per-worker) command handle used for server-level
        // commands. Register it with its own per-handle lock so that all handles
        // follow the same locking path — no special-case branching.
        handleLocks.put(0L, new ReentrantReadWriteLock());
    }

    @Override
    public long createTask(byte[] taskRequestBytes) {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("NativeTaskExecutor is closed");
            }
            String traceId = NativeTraceExporter.currentTraceId();
            long handle = NativeRuntime.nativeCreateTaskExecutorWithTrace(
                    taskRequestBytes, traceId);
            this.activeHandles.put(handle, Boolean.TRUE);
            this.handleLocks.put(handle, new ReentrantReadWriteLock());
            return handle;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Acquires the per-handle read lock, preventing concurrent releaseTask/close
     * from releasing the native handle while a JNI call is in flight.
     * Throws IllegalStateException if the handle is not tracked (already released
     * or never created).
     *
     * <p>Handle 0 is a global (per-worker) command handle registered at
     * construction time with its own dedicated lock, same as task handles.</p>
     *
     * @return the per-handle lock (caller must unlock readLock in finally block)
     */
    private ReentrantReadWriteLock acquireHandleReadLock(long executorHandle) {
        ReentrantReadWriteLock lock = handleLocks.get(executorHandle);
        if (lock == null) {
            throw new IllegalStateException(
                    "Handle " + executorHandle + " is not tracked");
        }
        lock.readLock().lock();
        if (closed && executorHandle == 0) {
            lock.readLock().unlock();
            throw new IllegalStateException(
                    "NativeTaskExecutor is closed, cannot operate on global handle");
        }
        if (!handleLocks.containsKey(executorHandle)) {
            lock.readLock().unlock();
            throw new IllegalStateException(
                    "Handle " + executorHandle + " is not tracked");
        }
        return lock;
    }

    @Override
    public void startTask(long executorHandle) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            String traceId = NativeTraceExporter.currentTraceId();
            NativeRuntime.nativeStartTaskWithTrace(executorHandle, traceId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Masks a byte to its unsigned value range. */
    private static final int UNSIGNED_BYTE_MASK = 0xFF;

    /** Masks the value bits of a single varint-encoded byte. */
    private static final int VARINT_VALUE_MASK = 0x7F;

    /** Masks the value bits of a single varint-encoded byte (long form). */
    private static final long VARINT_VALUE_MASK_LONG = 0x7FL;

    /**
     * Parses a structured batch result byte array into a {@link org.pangrova.spi.execution.BatchResult}.
     * Format: [status_byte | data...] where status uses BatchResultStatus proto enum values.
     */
    static org.pangrova.spi.execution.BatchResult parseStructuredResult(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return org.pangrova.spi.execution.BatchResult.eof();
        }
        int statusByte = raw[0] & UNSIGNED_BYTE_MASK;
        return switch (statusByte) {
            case 0 -> org.pangrova.spi.execution.BatchResult.dataSlice(raw, 1, raw.length - 1);
            case 1 -> org.pangrova.spi.execution.BatchResult.eof();
            case 2 -> org.pangrova.spi.execution.BatchResult.notReady();
            case 3 -> {
                String msg = raw.length > 1
                        ? new String(raw, 1, raw.length - 1, java.nio.charset.StandardCharsets.UTF_8)
                        : "Native execution error";
                yield org.pangrova.spi.execution.BatchResult.error(msg);
            }
            default -> org.pangrova.spi.execution.BatchResult.error("Unknown JNI status byte: " + statusByte);
        };
    }

    /**
     * Atomically removes the handle from {@link #handleLocks}, acquires the
     * per-handle write lock to wait for in-flight operations, removes from
     * {@link #activeHandles}, then releases the native executor outside
     * the lock. Only the thread that wins the atomic remove calls JNI,
     * preventing double-free.
     */
    @Override
    public void releaseTask(long executorHandle) {
        ReentrantReadWriteLock lock = handleLocks.remove(executorHandle);
        if (lock == null) {
            return; // Already released or never existed -- idempotent
        }
        boolean shouldRelease;
        lock.writeLock().lock();
        try {
            shouldRelease = activeHandles.remove(executorHandle) != null;
        } finally {
            lock.writeLock().unlock();
        }
        if (shouldRelease) {
            NativeRuntime.nativeReleaseExecutor(executorHandle);
        }
    }

    @Override
    public void cancelTask(long executorHandle) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            NativeRuntime.nativeCancelTask(executorHandle);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Queries native task state to determine if the task has reached a terminal state.
     * Calls {@link NativeRuntime#nativeGetTaskStatus} and parses the first varint field
     * (TaskState enum) from the serialized TaskStatus proto.
     *
     * <p>Terminal states: FINISHED(4), FAILED(5), CANCELLED(6).</p>
     *
     * <p>When the task has FAILED (stateValue == 5), throws {@link NativeRuntimeException}
     * with the error message and native stack trace extracted from the TaskStatus proto.</p>
     */
    @Override
    public boolean isFinished(long executorHandle) {
        ReentrantReadWriteLock lock;
        try {
            lock = acquireHandleReadLock(executorHandle);
        } catch (IllegalStateException e) {
            // Handle already released — the task is effectively finished.
            // This happens when releaseTask() is called while the completion
            // monitor is still polling (e.g., during inter-class cleanup).
            LOG.debug("event=is_finished_handle_released executorHandle={}", executorHandle);
            return true;
        }
        try {
            byte[] statusBytes = NativeRuntime.nativeGetTaskStatus(executorHandle);
            return evaluateNativeStatus(statusBytes, executorHandle);
        } catch (NativeBridgeException e) {
            // C++ HandleRegistry no longer holds this handle (removed by
            // removeTerminated in nativeGetTaskStatus). The native side has
            // already cleaned up — treat as finished, matching the Java-side
            // handle-not-tracked path above.
            LOG.debug("event=is_finished_native_handle_removed executorHandle={} error={}",
                    executorHandle, e.getMessage());
            return true;
        } catch (NativeRuntimeException e) {
            throw e;
        } catch (Exception e) {
            LOG.debug("event=is_finished_check_failed executorHandle={} error={}",
                    executorHandle, e.getMessage());
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Interprets the serialized TaskStatus proto and returns whether the task
     * reached a terminal state. Throws {@link NativeRuntimeException} when the
     * task has FAILED.
     */
    private static boolean evaluateNativeStatus(byte[] statusBytes, long executorHandle) {
        if (statusBytes == null || statusBytes.length == 0) {
            return false;
        }
        int stateValue = extractVarintField(statusBytes, 2);
        // Proto TaskState: UNSPECIFIED=0, PENDING=1, RUNNING=2, FLUSHING=3, FINISHED=4, FAILED=5, CANCELLED=6
        boolean finished = stateValue >= 4;
        if (stateValue >= 2) {
            LOG.debug("event=is_finished_poll executorHandle={} stateValue={} finished={}",
                    executorHandle, stateValue, finished);
        }
        if (stateValue == 5) {
            throwNativeFailure(statusBytes);
        }
        return finished;
    }

    /** Throws a {@link NativeRuntimeException} carrying the native failure details. */
    private static void throwNativeFailure(byte[] statusBytes) {
        String errorMessage = extractStringField(statusBytes, 3);
        String nativeStackTrace = extractStringField(statusBytes, 5);
        String fullMessage = errorMessage != null ? errorMessage : "Native task failed";
        NativeRuntimeException ex = new NativeRuntimeException(
                org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION, fullMessage);
        if (nativeStackTrace != null && !nativeStackTrace.isEmpty()) {
            ex.addSuppressed(new RuntimeException(
                    "Native C++ stack trace:\n" + nativeStackTrace));
        }
        throw ex;
    }

    /**
     * Extracts a length-delimited string field by field number from a proto message.
     * Returns null if the field is not found.
     */
    private static String extractStringField(byte[] data, int targetFieldNumber) {
        int pos = 0;
        while (pos < data.length) {
            long tagAndType = readVarint(data, pos);
            if (tagAndType < 0) {
                return null;
            }
            pos += varintSize(tagAndType);
            int fieldNumber = (int) (tagAndType >>> 3);
            int wireType = (int) (tagAndType & 0x07);
            if (wireType == 0) {
                long value = readVarint(data, pos);
                pos += varintSize(value);
            } else if (wireType == 2) {
                long len = readVarint(data, pos);
                pos += varintSize(len);
                if (fieldNumber == targetFieldNumber) {
                    return new String(data, pos, (int) len, java.nio.charset.StandardCharsets.UTF_8);
                }
                pos += (int) len;
            } else {
                return null;
            }
        }
        return null;
    }

    /**
     * Extracts the value of a varint field by field number from a proto message.
     * Returns -1 if the field is not found or the message is malformed.
     */
    private static int extractVarintField(byte[] data, int targetFieldNumber) {
        int pos = 0;
        while (pos < data.length) {
            long tagAndType = readVarint(data, pos);
            if (tagAndType < 0) {
                return -1;
            }
            pos += varintSize(tagAndType);
            int fieldNumber = (int) (tagAndType >>> 3);
            int wireType = (int) (tagAndType & 0x07);
            if (wireType == 0) {
                long value = readVarint(data, pos);
                if (fieldNumber == targetFieldNumber) {
                    return (int) value;
                }
                pos += varintSize(value);
            } else if (wireType == 2) {
                long len = readVarint(data, pos);
                pos += varintSize(len) + (int) len;
            } else {
                return -1;
            }
        }
        return -1;
    }

    private static long readVarint(byte[] data, int offset) {
        long result = 0;
        int shift = 0;
        int pos = offset;
        while (pos < data.length) {
            byte b = data[pos++];
            result |= (long) (b & VARINT_VALUE_MASK) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        return -1;
    }

    private static int varintSize(long value) {
        int size = 1;
        while ((value & ~VARINT_VALUE_MASK_LONG) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    @Override
    public byte[] executeCommand(long executorHandle, byte[] commandProto) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            return NativeRuntime.nativeCommand(executorHandle, commandProto);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Releases native resources if not already released.
     * Acquires per-handle write locks to wait for in-flight operations
     * before releasing each native handle.
     * Idempotent -- safe to call multiple times.
     */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        // Release all handles, acquiring each per-handle write lock
        // to wait for in-flight operations to complete
        for (var entry : handleLocks.entrySet()) {
            long handle = entry.getKey();
            ReentrantReadWriteLock lock = entry.getValue();
            lock.writeLock().lock();
            try {
                boolean shouldRelease = activeHandles.remove(handle) != null;
                if (shouldRelease) {
                    NativeRuntime.nativeReleaseExecutor(handle);
                }
            } catch (Throwable e) {
                LOG.warn("event=close_release_failed executorHandle={} error={}",
                        handle, e.getMessage(), e);
            } finally {
                lock.writeLock().unlock();
            }
        }
        activeHandles.clear();
        handleLocks.clear();
    }
}
