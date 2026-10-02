package org.pangrova.worker.execution;

import org.pangrova.observability.tracing.NativeTraceExporter;
import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.worker.ai.invocation.AiInvocationService;
import org.pangrova.worker.exchange.remote.RemoteShuffleByteEgress;
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

    /** Active handles remain tracked until their actual joined cleanup succeeds. */
    private final ConcurrentHashMap<Long, Boolean> activeHandles =
            new ConcurrentHashMap<>();

    /**
     * Per-handle read-write locks: read lock = handle operations (concurrent),
     * write lock = release/close (exclusive per handle). Prevents TOCTOU race
     * between JNI operations and native handle release.
     */
    private final ConcurrentHashMap<Long, ReentrantReadWriteLock> handleLocks =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, TaskIdentity> taskIdentities =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, TaskIdentity> externalShuffleTasks =
            new ConcurrentHashMap<>();

    /** Final Native reports remain owned only while other task cleanup still needs retry. */
    private final ConcurrentHashMap<Long, byte[]> nativeReleasedReports =
            new ConcurrentHashMap<>();

    private final RemoteShuffleByteEgress externalShuffleEgress;

    /** Serializes task creation with close; per-handle operations use their own locks. */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private volatile boolean closed = false;
    private volatile org.pangrova.worker.jni.write.WorkerCatalogRuntime catalogRuntime;

    /** Supplies the production Worker-local catalog authority before Native dataset task creation. */
    public void setCatalogRuntime(org.pangrova.worker.jni.write.WorkerCatalogRuntime runtime) {
        if (!activeHandles.isEmpty()) throw new IllegalStateException("Cannot replace catalogs with active Native tasks");
        catalogRuntime = java.util.Objects.requireNonNull(runtime);
    }

    {
        // Handle 0 is a global (per-worker) command handle used for server-level
        // commands. Register it with its own per-handle lock so that all handles
        // follow the same locking path — no special-case branching.
        handleLocks.put(0L, new ReentrantReadWriteLock());
    }

    /** Creates an executor for the Native Worker-local data plane. */
    public NativeTaskExecutor() {
        this(null);
    }

    /** Creates an executor with the sole external-shuffle JNI egress. */
    public NativeTaskExecutor(RemoteShuffleByteEgress externalShuffleEgress) {
        this.externalShuffleEgress = externalShuffleEgress;
    }

    @Override
    public long createTask(byte[] taskRequestBytes) {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("NativeTaskExecutor is closed");
            }
            var request = parseTaskRequest(taskRequestBytes);
            var local = org.pangrova.worker.jni.write.WorkerNativeStorageTasks.prepare(request, catalogRuntime);
            TaskIdentity identity = taskIdentity(request);
            boolean externalShuffle = prepareExternalShuffle(request);
            boolean aiRegistered = false;
            try {
                AiInvocationService.registerTask(request);
                aiRegistered = true;
                String traceId = NativeTraceExporter.currentTraceId();
                long handle = NativeRuntime.nativeCreateTaskExecutorWithTrace(
                        local.toByteArray(), traceId);
                trackCreatedHandle(handle, identity, externalShuffle);
                return handle;
            } catch (RuntimeException failure) {
                if (aiRegistered) { AiInvocationService.releaseTask(identity.tenantId(), identity.attempt()); }
                if (externalShuffle) {
                    externalShuffleEgress.releaseTask(identity.queryId(), identity.taskId());
                }
                throw failure;
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private void trackCreatedHandle(
            long handle, TaskIdentity identity, boolean externalShuffle) {
        activeHandles.put(handle, Boolean.TRUE);
        handleLocks.put(handle, new ReentrantReadWriteLock());
        rememberTaskIdentity(handle, identity);
        if (externalShuffle) {
            externalShuffleTasks.put(handle, identity);
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
    ReentrantReadWriteLock acquireHandleReadLock(long executorHandle) {
        ReentrantReadWriteLock lock = handleLocks.get(executorHandle);
        if (lock == null) {
            throw new IllegalStateException(
                    "Handle " + executorHandle + " is not tracked");
        }
        lock.readLock().lock();
        if (closed) {
            lock.readLock().unlock();
            throw new IllegalStateException(
                    "NativeTaskExecutor is closed");
        }
        if (!handleLocks.containsKey(executorHandle)) {
            lock.readLock().unlock();
            throw new IllegalStateException(
                    "Handle " + executorHandle + " is not tracked");
        }
        if (nativeReleasedReports.containsKey(executorHandle)) {
            lock.readLock().unlock();
            throw new IllegalStateException(
                    "Handle " + executorHandle + " has completed native release");
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

    /** Joins Native before removing the handle; failed cleanup remains retryable. */
    @Override
    public java.util.Optional<byte[]> releaseTask(long executorHandle) {
        ReentrantReadWriteLock lock = handleLocks.get(executorHandle);
        if (lock == null) {
            return java.util.Optional.empty();
        }
        lock.writeLock().lock();
        try {
            if (!activeHandles.containsKey(executorHandle)) {
                return java.util.Optional.empty();
            }
            revokeAiTask(executorHandle);
            byte[] nativeReport = nativeReleasedReports.get(executorHandle);
            if (nativeReport == null) {
                nativeReport = NativeRuntime.nativeReleaseExecutor(executorHandle);
                nativeReleasedReports.put(executorHandle, nativeReport);
            }
            byte[] finalReport = finalReleaseReport(executorHandle, nativeReport);
            releaseExternalShuffle(executorHandle);
            releaseAiTask(executorHandle);
            activeHandles.remove(executorHandle);
            handleLocks.remove(executorHandle, lock);
            nativeReleasedReports.remove(executorHandle);
            return java.util.Optional.of(finalReport);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private byte[] finalReleaseReport(long executorHandle, byte[] encoded) {
        var report = parseNativeTaskReport(encoded);
        TaskIdentity identity = requireTaskIdentity(executorHandle, report.getAttempt());
        if (report.getReportOrigin() != org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                || (report.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED
                && report.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED
                && report.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_CANCELLED)) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Joined Native release returned a nonterminal or non-Native task report");
        }
        return mergeProviderResources(report,
                AiInvocationService.resourceUsage(identity.tenantId(), identity.attempt())).toByteArray();
    }

    @Override
    public void cancelTask(long executorHandle) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            revokeAiTask(executorHandle);
            NativeRuntime.nativeCancelTask(executorHandle);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Queries native task state to determine if the task has reached a terminal state.
     * Calls {@link NativeRuntime#nativeGetTaskStatus} and decodes the canonical
     * v2 TaskReport response.
     *
     * <p>Terminal states: FINISHED(4), FAILED(5), CANCELLED(6).</p>
     *
     * <p>When the task has FAILED (stateValue == 5), throws {@link NativeRuntimeException}
     * with the error message and native stack trace extracted from the TaskStatus proto.</p>
     */
    @Override
    public boolean isFinished(long executorHandle) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            byte[] statusBytes = NativeRuntime.nativeGetTaskStatus(executorHandle);
            return evaluateNativeStatus(statusBytes, executorHandle);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public byte[] getTaskStatus(long executorHandle) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            byte[] status = NativeRuntime.nativeGetTaskStatus(executorHandle);
            if (status == null || status.length == 0) {
                return new byte[0];
            }
            var report = parseNativeTaskReport(status);
            var attempt = report.getAttempt();
            TaskIdentity identity = requireTaskIdentity(executorHandle, attempt);
            return mergeProviderResources(report,
                    AiInvocationService.resourceUsage(identity.tenantId(), identity.attempt())).toByteArray();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public byte[] getTaskStats(long executorHandle) {
        ReentrantReadWriteLock lock = acquireHandleReadLock(executorHandle);
        try {
            byte[] stats = NativeRuntime.nativeGetTaskStats(executorHandle);
            return stats == null ? new byte[0] : stats;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Interprets the serialized TaskStatus proto and returns whether the task
     * reached a terminal state. Throws {@link NativeRuntimeException} when the
     * task has FAILED.
     */
    static boolean evaluateNativeStatus(byte[] statusBytes, long executorHandle) {
        if (statusBytes == null || statusBytes.length == 0) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned an empty v2 TaskReport");
        }
        org.pangrova.protocol.v2.TaskReport status =
                parseNativeTaskReport(statusBytes);
        int stateValue = status.getStateValue();
        if (stateValue < 1 || stateValue > 6) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned an invalid task state: " + stateValue);
        }
        boolean finished = stateValue == 4 || stateValue == 6;
        if (stateValue >= 2) {
            LOG.debug("event=is_finished_poll executorHandle={} stateValue={} finished={}",
                    executorHandle, stateValue, finished);
        }
        if (stateValue == 5) {
            throwNativeFailure(status);
        }
        return finished;
    }

    /** Throws a {@link NativeRuntimeException} carrying the native failure details. */
    private static void throwNativeFailure(org.pangrova.protocol.v2.TaskReport status) {
        String message = status.hasError() && !status.getError().getMessage().isBlank()
                ? status.getError().getMessage()
                : "Native task failed";
        throw new NativeRuntimeException(
                org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                message);
    }

    private static org.pangrova.protocol.v2.TaskReport parseNativeTaskReport(
            byte[] statusBytes) {
        try {
            return org.pangrova.protocol.v2.TaskReport.parseFrom(statusBytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .PROTOCOL_DESERIALIZATION_FAILED,
                    "Native returned malformed v2 TaskReport", failure);
        }
    }

    private void releaseAiTask(long executorHandle) {
        TaskIdentity identity = taskIdentities.get(executorHandle);
        if (identity != null) {
            AiInvocationService.releaseTask(
                    identity.tenantId(), identity.attempt());
            taskIdentities.remove(executorHandle, identity);
        }
    }

    private void revokeAiTask(long executorHandle) {
        TaskIdentity identity = taskIdentities.get(executorHandle);
        if (identity != null) { AiInvocationService.cancelTask(identity.tenantId(), identity.attempt()); }
    }
    private static org.pangrova.protocol.v2.TaskRequest parseTaskRequest(
            byte[] taskRequestBytes) {
        try {
            return org.pangrova.protocol.v2.TaskRequest.parseFrom(taskRequestBytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .PROTOCOL_DESERIALIZATION_FAILED,
                    "TaskRequest is malformed", failure);
        }
    }

    private static TaskIdentity taskIdentity(
            org.pangrova.protocol.v2.TaskRequest request) {
        if (!request.hasAttempt()) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .PROTOCOL_DESERIALIZATION_FAILED,
                    "TaskRequest has no attempt identity");
        }
        var attempt = request.getAttempt();
        return new TaskIdentity(request.getSecurity().getTenantId(), attempt);
    }

    private boolean prepareExternalShuffle(org.pangrova.protocol.v2.TaskRequest request) {
        boolean hasBinding = request.hasExecutionOptions()
                && request.getExecutionOptions().hasShuffle()
                && request.getExecutionOptions().getShuffle().hasExecutionBinding();
        if (!hasBinding) {
            return false;
        }
        if (externalShuffleEgress == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .PROTOCOL_DESERIALIZATION_FAILED,
                    "TaskRequest requires external shuffle but this Worker uses Native shuffle");
        }
        return externalShuffleEgress.installTask(request);
    }

    private void rememberTaskIdentity(
            long executorHandle, TaskIdentity actual) {
        TaskIdentity expected = taskIdentities.putIfAbsent(executorHandle, actual);
        if (expected != null && !expected.equals(actual)) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .PROTOCOL_DESERIALIZATION_FAILED,
                    "Native task status identity differs from its request");
        }
    }

    private TaskIdentity requireTaskIdentity(
            long executorHandle, org.pangrova.protocol.v2.TaskAttemptId attempt) {
        TaskIdentity expected = taskIdentities.get(executorHandle);
        if (expected == null || !expected.attempt().equals(attempt)) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode
                            .PROTOCOL_DESERIALIZATION_FAILED,
                    "Native task status identity differs from its request");
        }
        return expected;
    }

    private static org.pangrova.protocol.v2.TaskReport mergeProviderResources(
            org.pangrova.protocol.v2.TaskReport report, org.pangrova.protocol.v2.ResourceUsage actual) {
        var nativeUsage = report.getUsage();
        if (nativeUsage.getProviderItems() != 0 || nativeUsage.getProviderAttempts() != 0
                || nativeUsage.getProviderRequestBytes() != 0 || nativeUsage.getProviderResponseBytes() != 0
                || nativeUsage.getObservedProviderResponseBytes() != 0) {
            throw new NativeRuntimeException(org.pangrova.common.exception.PangrovaErrorCode.PROTOCOL_DESERIALIZATION_FAILED,
                    "Native reported provider resources owned by the Worker transport");
        }
        return report.toBuilder().setUsage(nativeUsage.toBuilder()
                .setProviderItems(actual.getProviderItems()).setProviderAttempts(actual.getProviderAttempts())
                .setProviderRequestBytes(actual.getProviderRequestBytes())
                .setProviderResponseBytes(actual.getProviderResponseBytes())
                .setObservedProviderResponseBytes(actual.getObservedProviderResponseBytes())
                .setMediaBytes(Math.addExact(nativeUsage.getMediaBytes(), actual.getMediaBytes()))).build();
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

    /** Joins all handles; failed cleanup throws and remains tracked for retry. */
    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            closed = true;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        NativeRuntimeException failure = null;
        for (long handle : activeHandles.keySet()) {
            try {
                releaseTask(handle);
            } catch (Throwable e) {
                LOG.error("event=close_release_failed executorHandle={}", handle, e);
                if (failure == null) {
                    failure = new NativeRuntimeException(
                            org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                            "Native executor cleanup did not complete; tracked handles require retry", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (activeHandles.isEmpty()) {
            if (externalShuffleEgress != null) { externalShuffleEgress.close(); }
            handleLocks.remove(0L);
        }
        if (failure != null) { throw failure; }
    }

    private void releaseExternalShuffle(long executorHandle) {
        TaskIdentity identity = externalShuffleTasks.get(executorHandle);
        if (identity != null) {
            externalShuffleEgress.releaseTask(identity.queryId(), identity.taskId());
            externalShuffleTasks.remove(executorHandle, identity);
        }
    }

    private record TaskIdentity(String tenantId, org.pangrova.protocol.v2.TaskAttemptId attempt) {
        String queryId() { return attempt.getQueryOrJobId(); }
        String taskId() { return attempt.getTaskId(); }
    }
}
