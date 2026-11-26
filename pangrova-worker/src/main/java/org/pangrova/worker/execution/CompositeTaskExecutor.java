package org.pangrova.worker.execution;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes canonical tasks to either the Native engine or a Worker-JVM backend.
 *
 * <p>The Native executor is the sole owner of native handles. JVM handles are
 * private synthetic values translated to the backend's positive handle; no
 * Native command, memory limit, or drain probe is ever sent with a synthetic
 * handle.</p>
 */
@Experimental
public final class CompositeTaskExecutor implements TaskExecutor, AutoCloseable {

    private final TaskExecutor nativeExecutor;
    private final WorkerExecutedTaskBackend workerBackend;
    private final ConcurrentHashMap<Long, Long> workerHandles = new ConcurrentHashMap<>();
    private final AtomicLong nextWorkerHandle = new AtomicLong(Long.MAX_VALUE / 2);

    public CompositeTaskExecutor(
            TaskExecutor nativeExecutor,
            WorkerExecutedTaskBackend workerBackend) {
        this.nativeExecutor = java.util.Objects.requireNonNull(nativeExecutor, "nativeExecutor");
        this.workerBackend = java.util.Objects.requireNonNull(workerBackend, "workerBackend");
    }

    TaskExecutor nativeDelegate() {
        return nativeExecutor;
    }

    /** Returns whether the installed Worker JVM backend owns this task kind. */
    public boolean supportsWorkerTask(org.pangrova.protocol.v2.TaskKind kind) {
        return workerBackend.supports(kind);
    }

    @Override
    public long createTask(TaskRequest request) {
        return supportsWorkerTask(request.getTaskKind())
                ? createWorkerTask(request) : nativeExecutor.createTask(request);
    }

    @Override
    public long createAdmittedTask(ValidatedTaskSubmission admission) {
        var request = admission.submission().taskRequest();
        return supportsWorkerTask(request.getTaskKind())
                ? createWorkerTask(request) : nativeExecutor.createAdmittedTask(admission);
    }

    private long createWorkerTask(TaskRequest request) {
        long backendHandle = workerBackend.create(request);
        if (backendHandle <= 0) {
            throw new IllegalStateException("Worker-executed backend returned no positive handle");
        }
        long synthetic = nextWorkerHandle.getAndDecrement();
        workerHandles.put(synthetic, backendHandle);
        return synthetic;
    }

    @Override
    public void startTask(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        if (backendHandle == null) {
            nativeExecutor.startTask(executorHandle);
        } else {
            workerBackend.start(backendHandle);
        }
    }

    @Override
    public void startResultTask(long executorHandle, Runnable onReady) {
        if (workerHandles.containsKey(executorHandle)) {
            throw new IllegalArgumentException("A Worker-JVM task cannot own a Native result endpoint");
        }
        nativeExecutor.startResultTask(executorHandle, onReady);
    }

    @Override
    public Optional<byte[]> releaseTask(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        if (backendHandle == null) {
            return nativeExecutor.releaseTask(executorHandle);
        }
        try {
            Optional<byte[]> released = workerBackend.release(backendHandle);
            workerHandles.remove(executorHandle, backendHandle);
            return released;
        } catch (RuntimeException | Error failure) {
            // Retain the mapping so the caller can retry the same actual owner.
            throw failure;
        }
    }

    @Override
    public void cancelTask(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        if (backendHandle == null) {
            nativeExecutor.cancelTask(executorHandle);
        } else {
            workerBackend.cancel(backendHandle);
        }
    }

    @Override
    public byte[] executeCommand(long executorHandle, byte[] commandProto) {
        if (workerHandles.containsKey(executorHandle)) {
            throw new UnsupportedOperationException(
                    "Worker-executed tasks do not accept Native control commands");
        }
        return nativeExecutor.executeCommand(executorHandle, commandProto);
    }

    @Override
    public byte[] pushExchangeChunk(long executorHandle, byte[] metadata, byte[] data) {
        if (workerHandles.containsKey(executorHandle)) {
            throw new UnsupportedOperationException("Worker-executed tasks have no Native exchange ingress");
        }
        return nativeExecutor.pushExchangeChunk(executorHandle, metadata, data);
    }

    @Override
    public boolean isFinished(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        return backendHandle == null
                ? nativeExecutor.isFinished(executorHandle)
                : workerBackend.isFinished(backendHandle);
    }

    @Override
    public boolean awaitFinished(long executorHandle, int waitMillis) throws InterruptedException {
        Long backendHandle = workerHandles.get(executorHandle);
        return backendHandle == null
                ? nativeExecutor.awaitFinished(executorHandle, waitMillis)
                : workerBackend.awaitFinished(backendHandle, waitMillis);
    }

    @Override
    public byte[] getTaskStatus(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        return backendHandle == null
                ? nativeExecutor.getTaskStatus(executorHandle)
                : workerBackend.getTaskStatus(backendHandle);
    }

    @Override
    public byte[] getTaskStats(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        return backendHandle == null
                ? nativeExecutor.getTaskStats(executorHandle)
                : workerBackend.getTaskStats(backendHandle);
    }

    @Override
    public java.util.List<byte[]> getTaskCommitFragments(long executorHandle) {
        Long backendHandle = workerHandles.get(executorHandle);
        return backendHandle == null
                ? nativeExecutor.getTaskCommitFragments(executorHandle)
                : workerBackend.getCommitFragments(backendHandle);
    }

    @Override
    public boolean isWorkerExecuted(long executorHandle) {
        return workerHandles.containsKey(executorHandle);
    }

    @Override
    public void close() {
        workerBackend.close();
    }

}
