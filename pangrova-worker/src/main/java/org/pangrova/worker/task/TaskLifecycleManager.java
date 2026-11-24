package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.enums.SpanKind;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.worker.result.ResultTransport;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.api.ShuffleServerHandle;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.worker.shuffle.NativeShuffleProducerRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Manages task execution lifecycle: creation, start, completion, error handling,
 * cancellation, and resource release.
 *
 * <p>Extracted from TaskManager to isolate lifecycle orchestration from
 * task registry and state machine concerns.</p>
 *
 * <p>Concurrency strategy: stateless — all mutable state lives in TaskInfo.
 * Thread safety delegated to TaskManager's ConcurrentHashMap registry
 * and per-task AtomicReference state.</p>
 */
@ThreadSafe
final class TaskLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(TaskLifecycleManager.class);

    private final TaskManagerContext deps;
    private final TaskMemoryRequestor memoryRequestor;
    private final TaskMemoryAllocator memoryAllocator;
    private final TaskStatusCollector statusCollector;
    private final TaskCompletionMonitor completionMonitor;
    private final NativeShuffleProducerRegistration shuffleProducers;
    private final ResultTransport resultTransport;
    private final ExchangeTransport<TaskAttemptId, ExchangeInputBinding> exchangeTransport;
    private final TaskResourceReleaser resourceReleaser;
    private volatile org.pangrova.worker.write.NativeWriteExecutor writeExecutor;

    /** Runtime quota enforcer, set after construction via {@link #setQuotaEnforcer}. */
    private volatile TenantExecutionQuotaEnforcer quotaEnforcer;

    TaskLifecycleManager(TaskManagerContext deps,
                         TaskMemoryRequestor memoryRequestor,
                         TaskMemoryAllocator memoryAllocator,
                         TaskStatusCollector statusCollector) {
        this.deps = deps;
        this.memoryRequestor = memoryRequestor;
        this.memoryAllocator = memoryAllocator;
        this.statusCollector = statusCollector;
        this.completionMonitor = new TaskCompletionMonitor(deps, statusCollector);
        this.shuffleProducers = deps.shuffleProducers();
        this.resultTransport = deps.resultTransport();
        this.exchangeTransport = deps.exchangeTransport();
        this.resourceReleaser = new TaskResourceReleaser(
                shuffleProducers, deps, this::releaseMemoryIfNeeded);
    }

    void setQuotaEnforcer(TenantExecutionQuotaEnforcer enforcer) {
        this.quotaEnforcer = enforcer;
        this.completionMonitor.setQuotaEnforcer(enforcer);
    }

    void setWriteExecutor(org.pangrova.worker.write.NativeWriteExecutor executor) {
        this.writeExecutor = executor;
    }

    /**
     * Reserves the exact serialized-buffer admission budget before the task is
     * visible or receives HTTP success. Canonical Native memory is a hard
     * per-task growth limit charged by the Native allocation callback, not
     * pre-reserved resident memory. A rejected task has no Native executor.
     */
    void reserveAdmissionResources(TaskInfo info) {
        memoryRequestor.requestMemoryForTask(info.memoryAccount());
        try {
            memoryRequestor.requestBufferMemory(info.memoryAccount());
        } catch (RuntimeException failure) {
            memoryRequestor.releaseTaskMemory(info.memoryAccount());
            throw failure;
        }
    }

    /**
     * Executes a task: build protobuf, create native executor, start task, wire result pipeline.
     *
     * <p>Required Native serving and result/exchange transports must be wired before
     * starting the corresponding task paths.</p>
     *
     * @param taskId   worker-assigned task identifier
     * @param info     task tracking record
     * @param admission validated immutable Worker submission
     * @param callback lifecycle event callbacks (state transition, index cleanup, completion)
     * @return the original handled setup failure, or null after successful setup
     */
    RuntimeException executeTask(
            String taskId, TaskInfo info, ValidatedTaskSubmission admission, LifecycleCallback callback,
            CompletableFuture<Void> outputReady) {
        if (info.isWorkerExecuted()) {
            return executeWorkerExecutedTask(taskId, info, admission, callback);
        }
        boolean creationBegun = false;
        boolean creationDelegated = false;
        try {
            requireTaskTransports(info);
            requireNotCancelled(info);
            info.beginNativeCreation();
            creationBegun = true;
            bindWriteResources(taskId, info);
            creationDelegated = true;
            long handle = createExecutor(taskId, info, admission);
            Consumer<String> onCompleted = tid -> completionMonitor.onTaskCompleted(
                    tid, info, callback, () -> releaseAllResources(info, taskId));
            Consumer<Exception> onError = e -> completionMonitor.onTaskError(
                    taskId, info, e, callback, () -> releaseAllResources(info, taskId));

            // All runtime inputs and producer endpoints are bound before the
            // Native task can create drivers. This removes the ExchangeSource
            // construction race and makes startup fail atomically.
            wireExchangeSources(info, handle);

            // Start task-owned Native serving before its completion monitor
            if (shuffleProducers != null && isComputeTaskWithDirectShuffle(info)) {
                startShuffleViaSpi(info, handle, onError);
            }

            startExecutor(taskId, info, handle, callback, outputReady);

            routeCompletionPipeline(taskId, info, handle, callback, onCompleted, onError, outputReady);
            return null;
        } catch (RuntimeException | Error failure) {
            return handleSetupFailure(taskId, info, callback, failure, creationBegun, creationDelegated);
        }
    }

    /**
     * Starts a Worker-JVM task without entering any Native creation, memory,
     * exchange, shuffle, or resource-drain branch.
     */
    private RuntimeException executeWorkerExecutedTask(
            String taskId, TaskInfo info, ValidatedTaskSubmission admission, LifecycleCallback callback) {
        try {
            requireNotCancelled(info);
            long handle = deps.taskExecutor().createAdmittedTask(admission);
            if (handle <= 0) {
                throw new IllegalStateException(
                        "Worker-executed task creation returned no positive owner");
            }
            info.setExecutorHandle(handle);
            Consumer<String> onCompleted = tid -> completionMonitor.onTaskCompleted(
                    tid, info, callback, () -> releaseAllResources(info, taskId));
            Consumer<Exception> onError = e -> completionMonitor.onTaskError(
                    taskId, info, e, callback, () -> releaseAllResources(info, taskId));
            startExecutor(taskId, info, handle, callback, null);
            completionMonitor.startCompletionMonitor(taskId, info, handle, onCompleted, onError);
            return null;
        } catch (RuntimeException | Error failure) {
            return handleSetupFailure(taskId, info, callback, failure, false, false);
        }
    }

    private RuntimeException handleSetupFailure(String taskId, TaskInfo info, LifecycleCallback callback,
            Throwable failure, boolean creationBegun, boolean creationDelegated) {
        if (creationBegun && !creationDelegated) info.nativeCreationRolledBackBeforeEntry();
        Exception reportable = failure instanceof Exception exception ? exception
                : new IllegalStateException("Task setup failed", failure);
        try {
            completionMonitor.onTaskError(taskId, info, reportable, callback,
                    () -> releaseAllResources(info, taskId));
        } catch (RuntimeException | Error cleanup) {
            if (cleanup != failure) failure.addSuppressed(cleanup);
        }
        if (failure instanceof Error error) throw error;
        return (RuntimeException) failure;
    }

    private void bindWriteResources(String taskId, TaskInfo info) {
        if (!info.isWriteTask() || info.isWorkerExecuted()) {
            return;
        }
        var executor = writeExecutor;
        if (executor == null) {
            throw new IllegalStateException(
                    "Write task rejected because connector runtime is not ready");
        }
        executor.bindTask(canonicalWriteTaskId(taskId, info), info.request().taskRequest());
    }

    private void releaseAllResources(TaskInfo info, String taskId) {
        resourceReleaser.releaseAllResources(info, taskId, () -> {
            var executor = writeExecutor;
            if (executor != null) {
                executor.releaseTask(canonicalWriteTaskId(taskId, info));
            }
        });
    }

    private static String canonicalWriteTaskId(String fallbackTaskId, TaskInfo info) {
        if (info.request() != null
                && info.request().taskRequest().hasAttempt()
                && !info.request().taskRequest().getAttempt().getTaskId().isBlank()) {
            return info.request().taskRequest().getAttempt().getTaskId();
        }
        return fallbackTaskId;
    }

    /**
     * Routes a started task to its completion monitor. Result tasks retain
     * their real transport and resource owner after Native completion.
     */
    private void routeCompletionPipeline(String taskId, TaskInfo info, long handle,
                                         LifecycleCallback callback,
                                         Consumer<String> onCompleted,
                                         Consumer<Exception> onError,
                                         CompletableFuture<Void> outputReady) {
        // Native transport mode: all tasks use completion monitor.
        // The native engine handles data routing autonomously:
        //   - COMPUTE stages route via shuffle transport
        //   - OUTPUT stages route via NativeResultServer
        //   - Write tasks execute autonomously
        if (resultTransport != null) {
            LOG.info("event=native_transport_task_start taskId={} taskType={}",
                    taskId, info.taskType());
            if (isResultTask(info)) {
                monitorResultTask(taskId, info, handle, callback, outputReady);
            } else {
                completionMonitor.startCompletionMonitor(taskId, info, handle, onCompleted, onError);
            }
            return;
        }

        if (info.isWriteTask()) {
            completionMonitor.startCompletionMonitor(taskId, info, handle, onCompleted, onError);
            return;
        }

        // No native transport and not a write/result task — this is an invalid
        // configuration. In production, native transport is always available.
        throw new IllegalStateException(
                "No result transport configured for non-write task. "
                + "taskId=" + taskId + " queryId=" + info.queryId());
    }

    /**
     * Starts direct serving through the Native adapter for this task handle.
     */
    private void startShuffleViaSpi(TaskInfo info, long handle,
                                    Consumer<Exception> onError) {
        if (!isComputeTaskWithDirectShuffle(info)) {
            return;
        }
        var shuffle = info.request().taskRequest().getExecutionOptions().getShuffle();
        ShuffleServerHandle shuffleHandle = shuffleProducers.registerProducer(
                handle, shuffle.getPartitionCount());
        info.setShuffleServerHandle(shuffleHandle);
        LOG.info("event=native_shuffle_producer_registered taskId={} host={} port={}",
                info.taskId(), shuffleHandle.host(), shuffleHandle.port());
    }

    private void wireExchangeSources(TaskInfo info, long handle) {
        if (info.request() == null) {
            return;
        }
        var request = info.request().taskRequest();
        var bindings = request.getExchangeInputsList();
        if (bindings.isEmpty()) {
            return;
        }
        exchangeTransport.bindInputs(handle, request.getAttempt(), bindings);
        LOG.info("event=exchange_sources_wired taskId={} sourceCount={}",
                info.taskId(), bindings.stream()
                        .mapToInt(binding -> binding.hasDirect()
                                ? binding.getDirect().getSourcesCount() : 1)
                        .sum());
    }

    private boolean isComputeTaskWithShuffle(TaskInfo info) {
        return "COMPUTE".equalsIgnoreCase(info.taskType())
                && info.request() != null
                && info.request().taskRequest().hasExecutionOptions()
                && info.request().taskRequest().getExecutionOptions().hasShuffle();
    }

    private boolean isComputeTaskWithDirectShuffle(TaskInfo info) {
        return isComputeTaskWithShuffle(info)
                && isDirectFlightShuffle(
                        info.request().taskRequest().getExecutionOptions().getShuffle());
    }

    static boolean isDirectFlightShuffle(org.pangrova.protocol.v2.ShuffleTaskOptions shuffle) {
        // A remote execution binding owns the serialized queue through its
        // egress pump. Registering a Flight demuxer as well would race it for
        // batches and silently truncate one of the two data paths.
        return !shuffle.hasExecutionBinding();
    }

    /**
     * Determines if a task belongs to an autonomous stage that requires no Java
     * data threads. Autonomous stages run entirely in native code and only need
     * the completion monitor for lifecycle tracking.
     */
    private boolean isAutonomousStage(TaskInfo info) {
        return resultTransport != null || isComputeTaskWithShuffle(info);
    }

    /**
     * Starts a completion monitor for an autonomous task. Delegates to
     * {@link TaskCompletionMonitor#startCompletionMonitor}.
     */
    private void startCompletionMonitor(String taskId, TaskInfo info, long handle,
                                        java.util.function.Consumer<String> onCompleted,
                                        java.util.function.Consumer<Exception> onError) {
        completionMonitor.startCompletionMonitor(taskId, info, handle, onCompleted, onError);
    }

    /**
     * Requests Native cancellation. The completion monitor's synchronous
     * releaseTask call joins the Native driver before terminal reporting.
     */
    void cancelNativeExecutor(TaskInfo info) {
        if (deps == null || deps.taskExecutor() == null) {
            return;
        }
        long handle = info.executorHandle();
        if (handle == 0) {
            return;
        }
        if (info.isWorkerExecuted()) {
            deps.taskExecutor().cancelTask(handle);
            return;
        }
        RuntimeException failure = null;
        if (exchangeTransport != null) {
            try {
                exchangeTransport.releaseInputs(handle);
            } catch (RuntimeException observed) {
                failure = observed;
            }
        }
        // Cancellation must still reach the actual Native owner if reader revocation failed.
        try {
            deps.taskExecutor().cancelTask(handle);
        } catch (RuntimeException observed) {
            if (failure == null) {
                failure = observed;
            } else if (failure != observed) {
                failure.addSuppressed(observed);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Releases memory allocated for a task (both hierarchical and arbitrator).
     *
     * <p>Thread-safety: {@link org.pangrova.worker.resource.memory.MemoryArbitrator}
     * uses {@link java.util.concurrent.atomic.AtomicLong} CAS for all counter
     * updates. After this method returns, the released bytes are immediately
     * visible to any thread reading
     * {@link org.pangrova.worker.resource.memory.MemoryArbitrator#getAvailableMemory()},
     * including the heartbeat payload supplier. No additional memory barrier
     * or synchronization is required.</p>
     */
    void releaseMemoryIfNeeded(TaskInfo info) {
        if (memoryRequestor == null) {
            return;
        }
        memoryRequestor.releaseTaskMemory(info.memoryAccount());
        memoryRequestor.releaseBufferMemory(info.memoryAccount());
        LOG.debug("event=task_memory_released taskId={} queryId={}",
                info.taskId(), info.queryId());
    }

    // ── Lifecycle flow methods ──

    private long createExecutor(String taskId, TaskInfo info, ValidatedTaskSubmission admission) {
        long handle = TaskExecutorCreation.create(info, deps.taskExecutor(), admission, () -> {
            requireNotCancelled(info);
            traceTaskExecute(taskId, info);
        });
        requireNotCancelled(info);
        // Propagate memory limit before startTask(), which launches a
        // background thread that calls NativeMemoryGovernor::
        // createTaskPool() → addRootPool(). If propagateMemoryLimitToNative
        // runs concurrently (it reads from the same MemoryManager via
        // usedMemory()/getTotalBytes()), the concurrent read+write can
        // corrupt the native engine's internal F14 hash table.
        propagateMemoryLimitToNative(info);
        return handle;
    }

    private static void requireNotCancelled(TaskInfo info) {
        if (info.getState() == TaskState.CANCELLING) {
            throw new java.util.concurrent.CancellationException(
                    "Task was cancelled before Native start: " + info.taskId());
        }
    }

    private void requireTaskTransports(TaskInfo info) {
        var request = info.request().taskRequest();
        if (shuffleProducers != null) shuffleProducers.validateTask(request);
        if (!info.isWriteTask() && resultTransport == null) {
            throw new IllegalStateException("Native result transport is unavailable: task=" + info.taskId());
        }
        if (request.getExchangeInputsCount() != 0 && exchangeTransport == null) {
            throw new IllegalStateException("Native exchange transport is unavailable: task=" + info.taskId());
        }
        if (isComputeTaskWithDirectShuffle(info) && shuffleProducers == null) {
            throw new IllegalStateException("Native shuffle transport is unavailable: task=" + info.taskId());
        }
    }

    private void startExecutor(String taskId, TaskInfo info, long handle,
                               LifecycleCallback callback, CompletableFuture<Void> outputReady) {
        requireNotCancelled(info);
        // Admission is checked before any Native driver can consume data.
        TenantExecutionQuotaEnforcer enforcer = this.quotaEnforcer;
        if (enforcer != null) {
            enforcer.checkDuringExecution(taskId);
        }
        if (outputReady == null) {
            deps.taskExecutor().startTask(handle);
        } else {
            deps.taskExecutor().startResultTask(handle, () -> outputReady.complete(null));
        }
        if (!callback.transition(taskId, TaskState.RUNNING)) {
            if (info.getState() == TaskState.CANCELLING) {
                deps.taskExecutor().cancelTask(handle);
                throw new java.util.concurrent.CancellationException(
                        "Task was cancelled during Native start: " + taskId);
            }
            throw new IllegalStateException(
                    "Task state changed during Native start: " + taskId);
        }
        if (statusCollector != null) {
            statusCollector.reportStatus(info, "RUNNING", null);
        }
        LOG.info("event=task_started taskId={} queryId={} executorHandle={} isResultTask={}",
                taskId, info.queryId(), handle, isResultTask(info));
    }

    /**
     * Identifies final-stage tasks whose Velox result callback feeds the task-owned Flight queue.
     */
    private static boolean isResultTask(TaskInfo info) {
        var request = info.request();
        return request != null
                && request.taskRequest().hasExecutionOptions()
                && request.taskRequest().getExecutionOptions().getDirectResultOutput();
    }

    /**
     * Keeps the result buffer alive for retrieval while waiting for Native to
     * finish. Only the completion monitor can freeze a canonical terminal
     * report; result availability itself is not a terminal task state.
     */
    private void monitorResultTask(String taskId, TaskInfo info, long handle,
                                   LifecycleCallback callback, CompletableFuture<Void> outputReady) {
        completionMonitor.startCompletionMonitor(taskId, info, handle,
                tid -> completionMonitor.onTaskCompleted(tid, info, callback, () -> {
                    if (info.getState() == TaskState.CANCELLING) {
                        releaseAllResources(info, taskId);
                        return;
                    }
                    // Do not release the executor: it still owns the result buffer.
                    // The result consumer or worker drain releases it later.
                    resourceReleaser.unregisterShuffleProducerIfNeeded(info);
                }),
                failure -> {
                    // The monitor has already captured the exact Native report. Unblock the submitter
                    // even when subsequent cleanup or terminal HTTP delivery is delayed.
                    failResultAcknowledgement(info, outputReady, failure);
                    completionMonitor.onTaskError(taskId, info, failure, callback,
                            () -> releaseAllResources(info, taskId));
                });
    }

    private static void failResultAcknowledgement(
            TaskInfo info, CompletableFuture<Void> outputReady, Exception failure) {
        if (outputReady == null || outputReady.isDone()) return;
        if (info.getState() == TaskState.CANCELLING || info.getState() == TaskState.CANCELLED) {
            outputReady.completeExceptionally(new java.util.concurrent.CancellationException(
                    "Task was cancelled before Flight registration: " + info.taskId()));
            return;
        }
        var evidence = info.terminalTaskEvidence();
        var nativeError = evidence == null ? null : evidence.report().getError();
        outputReady.completeExceptionally(nativeError != null && !nativeError.getCode().isBlank()
                ? new IllegalStateException(nativeError.getCode() + ": " + nativeError.getMessage(), failure)
                : failure);
    }

    /** Releases the actual owner retained for a terminal result. */
    void releaseFinishedTaskExecutor(TaskInfo info) {
        releaseAllResources(info, info.taskId());
        statusCollector.reportReleasedTerminal(info);
    }

    java.util.concurrent.CompletionStage<Void> releaseResultTaskExecutor(
            TaskInfo info, java.util.function.Consumer<TaskInfo> captureReceipt) {
        releaseAllResources(info, info.taskId());
        captureReceipt.accept(info);
        return statusCollector.reportReleasedTerminalAsync(info);
    }

    void retryInitialResultStatus(TaskInfo info) {
        statusCollector.retryInitialResultStatus(info);
    }

    void releaseRetiredTaskExecutor(TaskInfo info, LifecycleCallback callback) {
        TaskRetirement.release(info, callback, statusCollector, () -> releaseAllResources(info, info.taskId()));
    }

    private void traceTaskExecute(String taskId, TaskInfo info) {
        TracingProvider tracing = deps.tracingProvider();
        if (tracing == null) {
            return;
        }
        Span span = tracing.startSpan("pangrova.task.execute", SpanKind.INTERNAL);
        span.setAttribute("queryId", info.queryId() != null ? info.queryId() : "")
            .setAttribute("taskId", taskId);
        if (info.hasTaskMetadata()) {
            span.setAttribute("stageId", info.stageId());
        }
        span.end();
    }

    private void propagateMemoryLimitToNative(TaskInfo info) {
        if (memoryAllocator != null && !info.isWorkerExecuted()) {
            memoryAllocator.propagateMemoryLimitToNative(info.memoryAccount(), info.executorHandle());
        }
    }

    /**
     * Callback interface for lifecycle events that require TaskManager coordination.
     * Decouples lifecycle orchestration from registry/state-machine ownership.
     */
    interface LifecycleCallback {
        /** Drive a state transition on the task registry. */
        boolean transition(String taskId, TaskState target);

        /** Remove coordinator taskId mapping on terminal state. */
        void clearCoordinatorIndex(TaskInfo info);

        /** Starts result-release observation after publishing the successful execution. */
        void watchFinishedResult(TaskInfo info);

        /** Notify completion callback with queryId. */
        void notifyCompletion(String queryId);
    }
}
