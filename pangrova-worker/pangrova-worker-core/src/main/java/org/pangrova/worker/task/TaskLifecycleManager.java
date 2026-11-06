package org.pangrova.worker.task;

import org.pangrova.worker.task.state.TaskState;

import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.tracing.SpanKind;
import org.pangrova.observability.spi.tracing.TracingProvider;
import org.pangrova.worker.result.ResultTransport;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.api.ShuffleServerHandle;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.shuffle.adapter.nativebridge.NativeShuffleTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final NativeShuffleTransport shuffleTransport;
    private final ResultTransport resultTransport;
    private final ExchangeTransport exchangeTransport;
    private final TaskResourceReleaser resourceReleaser;
    private final TaskResourceDrainProbe resourceDrainProbe;
    private volatile org.pangrova.worker.nativebridge.jni.write.NativeWriteExecutor writeExecutor;

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
        this.shuffleTransport = deps.shuffleTransport();
        this.resultTransport = deps.resultTransport();
        this.exchangeTransport = deps.exchangeTransport();
        this.resourceReleaser = new TaskResourceReleaser(
                shuffleTransport, deps, this::releaseMemoryIfNeeded);
        this.resourceDrainProbe = new TaskResourceDrainProbe(deps.taskExecutor());
    }

    void setQuotaEnforcer(TenantExecutionQuotaEnforcer enforcer) {
        this.quotaEnforcer = enforcer;
        this.completionMonitor.setQuotaEnforcer(enforcer);
    }

    void setWriteExecutor(org.pangrova.worker.nativebridge.jni.write.NativeWriteExecutor executor) {
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
     * @param callback lifecycle event callbacks (state transition, index cleanup, completion)
     */
    void executeTask(String taskId, TaskInfo info, LifecycleCallback callback) {
        boolean creationBegun = false;
        boolean creationDelegated = false;
        try {
            requireTaskTransports(info);
            requireNotCancelled(info);
            info.beginNativeCreation();
            creationBegun = true;
            bindWriteResources(taskId, info);
            creationDelegated = true;
            long handle = createExecutor(taskId, info);
            Consumer<String> onCompleted = tid -> completionMonitor.onTaskCompleted(
                    tid, info, callback, () -> releaseAllResources(info, taskId));
            Consumer<Exception> onError = e -> completionMonitor.onTaskError(
                    taskId, info, e, callback, () -> releaseAllResources(info, taskId));

            // All runtime inputs and producer endpoints are bound before the
            // Native task can create drivers. This removes the ExchangeSource
            // construction race and makes startup fail atomically.
            wireExchangeSources(info, handle);

            // Start task-owned Native serving before its completion monitor
            if (shuffleTransport != null && isComputeTaskWithDirectShuffle(info)) {
                startShuffleViaSpi(info, handle, onError);
            }

            startExecutor(taskId, info, handle, callback);

            routeCompletionPipeline(taskId, info, handle, callback, onCompleted, onError);
        } catch (RuntimeException | Error failure) {
            handleSetupFailure(taskId, info, callback, failure, creationBegun, creationDelegated);
        }
    }

    private void handleSetupFailure(String taskId, TaskInfo info, LifecycleCallback callback,
                                   Throwable failure, boolean creationBegun, boolean creationDelegated) {
        if (creationBegun && !creationDelegated) info.nativeCreationRolledBackBeforeEntry();
        Exception reportable = failure instanceof Exception exception ? exception
                : new IllegalStateException("Task setup failed", failure);
        try {
            completionMonitor.onTaskError(taskId, info, reportable, callback,
                    () -> releaseAllResources(info, taskId));
        } catch (RuntimeException | Error cleanup) {
            if (cleanup != failure) failure.addSuppressed(cleanup);
            if (failure instanceof Error error) throw error;
            throw cleanup;
        }
        if (failure instanceof Error error) throw error;
    }

    private void bindWriteResources(String taskId, TaskInfo info) {
        if (!info.isWriteTask()) {
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
                                         Consumer<Exception> onError) {
        // Native transport mode: all tasks use completion monitor.
        // The native engine handles data routing autonomously:
        //   - COMPUTE stages route via shuffle transport
        //   - OUTPUT stages route via NativeResultServer
        //   - Write tasks execute autonomously
        if (resultTransport != null) {
            LOG.info("event=native_transport_task_start taskId={} taskType={}",
                    taskId, info.descriptor() != null
                            ? info.descriptor().taskType() : "unknown");
            if (isResultTask(info)) {
                monitorResultTask(taskId, info, handle, callback);
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
        ShuffleServerHandle shuffleHandle = shuffleTransport.startServer(
                handle, shuffle.getPartitionCount());
        info.setShuffleServerHandle(shuffleHandle);
        LOG.info("event=shuffle_server_started_via_spi taskId={} host={} port={}",
                info.taskId(), shuffleHandle.host(), shuffleHandle.port());
    }

    private void wireExchangeSources(TaskInfo info, long handle) {
        if (info.request() == null) {
            return;
        }
        var bindings = info.request().taskRequest().getExchangeInputsList();
        if (bindings.isEmpty()) {
            return;
        }
        var inputs = org.pangrova.worker.exchange.source.ExchangeSourceConverter
                .toExchangeInputs(
                        bindings, info.request().taskRequest().getAttempt(),
                        info.request().taskRequest().getDeadlineEpochMs());
        var consumerAttempt = org.pangrova.worker.exchange.source.ExchangeSourceConverter
                .toExchangeTaskAttempt(info.request().taskRequest().getAttempt());
        exchangeTransport.bindInputs(handle, consumerAttempt, inputs);
        LOG.info("event=exchange_sources_wired taskId={} sourceCount={}",
                info.taskId(), bindings.stream()
                        .mapToInt(binding -> binding.hasDirect()
                                ? binding.getDirect().getSourcesCount() : 1)
                        .sum());
    }

    private boolean isComputeTaskWithShuffle(TaskInfo info) {
        var desc = info.descriptor();
        return desc != null
                && "COMPUTE".equalsIgnoreCase(desc.taskType())
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

    private long createExecutor(String taskId, TaskInfo info) {
        long handle = TaskExecutorCreation.create(info, deps.taskExecutor(), () -> {
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
        if (!info.isWriteTask() && resultTransport == null) {
            throw new IllegalStateException("Native result transport is unavailable: task=" + info.taskId());
        }
        if (request.getExchangeInputsCount() != 0 && exchangeTransport == null) {
            throw new IllegalStateException("Native exchange transport is unavailable: task=" + info.taskId());
        }
        if (isComputeTaskWithDirectShuffle(info) && shuffleTransport == null) {
            throw new IllegalStateException("Native shuffle transport is unavailable: task=" + info.taskId());
        }
    }

    private void startExecutor(String taskId, TaskInfo info, long handle,
                               LifecycleCallback callback) {
        requireNotCancelled(info);
        // Admission is checked before any Native driver can consume data.
        TenantExecutionQuotaEnforcer enforcer = this.quotaEnforcer;
        if (enforcer != null) {
            enforcer.checkDuringExecution(taskId);
        }
        deps.taskExecutor().startTask(handle);
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
     * Returns true if this task is a final-stage result task (OUTPUT type).
     * When true, the native side wraps the plan with PartitionedOutputNode
     * and routes output through the native OutputBufferManager.
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
                                   LifecycleCallback callback) {
        completionMonitor.startCompletionMonitor(taskId, info, handle,
                tid -> completionMonitor.onTaskCompleted(tid, info, callback, () -> {
                    if (info.getState() == TaskState.CANCELLING) {
                        releaseAllResources(info, taskId);
                        return;
                    }
                    // Do not release the executor: it still owns the result buffer.
                    // The result consumer or worker drain releases it later.
                    resourceReleaser.stopShuffleServerIfNeeded(info);
                }),
                e -> completionMonitor.onTaskError(
                        taskId, info, e, callback,
                        () -> releaseAllResources(info, taskId)));
    }

    /** Releases the actual owner retained for a terminal result. */
    void releaseFinishedTaskExecutor(TaskInfo info) {
        releaseAllResources(info, info.taskId());
        statusCollector.reportReleasedTerminal(info);
    }

    void releaseRetiredTaskExecutor(TaskInfo info, LifecycleCallback callback) {
        TaskRetirement.release(info, callback, statusCollector, () -> releaseAllResources(info, info.taskId()));
    }

    boolean resultResourcesDrained(TaskInfo info) {
        return resourceDrainProbe.resultDrained(info);
    }

    private void traceTaskExecute(String taskId, TaskInfo info) {
        TracingProvider tracing = deps.tracingProvider();
        if (tracing == null) {
            return;
        }
        Span span = tracing.startSpan("pangrova.task.execute", SpanKind.INTERNAL);
        span.setAttribute("queryId", info.queryId() != null ? info.queryId() : "")
            .setAttribute("taskId", taskId);
        if (info.descriptor() != null) {
            span.setAttribute("stageId", info.descriptor().stageId());
        }
        span.end();
    }

    private void propagateMemoryLimitToNative(TaskInfo info) {
        if (memoryAllocator != null) {
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

        /** Notify completion callback with queryId. */
        void notifyCompletion(String queryId);
    }
}
