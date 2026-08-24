package org.pangrova.worker.task;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TracingProvider;
import org.pangrova.spi.execution.ResultTransport;
import org.pangrova.shuffle.exchange.ExchangeTransport;
import org.pangrova.shuffle.ShuffleServerHandle;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.shuffle.ShuffleTransport;
import org.pangrova.worker.jni.NativeRuntimeException;
import org.pangrova.worker.task.convert.TaskRequestBuilder;
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
    private final ShuffleTransport shuffleTransport;
    private final ResultTransport resultTransport;
    private final ExchangeTransport exchangeTransport;
    private final VectorSearchTaskSupport vectorSearchSupport;
    private final TaskResourceReleaser resourceReleaser;

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
        this.vectorSearchSupport = new VectorSearchTaskSupport(
                new org.pangrova.worker.execution.VectorSearchResultBuffer(),
                new org.pangrova.worker.ai.rerank.WorkerRerankerExecutor(),
                statusCollector,
                this::releaseMemoryIfNeeded);
        this.resourceReleaser = new TaskResourceReleaser(
                shuffleTransport, deps, this::releaseMemoryIfNeeded);
    }

    void setQuotaEnforcer(TenantExecutionQuotaEnforcer enforcer) {
        this.quotaEnforcer = enforcer;
        this.completionMonitor.setQuotaEnforcer(enforcer);
    }

    /**
     * Executes a task: build protobuf, create native executor, start task, wire result pipeline.
     *
     * <p>When transport SPIs are wired (non-null), uses SPI calls for shuffle, result,
     * Uses transport SPIs for shuffle, result, and exchange transport.</p>
     *
     * @param taskId   worker-assigned task identifier
     * @param info     task tracking record
     * @param callback lifecycle event callbacks (state transition, index cleanup, completion)
     */
    void executeTask(String taskId, TaskInfo info, LifecycleCallback callback) {
        try {
            // Vector search interception: if the task carries a vector search operator
            // in session properties, execute via VectorSearchTaskInterceptor and skip
            // the native Substrait plan execution. The ANN index path is faster than
            // the standard ORDER BY + LIMIT plan for high-dimensional vector data.
            if (vectorSearchSupport.tryExecuteVectorSearch(taskId, info, callback)) {
                return;
            }

            long handle = createAndStartExecutor(taskId, info, callback);
            Consumer<String> onCompleted = tid -> completionMonitor.onTaskCompleted(
                    tid, info, callback, () -> resourceReleaser.releaseAllResources(info, taskId));
            Consumer<Exception> onError = e -> completionMonitor.onTaskError(
                    taskId, info, e, callback, () -> resourceReleaser.releaseAllResources(info, taskId));

            // Wire exchange sources from the request so the native Exchange
            // operator knows where to pull upstream shuffle data from.
            wireExchangeSources(info, handle);

            // Shuffle pipeline via transport SPI (before completion monitor)
            if (shuffleTransport != null && isComputeTaskWithShuffle(info)) {
                startShuffleViaSpi(info, handle, onError);
            }

            routeCompletionPipeline(taskId, info, handle, callback, onCompleted, onError);
        } catch (Exception e) {
            completionMonitor.onTaskError(taskId, info, e, callback,
                    () -> resourceReleaser.releaseAllResources(info, taskId));
        }
    }

    /**
     * Routes a started task to its completion pipeline based on transport mode and
     * task type. Native-transport tasks go through the result transport; write/result
     * tasks without native transport fall back to the completion monitor; anything
     * else is an invalid configuration.
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
                    taskId, info.request() != null ? info.request().taskType() : "unknown");
            if (isResultTask(info)) {
                reportResultTaskReady(taskId, info, handle, callback, onCompleted, onError);
            } else {
                completionMonitor.startCompletionMonitor(taskId, info, handle, onCompleted, onError);
            }
            return;
        }

        // No native transport: write tasks and result tasks use completion monitor.
        // Production invariant: WorkerTaskBootstrap.createTaskExecutor() throws
        // BootstrapFailedException when native is unavailable, so resultTransport
        // is always non-null in production. This branch exists only for test
        // configurations that use TransportRefs.empty().
        if (info.isWriteTask() || isResultTask(info)) {
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
     * Starts shuffle pipeline via the ShuffleTransport SPI.
     */
    private void startShuffleViaSpi(TaskInfo info, long handle,
                                    Consumer<Exception> onError) {
        if (!isComputeTaskWithShuffle(info)) {
            return;
        }
        var sc = info.request().shuffleConfig();
        ShuffleServerHandle shuffleHandle = shuffleTransport.startServer(handle, sc.numPartitions());
        info.setShufflePort(shuffleHandle.port());
        LOG.info("event=shuffle_server_started_via_spi taskId={} host={} port={}",
                info.taskId(), shuffleHandle.host(), shuffleHandle.port());
        // Pre-create partition buffers for the shuffle buffer manager
        preCreatePartitionBuffers(info);
    }

    private void preCreatePartitionBuffers(TaskInfo info) {
        // No-op: native shuffle manages partition buffers in C++
    }

    private void wireExchangeSources(TaskInfo info, long handle) {
        if (exchangeTransport == null || info.request() == null) {
            return;
        }
        var descriptors = info.request().exchangeSources();
        if (descriptors == null || descriptors.isEmpty()) {
            return;
        }
        var spiSources = org.pangrova.worker.exchange.source.ExchangeSourceConverter
                .toSpiEndpoints(descriptors);
        exchangeTransport.startExchange(handle, spiSources);
        exchangeTransport.noMoreSources(handle);
        LOG.info("event=exchange_sources_wired taskId={} sourceCount={}",
                info.taskId(), spiSources.size());
    }

    private boolean isComputeTaskWithShuffle(TaskInfo info) {
        var desc = info.descriptor();
        return desc != null
                && "COMPUTE".equalsIgnoreCase(desc.taskType())
                && info.request() != null
                && info.request().shuffleConfig() != null;
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

    private static final int CANCEL_VERIFY_ATTEMPTS = 3;
    private static final long CANCEL_VERIFY_INTERVAL_MS = 200;

    /**
     * Cancels the native executor for a task and verifies that the cancellation
     * was acknowledged. Polls the task state up to {@value #CANCEL_VERIFY_ATTEMPTS}
     * times to confirm the native side has stopped execution.
     */
    void cancelNativeExecutor(TaskInfo info) {
        if (deps == null || deps.taskExecutor() == null) {
            return;
        }
        long handle = info.executorHandle();
        if (handle == 0) {
            return;
        }
        try {
            deps.taskExecutor().cancelTask(handle);
        } catch (Exception e) {
            LOG.warn("event=native_cancel_failed taskId={} error={}",
                    info.taskId(), e.getMessage());
            return;
        }

        // Wait for cancellation acknowledgement via CountDownLatch instead of busy-wait.
        try {
            boolean terminated = info.awaitTerminal(
                    CANCEL_VERIFY_ATTEMPTS * CANCEL_VERIFY_INTERVAL_MS,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!terminated) {
                TaskManager.TaskState finalState = info.getState();
                LOG.warn("event=native_cancel_unverified taskId={} handle={} "
                        + "timeout_ms={} state={}",
                        info.taskId(), handle,
                        CANCEL_VERIFY_ATTEMPTS * CANCEL_VERIFY_INTERVAL_MS,
                        finalState);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Releases memory allocated for a task (both hierarchical and arbitrator).
     *
     * <p>Thread-safety: {@link org.pangrova.resource.memory.MemoryArbitrator}
     * uses {@link java.util.concurrent.atomic.AtomicLong} CAS for all counter
     * updates. After this method returns, the released bytes are immediately
     * visible to any thread reading
     * {@link org.pangrova.resource.memory.MemoryArbitrator#getAvailableMemory()},
     * including the heartbeat payload supplier. No additional memory barrier
     * or synchronization is required.</p>
     */
    void releaseMemoryIfNeeded(TaskInfo info) {
        if (memoryRequestor == null) {
            return;
        }
        memoryRequestor.releaseTaskMemory(info);
        memoryRequestor.releaseBufferMemory(info);
        LOG.debug("event=task_memory_released taskId={} queryId={}",
                info.taskId(), info.queryId());
    }

    // ── Lifecycle flow methods ──

    private long createAndStartExecutor(String taskId, TaskInfo info,
                                        LifecycleCallback callback) {
        memoryRequestor.requestMemoryForTask(info);
        memoryRequestor.requestBufferMemory(info);
        byte[] taskRequestBytes = TaskRequestBuilder.build(info.request());
        boolean isResultTask = isResultTask(info);
        byte[] commandBytes = buildCreateExecutorCommand(taskRequestBytes, isResultTask);
        traceTaskExecute(taskId, info);
        long handle = deps.taskExecutor().createTask(commandBytes);
        validateHandle(taskId, handle);
        info.setExecutorHandle(handle);
        // Propagate memory limit BEFORE starting the task. startTask()
        // launches a background thread that calls NativeMemoryGovernor::
        // createTaskPool() → addRootPool(). If propagateMemoryLimitToNative
        // runs concurrently (it reads from the same MemoryManager via
        // usedMemory()/getTotalBytes()), the concurrent read+write can
        // corrupt the native engine's internal F14 hash table.
        propagateMemoryLimitToNative(info);
        deps.taskExecutor().startTask(handle);
        callback.transition(taskId, TaskManager.TaskState.RUNNING);
        if (statusCollector != null) {
            statusCollector.reportStatus(info, "RUNNING", null);
        }
        // Runtime quota check at task start (stage boundary)
        TenantExecutionQuotaEnforcer enforcer = this.quotaEnforcer;
        if (enforcer != null) {
            enforcer.checkDuringExecution(taskId);
        }
        LOG.info("event=task_started taskId={} queryId={} executorHandle={} isResultTask={}",
                taskId, info.queryId(), handle, isResultTask);
        return handle;
    }

    /**
     * Returns true if this task is a final-stage result task (OUTPUT type).
     * When true, the native side wraps the plan with PartitionedOutputNode
     * and routes output through the native OutputBufferManager.
     */
    private static boolean isResultTask(TaskInfo info) {
        var request = info.request();
        return request != null && "OUTPUT".equals(request.taskType());
    }

    /**
     * Reports a result task as completed immediately, breaking the deadlock
     * between DAG dispatch and result consumption.
     *
     * <p>Result tasks use PartitionedOutputNode which keeps the native task
     * in RUNNING state until all output is consumed via DoGet. The coordinator
     * only initiates DoGet after the DAG completes. Waiting for native task
     * completion here would deadlock. Instead, report FINISHED immediately
     * so the DAG can complete, then the coordinator fetches results.</p>
     *
     * <p>Native resources are NOT released here — the background completion
     * monitor releases them after DoGet finishes and the native task reaches
     * a terminal state.</p>
     */
    private void reportResultTaskReady(String taskId, TaskInfo info, long handle,
                                       LifecycleCallback callback,
                                       Consumer<String> onCompleted,
                                       Consumer<Exception> onError) {
        // Result task resource lifecycle:
        //
        // The native task is executing asynchronously (started by createAndStartExecutor).
        // It will write output to OutputBufferManager when complete. Result data retrieval reads
        // from OutputBufferManager independently.
        //
        // The native executor must NOT be released when the native task completes,
        // because releasing the executor destroys the native Task and its OutputBuffer.
        // DoGet needs the OutputBuffer alive to fetch results. The executor is released
        // later when the task is explicitly cancelled (cancelTask / drainAndWait).
        //
        // Non-executor resources (shuffle server, exchange client) are safe to release
        // on completion — they are not needed for DoGet.
        completionMonitor.startCompletionMonitor(taskId, info, handle,
                tid -> {
                    resourceReleaser.stopShuffleServerIfNeeded(info);
                    resourceReleaser.destroyExchangeClientIfNeeded(taskId);
                    releaseMemoryIfNeeded(info);
                    callback.clearCoordinatorIndex(info);
                },
                e -> completionMonitor.onTaskError(
                        taskId, info, e, callback,
                        () -> {
                            resourceReleaser.releaseAllResources(info, taskId);
                            callback.clearCoordinatorIndex(info);
                        }));

        LOG.info("event=result_task_ready_pre_report taskId={} queryId={} statusCollectorNull={}",
                taskId, info.queryId(), statusCollector == null);
        if (statusCollector != null) {
            statusCollector.reportStatus(info, "FINISHED", null);
            statusCollector.logTaskCompleted(taskId);
        }
        LOG.info("event=result_task_ready_post_report taskId={} queryId={}",
                taskId, info.queryId());
        callback.transition(taskId, TaskManager.TaskState.FINISHING);
        callback.transition(taskId, TaskManager.TaskState.FINISHED);
        callback.notifyCompletion(info.queryId());
    }

    /**
     * Wraps serialized TaskRequest bytes in a CreateExecutorCommand proto envelope.
     * Uses the generated protobuf builder to ensure wire-format correctness
     * and compile-time safety when the proto schema evolves.
     */
    private static byte[] buildCreateExecutorCommand(byte[] taskRequestBytes, boolean isResultTask) {
        return org.pangrova.protocol.v1.exchange.CreateExecutorCommand.newBuilder()
                .setTaskRequest(com.google.protobuf.ByteString.copyFrom(taskRequestBytes))
                .setIsResultTask(isResultTask)
                .build()
                .toByteArray();
    }

    /**
     * Releases the native executor for a finished result task whose executor was
     * intentionally kept alive for result data retrieval. Called during inter-class drain
     * to prevent executor handle leaks across test classes.
     */
    void releaseFinishedTaskExecutor(TaskInfo info) {
        resourceReleaser.releaseExecutorIfNeeded(info);
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

    private void validateHandle(String taskId, long handle) {
        if (handle == 0) {
            throw new NativeRuntimeException(
                    PangrovaErrorCode.NATIVE_INVALID_HANDLE,
                    "createTask returned zero handle for task " + taskId);
        }
    }

    private void propagateMemoryLimitToNative(TaskInfo info) {
        if (memoryAllocator != null) {
            memoryAllocator.propagateMemoryLimitToNative(info);
        }
    }

    /**
     * Serializes a VectorSearchResult into protobuf wire format for coordinator
     * retrieval. Delegates to {@link VectorSearchTaskSupport}; retained here as the
     * stable static entry point for result-transport consumers.
     */
    private static byte[] serializeVectorSearchResult(
            org.pangrova.spi.index.vector.VectorSearchResult result) {
        return VectorSearchTaskSupport.serializeResult(result);
    }
    /** Returns the vector search result buffer for coordinator retrieval. */
    org.pangrova.worker.execution.VectorSearchResultBuffer getVectorResultBuffer() {
        return vectorSearchSupport.resultBuffer();
    }

    /**
     * Callback interface for lifecycle events that require TaskManager coordination.
     * Decouples lifecycle orchestration from registry/state-machine ownership.
     */
    interface LifecycleCallback {
        /** Drive a state transition on the task registry. */
        boolean transition(String taskId, TaskManager.TaskState target);

        /** Remove coordinator taskId mapping on terminal state. */
        void clearCoordinatorIndex(TaskInfo info);

        /** Notify completion callback with queryId. */
        void notifyCompletion(String queryId);
    }
}
