package org.pangrova.worker.task;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.spi.execution.TaskStatus;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.worker.task.state.TaskStateTransitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Worker-side task lifecycle manager with state machine enforcement.
 * <p>Concurrency strategy: ConcurrentHashMap for task registry,
 * AtomicReference per state, AtomicLong IDs, and volatile callbacks.</p>
 */
@ThreadSafe
public class TaskManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

    static final int MAX_TRACKED_TASKS = 50_000;

    /**
     * Worker task states with defined transition rules.
     */
    public enum TaskState {
        RECEIVED, INITIALIZING, RUNNING, FINISHING, CANCELLING,
        FINISHED, FAILED, CANCELLED
    }

    final ConcurrentHashMap<String, TaskInfo> tasks = new ConcurrentHashMap<>();
    final AtomicLong taskIdCounter = new AtomicLong(0);

    /** Maps coordinator-assigned taskId → worker-generated taskId for submission dedup. */
    final ConcurrentHashMap<String, String> coordinatorTaskIndex = new ConcurrentHashMap<>();

    /**
     * Resolves the worker-internal task ID for a given coordinator-assigned task ID.
     * The worker-internal ID is used as the native task_id in OutputBufferManager
     * and the result server, so result fetch tickets must use this ID.
     *
     * @param coordinatorTaskId the coordinator-assigned task ID
     * @return the worker task ID, or null if not found
     */
    public String resolveWorkerTaskId(String coordinatorTaskId) {
        if (coordinatorTaskId == null) {
            return null;
        }
        return coordinatorTaskIndex.get(coordinatorTaskId);
    }

    final TaskLifecycleManager lifecycleManager;
    final TaskStatusCollector statusCollector;
    final TaskLifecycleManager.LifecycleCallback lifecycleCallback;
    private final org.pangrova.spi.execution.TaskExecutor taskExecutor;
    private final TaskManagerContext deps;

    volatile TenantExecutionQuotaEnforcer quotaEnforcer;

    /**
     * Injects the NativeWriteExecutor for per-task commit metadata isolation.
     * Called during bootstrap after the write executor is constructed.
     */
    public void setWriteExecutor(org.pangrova.worker.jni.write.NativeWriteExecutor executor) {
        if (statusCollector != null) {
            statusCollector.setWriteExecutor(executor);
        }
        if (lifecycleManager != null) {
            lifecycleManager.setWriteExecutor(executor);
        }
    }

    /**
     * Optional retry executor consulted by failure handlers to determine whether
     * a transient task failure should be retried with backoff. Injected during
     * bootstrap; null means retries are disabled.
     */
    private volatile TaskRetryExecutor retryExecutor;

    /**
     * Optional cancellation propagator that fans a cancel signal across every
     * task belonging to the same query. Injected during bootstrap; when set,
     * task submission/completion deltas are mirrored into the query index.
     */
    volatile QueryCancellationPropagator cancellationPropagator;

    private volatile Consumer<String> completionCallback;

    volatile TaskStatusReporter autoFinishReporter;

    volatile org.pangrova.worker.cache.RecentFileAccessTracker recentFileAccessTracker;

    final TaskDrainExecutor drainExecutor;
    private final TaskSubmissionHandler submissionHandler;
    private final ControlEpochFence controlEpochFence = new ControlEpochFence();
    private final ExecutionOwnerFence executionOwnerFence = new ExecutionOwnerFence();

    /**
     * Full wiring constructor — accepts all dependencies via parameter object.
     *
     * @param deps               grouped dependencies (reporter, buffers, executor)
     * @param workerId           identifier of this worker node for status reporting
     * @param minAllocationRatio minimum allocated/requested ratio (0.0–1.0); task rejected if below
     */
    public TaskManager(TaskManagerContext deps, String workerId, double minAllocationRatio) {
        checkNotNull(deps, "deps must not be null");
        checkNotNull(workerId, "workerId must not be null");
        this.deps = deps;
        TaskMemoryRequestor memoryRequestor = new TaskMemoryRequestor(
                deps.resourceManager().getMemoryArbitrator(), minAllocationRatio);
        memoryRequestor.setResourceManager(deps.resourceManager());
        if (deps.nodeMemoryPool() != null) {
            memoryRequestor.setNodeMemoryPool(deps.nodeMemoryPool());
        }
        TaskMemoryAllocator memoryAllocator = new TaskMemoryAllocator(
                deps.resourceManager().getMemoryArbitrator(), deps.nodeMemoryPool());
        this.statusCollector = new TaskStatusCollector(
                deps.reporter(), workerId, memoryAllocator);
        this.lifecycleManager = new TaskLifecycleManager(
                deps, memoryRequestor, memoryAllocator, statusCollector);
        this.lifecycleCallback = buildLifecycleCallback();
        this.taskExecutor = deps.taskExecutor();
        this.drainExecutor = new TaskDrainExecutor(tasks, lifecycleManager,
                taskId -> cancelTask(taskId));
        this.submissionHandler = new TaskSubmissionHandler(this);
    }

    /**
     * Creates a TaskManager with state-machine only — no execution pipeline.
     * Used by WorkerHttpServer wiring where only task submission tracking
     * and HTTP routing are needed, without native engine execution.
     */
    public static TaskManager forHttpWiringOnly() {
        return new TaskManager("http-wiring-worker");
    }

    /**
     * Releases thread pools and pipeline resources.
     * Must be called during worker shutdown to prevent thread/memory leaks
     * in long-running JVM processes (e.g., E2E test suites in single-fork mode).
     */
    @Override
    public void close() {
        drainExecutor.beginDraining();
        TaskManagerShutdown.close(deps, taskExecutor, lifecycleManager, java.util.List.copyOf(tasks.values()));
    }

    /**
     * Internal constructor for state-machine-only mode (no execution pipeline).
     */
    private TaskManager(String workerId) {
        checkNotNull(workerId, "workerId must not be null");
        this.deps = null;
        this.lifecycleManager = null;
        this.statusCollector = null;
        this.taskExecutor = null;
        this.lifecycleCallback = buildLifecycleCallback();
        this.drainExecutor = new TaskDrainExecutor(tasks, null,
                taskId -> cancelTask(taskId));
        this.submissionHandler = new TaskSubmissionHandler(this);
    }

    /**
     * Sets the per-tenant quota enforcer. Called during bootstrap after
     * TenantResourceTracker is initialized.
     */
    public void setQuotaEnforcer(TenantExecutionQuotaEnforcer enforcer) {
        this.quotaEnforcer = enforcer;
        if (lifecycleManager != null) {
            lifecycleManager.setQuotaEnforcer(enforcer);
        }
    }

    /**
     * Injects the {@link TaskRetryExecutor} used to evaluate whether a transient
     * task failure should be retried. Called during bootstrap; idempotent.
     */
    public void setRetryExecutor(TaskRetryExecutor executor) {
        this.retryExecutor = executor;
    }

    public TaskRetryExecutor retryExecutor() {
        return retryExecutor;
    }

    /**
     * Injects the {@link QueryCancellationPropagator} used to fan cancel signals
     * across tasks belonging to the same query.
     */
    public void setCancellationPropagator(QueryCancellationPropagator propagator) {
        this.cancellationPropagator = propagator;
    }

    public QueryCancellationPropagator cancellationPropagator() {
        return cancellationPropagator;
    }

    public String submitTask(TaskDescriptor descriptor, SubmitTaskRequest request) {
        return submissionHandler.submitTask(descriptor, request);
    }

    public String submitTask(TaskDescriptor descriptor) {
        return submissionHandler.submitTask(descriptor);
    }

    /**
     * Enables fail-closed Coordinator epoch validation for all subsequent task
     * admissions. Production Worker bootstrap calls this before exposing the
     * task HTTP endpoint whenever a Coordinator URL is configured.
     */
    public void requireControlEpochHandshake() {
        controlEpochFence.requireHandshake();
    }

    /**
     * Observes an epoch received over the authenticated registration/heartbeat
     * control channel. Epochs advance monotonically and can never be rolled back.
     */
    public void observeControlEpoch(long epoch) {
        controlEpochFence.observe(epoch);
    }

    /** Rejects admission when the request does not carry the active control epoch. */
    public void verifyControlEpochForAdmission(Long epoch) {
        controlEpochFence.verify(epoch);
    }

    /** Rejects task admission when its exact query or job owner does not match. */
    public void verifyExecutionOwnerForAdmission(SubmitTaskRequest request) {
        var attempt = request.taskRequest().getAttempt();
        executionOwnerFence.verify(
                attempt.getQueryOrJobId(),
                org.pangrova.protocol.rest.task.ExecutionOwnerIdentity.from(
                        attempt.getExecutionOwner()));
    }

    /** Applies the same execution-owner fence to metadata publication commands. */
    public void verifyExecutionOwnerForControl(
            String executionId, org.pangrova.protocol.v2.ExecutionOwnerIdentity identity) {
        executionOwnerFence.verify(executionId,
                org.pangrova.protocol.rest.task.ExecutionOwnerIdentity.from(identity));
    }

    public TaskCancelResult cancelTask(String taskId) {
        return submissionHandler.cancelTask(taskId);
    }

    public void releaseTaskExecutor(String taskId) {
        submissionHandler.releaseTaskExecutor(taskId);
    }

    /**
     * Initializes the task manager. Called during Worker Phase 2 startup.
     */
    public void initialize() {
        if (deps != null && deps.transportDeps() != null && !deps.transportDeps().isFullyWired()) {
            LOG.warn("event=transports_not_fully_wired message=TransportRefs are not fully wired; "
                    + "exchange operations may silently no-op in production");
        }
        LOG.info("event=task_manager_initialized");
    }

    /**
     * Sets a callback invoked with queryId when a task completes successfully.
     * Used by WorkerBootstrap to wire result expiration scheduling.
     */
    public void setCompletionCallback(Consumer<String> callback) {
        this.completionCallback = callback;
    }

    /**
     * Enables auto-finish mode for forHttpWiringOnly instances. When set,
     * tasks submitted via {@link #submitTask(TaskDescriptor, SubmitTaskRequest)}
     * are automatically advanced to FINISHED and reported through the given
     * reporter. This closes the Coordinator→Worker→Coordinator lifecycle loop
     * in environments without a native executor.
     */
    public void setAutoFinishReporter(TaskStatusReporter reporter) {
        this.autoFinishReporter = reporter;
    }

    /**
     * F-CHAIN-CACHE-02: Wires the producer side of cache-locality scheduling.
     * Once set, every task submission records its split file paths into the
     * tracker, which the worker heartbeat samples and reports to the
     * coordinator. Set during worker bootstrap; null in tests that do not
     * exercise the heartbeat path.
     */
    public void setRecentFileAccessTracker(org.pangrova.worker.cache.RecentFileAccessTracker tracker) {
        this.recentFileAccessTracker = tracker;
    }

    /**
     * Drains running tasks and waits for completion. Called during Worker shutdown.
     */
    public void drainAndWait() {
        drainExecutor.drainAndWait();
    }

    /**
     * Drains running tasks and waits for completion with a configurable timeout.
     */
    public void drainAndWait(int timeoutSeconds) {
        drainExecutor.drainAndWait(timeoutSeconds);
    }

    /**
     * Returns the current state of a task.
     */
    public Optional<TaskState> getTaskState(String taskId) {
        return Optional.ofNullable(tasks.get(taskId)).map(TaskInfo::getState);
    }

    /**
     * Returns the current SPI-level state of a task.
     * External code should use this instead of accessing TaskStateMapper directly.
     */
    public Optional<TaskStatus.TaskState> getTaskSpiState(String taskId) {
        return getTaskState(taskId).map(TaskStateMapper::toSpiState);
    }

    /**
     * Returns native executor handle for a task when available.
     */
    public OptionalLong getExecutorHandle(String taskId) {
        TaskInfo info = tasks.get(taskId);
        if (info == null || info.executorHandle() == 0L) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(info.executorHandle());
    }

    /**
     * Returns the per-task shuffle server port, or 0 if no shuffle server was started.
     */
    public int getShufflePort(String taskId) {
        TaskInfo info = tasks.get(taskId);
        return info != null ? info.shufflePort() : 0;
    }

    public Optional<org.pangrova.shuffle.ShuffleServerHandle> getShuffleServerHandle(
            String taskId) {
        TaskInfo info = tasks.get(taskId);
        return Optional.ofNullable(info == null ? null : info.shuffleServerHandle());
    }

    /**
     * Returns the shared {@link TaskExecutor} used by this TaskManager.
     * The exchange client must use the SAME executor instance that the task
     * is running on — creating a new executor would route exchange data
     * to a different native context (BUG-W03).
     *
     * @return the shared TaskExecutor, or empty if TaskManager is in
     *         state-machine-only mode (no execution pipeline)
     */
    public Optional<org.pangrova.spi.execution.TaskExecutor> getSharedNativeExecutor() {
        return Optional.ofNullable(taskExecutor);
    }

    /**
     * Returns the count of tasks currently in RUNNING state.
     */
    public int getRunningTaskCount() {
        return (int) tasks.values().stream()
                .filter(info -> info.getState() == TaskState.RUNNING)
                .count();
    }

    /**
     * Cancels all non-terminal tasks and removes terminal ones from tracking.
     */
    public int cancelAllRunningTasks() {
        return drainExecutor.cancelAllRunningTasks();
    }

    /**
     * Releases native executors for tasks that have reached a terminal state.
     */
    public int releaseTerminalTaskExecutors() {
        return drainExecutor.releaseTerminalTaskExecutors();
    }

    /**
     * Removes tasks in terminal state older than the specified retention.
     */
    public int cleanupTerminalTasks(java.time.Duration retention) {
        return TaskCleanupScheduler.cleanupTerminalTasks(
                tasks, retention, info -> {
                    drainExecutor.releaseTerminalTask(info);
                    removeCoordinatorIndex(info);
                }, drainExecutor::resultResourcesDrained);
    }

    /**
     * Starts periodic cleanup of terminal tasks.
     */
    public void startPeriodicCleanup(java.util.concurrent.ScheduledExecutorService executor,
                                     java.time.Duration retention,
                                     java.time.Duration interval) {
        drainExecutor.startPeriodicCleanup(executor, retention, interval,
                this::removeCoordinatorIndex);
    }

    void removeCoordinatorIndex(TaskInfo info) {
        SubmitTaskRequest req = info.request();
        if (req != null) {
            coordinatorTaskIndex.remove(req.taskRequest().getAttempt().getTaskId());
        }
    }

    private void notifyCompletion(String queryId) {
        Consumer<String> cb = completionCallback;
        if (cb != null) {
            cb.accept(queryId);
        }
    }

    private TaskLifecycleManager.LifecycleCallback buildLifecycleCallback() {
        return new TaskLifecycleManager.LifecycleCallback() {
            @Override
            public boolean transition(String taskId, TaskState target) {
                return TaskManager.this.transition(taskId, target);
            }

            @Override
            public void clearCoordinatorIndex(TaskInfo info) {
                removeCoordinatorIndex(info);
            }

            @Override
            public void notifyCompletion(String queryId) {
                TaskManager.this.notifyCompletion(queryId);
            }
        };
    }

    /**
     * Transitions a task to the target state with validation.
     */
    boolean transition(String taskId, TaskState target) {
        TaskInfo info = tasks.get(taskId);
        if (info == null) {
            return false;
        }
        // CAS loop: atomically check current state is allowed and update.
        // Prevents race conditions where concurrent transitions (e.g. cancel + fail)
        // could bypass validation.
        while (true) {
            TaskState current = info.getState();
            if (!TaskStateTransitions.isAllowed(current, target)) {
                LOG.warn("event=invalid_task_transition taskId={} from={} to={}", taskId, current, target);
                return false;
            }
            if (info.compareAndSetState(current, target)) {
                LOG.debug("event=task_transition taskId={} from={} to={}", taskId, current, target);
                if (isTerminalState(target)) {
                    onTerminalTransition(info, taskId);
                }
                return true;
            }
            // CAS failed — another thread changed state; retry with fresh read.
        }
    }

    private static boolean isTerminalState(TaskState state) {
        return state == TaskState.FINISHED || state == TaskState.FAILED
                || state == TaskState.CANCELLED;
    }

    private void onTerminalTransition(TaskInfo info, String taskId) {
        QueryCancellationPropagator prop = cancellationPropagator;
        if (prop != null) {
            prop.deregisterTask(info.queryId(), taskId);
        }
        boolean hasLiveTask = tasks.values().stream()
                .filter(task -> task.queryId().equals(info.queryId()))
                .anyMatch(task -> !isTerminalState(task.getState()));
        if (!hasLiveTask) {
            executionOwnerFence.retire(info.queryId());
        }
        drainExecutor.signalTerminalTransition();
    }
}
