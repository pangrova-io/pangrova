package org.pangrova.worker.task;

import org.pangrova.protocol.enums.ReportedTaskState;
import org.pangrova.worker.enums.TaskAdmissionReason;
import org.pangrova.worker.exception.TaskAdmissionException;
import org.pangrova.worker.task.report.TaskStatusReporter;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.worker.enums.TaskState;
import org.pangrova.worker.task.state.TaskStateTransitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import org.pangrova.common.concurrency.ThreadSafe;

/**
 * Worker-side task lifecycle manager with state machine enforcement.
 * <p>Concurrency strategy: ConcurrentHashMap for task registry,
 * AtomicReference per state, AtomicLong IDs, and volatile callbacks.</p>
 */
@ThreadSafe
public class TaskManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

    static final int MAX_TRACKED_TASKS = 50_000;

    final WorkerTaskRegistry tasks = new WorkerTaskRegistry(MAX_TRACKED_TASKS);

    /** Bounded exact admissions survive task-resource cleanup until their deadlines. */
    final TaskAdmissionRegistry taskAdmissions = new TaskAdmissionRegistry(
            MAX_TRACKED_TASKS, java.time.Clock.systemUTC());
    private final org.pangrova.common.lifecycle.ProcessIncarnationStore admissionIncarnation;

    public org.pangrova.protocol.execution.identity.WorkerExecutionIdentity workerExecutionIdentity() {
        var identity = admissionIncarnation.identity();
        return new org.pangrova.protocol.execution.identity.WorkerExecutionIdentity(
                identity.ownershipDomainId(), identity.incarnationId());
    }

    void verifyWorkerExecutionIdentity(org.pangrova.protocol.v2.TaskAttemptId attempt) {
        if (!attempt.hasWorkerExecutionIdentity()
                || !attempt.getWorkerExecutionIdentity().equals(workerExecutionIdentity().toProto())) {
            throw new TaskAdmissionException(TaskAdmissionReason.IDENTITY_CONFLICT,
                    "Task target does not match this Worker execution incarnation");
        }
    }

    boolean retiredWorkerExecutionIdentity(org.pangrova.protocol.v2.TaskAttemptId attempt) {
        if (!attempt.hasWorkerExecutionIdentity()) return false;
        var identity = org.pangrova.protocol.execution.identity.WorkerExecutionIdentity.from(
                attempt.getWorkerExecutionIdentity());
        return admissionIncarnation.isRetired(new org.pangrova.common.lifecycle.ProcessIncarnationStore.Identity(
                identity.admissionDomainId(), identity.incarnationId()));
    }

    /** Resolves the admitted local execution/result ID, or null if not admitted. */
    public String resolveWorkerTaskId(String coordinatorTaskId) {
        if (coordinatorTaskId == null) {
            return null;
        }
        return taskAdmissions.resolve(coordinatorTaskId);
    }

    final TaskLifecycleManager lifecycleManager;
    final TaskStatusCollector statusCollector;
    final TaskLifecycleManager.LifecycleCallback lifecycleCallback;
    private final org.pangrova.worker.execution.TaskExecutor taskExecutor;
    private final TaskManagerContext deps;

    volatile TenantExecutionQuotaEnforcer quotaEnforcer;

    /**
     * Injects the NativeWriteExecutor for per-task commit metadata isolation.
     * Called during bootstrap after the write executor is constructed.
     */
    public void setWriteExecutor(org.pangrova.worker.write.NativeWriteExecutor executor) {
        if (statusCollector != null) {
            statusCollector.setWriteExecutor(executor);
        }
        if (lifecycleManager != null) {
            lifecycleManager.setWriteExecutor(executor);
        }
    }

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
    private static final int MAX_CONCURRENT_TASK_SETUPS = 64;
    private static final int MAX_QUEUED_TASK_SETUPS = 64;
    private static final long TASK_SETUP_DRAIN_TIMEOUT_MS = 30_000;

    private final java.util.concurrent.ExecutorService taskSetupExecutor =
            org.pangrova.common.concurrency.ManagedExecutorFactory.newBoundedThreadPool(
                    "worker-task-setup", MAX_CONCURRENT_TASK_SETUPS, MAX_QUEUED_TASK_SETUPS);
    private final java.util.concurrent.Semaphore taskSubmissionBytes =
            new java.util.concurrent.Semaphore(SubmitTaskRequest.MAX_HTTP_BODY_BYTES);

    private final TaskSubmissionHandler submissionHandler;
    private final WorkerTaskCancellation cancellation;
    private final TaskAdmissionControl admissionControl = new TaskAdmissionControl(this);
    private volatile SourcePreparationControl sourcePreparationControl;
    private final ControlEpochFence controlEpochFence = new ControlEpochFence();
    private final ExecutionOwnerFence executionOwnerFence = new ExecutionOwnerFence();

    /**
     * Full wiring constructor — accepts all dependencies via parameter object.
     *
     * @param deps               grouped dependencies (reporter, buffers, executor)
     * @param workerId           identifier of this worker node for status reporting
     * @param minAllocationRatio minimum allocated/requested ratio (0.0–1.0); task rejected if below
     */
    public TaskManager(TaskManagerContext deps, String workerId, double minAllocationRatio,
                       org.pangrova.common.lifecycle.ProcessIncarnationStore admissionIncarnation) {
        this.admissionIncarnation = checkNotNull(admissionIncarnation, "admissionIncarnation");
        admissionIncarnation.claimAuthority("worker-task-admission");
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
        this.cancellation = new WorkerTaskCancellation(tasks, lifecycleManager, lifecycleCallback);
    }

    /**
     * Creates a TaskManager with state-machine only — no execution pipeline.
     * Used by WorkerHttpServer wiring where only task submission tracking
     * and HTTP routing are needed, without native engine execution.
     */
    public static TaskManager forHttpWiringOnly() {
        try {
            return new TaskManager(org.pangrova.common.lifecycle.ProcessIncarnationStore.open(
                    java.nio.file.Files.createTempDirectory("pangrova-http-wiring-admission-")));
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    /**
     * Releases thread pools and pipeline resources.
     * Must be called during worker shutdown to prevent thread/memory leaks
     * in long-running JVM processes (e.g., E2E test suites in single-fork mode).
     */
    @Override
    public void close() {
        drainExecutor.beginDraining();
        taskSetupExecutor.shutdown();
        TaskAdmissionRollback.release(
                () -> {
                    var preparation = sourcePreparationControl;
                    if (preparation == null) return;
                    try {
                        preparation.close();
                    } catch (Exception failure) {
                        throw new IllegalStateException("Source preparation shutdown has not joined", failure);
                    }
                },
                () -> {
                    for (TaskInfo info : tasks.values()) info.retryCleanup();
                },
                taskAdmissions::sealAndAwaitAdmissions,
                () -> {
                    org.pangrova.common.concurrency.ExecutorDrain.shutdownAndAwait(
                            taskSetupExecutor, TASK_SETUP_DRAIN_TIMEOUT_MS);
                    TaskManagerShutdown.close(deps, taskExecutor, lifecycleManager, lifecycleCallback,
                            java.util.List.copyOf(tasks.values()));
                });
        if (deps == null) admissionIncarnation.closeAfterDrain(() -> {});
    }

    /**
     * Internal constructor for state-machine-only mode (no execution pipeline).
     */
    TaskManager(org.pangrova.common.lifecycle.ProcessIncarnationStore admissionIncarnation) {
        this.admissionIncarnation = checkNotNull(admissionIncarnation, "admissionIncarnation");
        admissionIncarnation.claimAuthority("worker-task-admission");
        this.deps = null;
        this.lifecycleManager = null;
        this.statusCollector = null;
        this.taskExecutor = null;
        this.lifecycleCallback = buildLifecycleCallback();
        this.drainExecutor = new TaskDrainExecutor(tasks, null,
                taskId -> cancelTask(taskId));
        this.submissionHandler = new TaskSubmissionHandler(this);
        this.cancellation = new WorkerTaskCancellation(tasks, lifecycleManager, lifecycleCallback);
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
     * Injects the {@link QueryCancellationPropagator} used to fan cancel signals
     * across tasks belonging to the same query.
     */
    public void setCancellationPropagator(QueryCancellationPropagator propagator) {
        this.cancellationPropagator = propagator;
    }

    public QueryCancellationPropagator cancellationPropagator() {
        return cancellationPropagator;
    }

    /** Binds one Worker-owned source runtime before its private HTTP listener starts. */
    public synchronized void configureSourcePreparations(org.pangrova.worker.copy.CopyStageLeases leases) {
        if (sourcePreparationControl != null) {
            if (!sourcePreparationControl.uses(leases)) {
                throw new IllegalStateException("Source runtime is already bound");
            }
            return;
        }
        var pool = deps == null ? null : deps.nodeMemoryPool();
        if (deps != null && pool == null) {
            throw new IllegalStateException("Source preparation requires node memory admission");
        }
        sourcePreparationControl = new SourcePreparationControl(this, leases, pool);
    }

    public org.pangrova.protocol.v2.SourcePreparationResult prepareSource(
            org.pangrova.protocol.rest.copy.preparation.PrepareCopySourceRequest request) throws java.io.IOException {
        var preparation = java.util.Objects.requireNonNull(sourcePreparationControl, "Source preparation runtime");
        return preparation.execute(request);
    }

    SourcePreparationControl sourcePreparations() { return sourcePreparationControl; }

    /** Direct canonical submission for in-process Worker execution. */
    public String submitTask(SubmitTaskRequest request) {
        return submissionHandler.submitTask(request);
    }

    /** Observes the actual admission without transferring or cancelling its ownership. */
    public interface TaskAdmission {
        boolean first();

        boolean completed();

        java.util.concurrent.CompletionStage<String> completion();
    }

    /** Acquires a Worker-local body lease that can be handed to the setup executor. */
    public TaskBodyAdmission admitTaskBody() {
        return new TaskBodyAdmission(taskSubmissionBytes);
    }

    /** The same byte allowance covers HTTP decode, queueing and running setup. */
    public static final class TaskBodyAdmission implements AutoCloseable {
        private final java.util.concurrent.Semaphore capacity;
        private int bytes;
        private boolean ingressClosed;

        private TaskBodyAdmission(java.util.concurrent.Semaphore capacity) {
            this.capacity = capacity;
        }

        /** Reserves a declared length or the next observed length before parsing more bytes. */
        public void reserve(int length) {
            if (ingressClosed) throw new IllegalStateException("Task body admission is no longer accepting bytes");
            if (length <= bytes) return;
            if (!capacity.tryAcquire(length - bytes)) {
                throw new org.pangrova.worker.exception.TaskAdmissionException(
                        org.pangrova.worker.enums.TaskAdmissionReason.CAPACITY,
                        "Worker lacks task submission byte capacity");
            }
            bytes = length;
        }

        @Override
        public void close() {
            if (!ingressClosed) {
                ingressClosed = true;
                release();
            }
        }

        private void release() {
            capacity.release(bytes);
            bytes = 0;
        }
    }

    public TaskAdmission submitValidatedTask(
            ValidatedTaskSubmission request, TaskBodyAdmission body) {
        return submissionHandler.submitValidatedTask(request, body);
    }

    void scheduleTaskSetup(TaskBodyAdmission body, Runnable setup) {
        if (body.capacity != taskSubmissionBytes || body.ingressClosed || body.bytes == 0) {
            throw new IllegalStateException("Worker task setup requires its original task body admission");
        }
        // From this point either a scheduled setup or its rejection releases the allowance.
        body.ingressClosed = true;
        boolean scheduled = false;
        try {
            taskSetupExecutor.execute(() -> {
                try {
                    setup.run();
                } finally {
                    body.release();
                }
            });
            scheduled = true;
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            throw new org.pangrova.worker.exception.TaskAdmissionException(
                    org.pangrova.worker.enums.TaskAdmissionReason.CAPACITY,
                    "Worker task setup is closed or at capacity");
        } finally {
            if (!scheduled) body.release();
        }
    }

    String submitTask(TaskDescriptor descriptor) {
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

    void verifyExistingCleanupControlEpoch(long epoch) {
        controlEpochFence.verifyExistingCleanup(epoch);
    }

    /** Reports whether this process can execute bounded CDC ingestion in its JVM. */
    public boolean cdcIngestSupported() {
        return taskExecutor instanceof org.pangrova.worker.execution.CompositeTaskExecutor composite
                && composite.supportsWorkerTask(org.pangrova.protocol.v2.TaskKind.TASK_KIND_CDC_INGEST);
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
        return cancellation.cancelTask(taskId);
    }

    /** Seals an exact canonical admission; empty means real resource exit remains unproven. */
    public Optional<org.pangrova.protocol.v2.TaskReport> sealAdmission(
            org.pangrova.protocol.v2.TaskAdmissionSealRequest request) {
        return admissionControl.seal(request);
    }

    /** Checks the physical admission deadline before ticket creation or any resource acquisition. */
    public void verifyAdmissionDeadline(long deadlineEpochMs) {
        taskAdmissions.requireUnexpired(deadlineEpochMs);
    }

    public void releaseTaskExecutor(String taskId) {
        cancellation.releaseTaskExecutor(taskId);
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

    /** Enables state-machine-only reporting; this mode supplies no Native execution evidence. */
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
     * Permanently rejects new admission/publication after this process loses membership.
     * Does not block the heartbeat callback or claim existing executions have drained.
     * Existing task cleanup, status and output release remain available.
     */
    public void fenceAdmission() {
        taskAdmissions.closeAdmissions();
        drainExecutor.beginDraining();
    }

    /** Drains running tasks and waits for completion during Worker shutdown. */
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
     * Returns the observable state of a registered task.
     * External code should use this instead of accessing TaskStateMapper directly.
     */
    public Optional<ReportedTaskState> getReportedTaskState(String taskId) {
        return getTaskState(taskId).map(TaskStateMapper::toReportedState);
    }

    /** Resolves a Native handle only for the exact admitted running attempt. */
    public OptionalLong getRuntimeFilterHandle(org.pangrova.protocol.v2.TaskAttemptId attempt) {
        String local = taskAdmissions.resolveRuntimeFilterAttempt(attempt);
        TaskInfo info = local == null ? null : tasks.get(local);
        if (info == null || info.getState() != TaskState.RUNNING) {
            return OptionalLong.empty();
        }
        long handle = info.executorHandle();
        return handle == 0L ? OptionalLong.empty() : OptionalLong.of(handle);
    }

    /** Snapshot of completed Native scalar-index decisions in retained task records. */
    public IndexSplitTotals getNativeIndexSplitTotals() {
        long evaluated = 0;
        long skipped = 0;
        for (TaskInfo info : tasks.values()) {
            var stats = info.terminalTaskStats();
            if (stats == null || stats.getSerializedSize() == 0) {
                continue;
            }
            evaluated = Math.addExact(evaluated, stats.getIndexSplitsEvaluated());
            skipped = Math.addExact(skipped, stats.getIndexSplitsSkipped());
        }
        return new IndexSplitTotals(evaluated, skipped);
    }

    public record IndexSplitTotals(long evaluated, long skipped) {}

    /**
     * Returns the per-task shuffle server port, or 0 if no shuffle server was started.
     */
    public int getShufflePort(String taskId) {
        TaskInfo info = tasks.get(taskId);
        return info != null ? info.shufflePort() : 0;
    }

    public Optional<org.pangrova.shuffle.api.ShuffleServerHandle> getShuffleServerHandle(
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
    public Optional<org.pangrova.worker.execution.TaskExecutor> getSharedNativeExecutor() {
        return Optional.ofNullable(taskExecutor);
    }

    /** Counts admitted attempts that still own preparation, execution or cleanup. */
    public int getInFlightTaskCount() {
        return taskAdmissions.inFlightTaskCount();
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
                });
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
        admissionControl.completed(info);
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
            public void watchFinishedResult(TaskInfo info) {
                drainExecutor.watchFinishedResult(info, taskExecutor, TaskManager.this::removeCoordinatorIndex);
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
