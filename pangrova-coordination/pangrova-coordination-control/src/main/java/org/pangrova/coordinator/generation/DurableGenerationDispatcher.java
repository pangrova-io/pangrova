package org.pangrova.coordinator.generation;

import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.placement.WorkerCapabilityMatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import static org.pangrova.common.util.Preconditions.requirePositiveDuration;

/**
 * Query-role control-plane poller for replayable Native artifact work.
 *
 * <p>It moves only immutable plans, bindings, split descriptors, leases and
 * Worker identities. Source rows and artifact bytes flow directly between the
 * Native Worker and object storage.</p>
 */
public final class DurableGenerationDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(
            DurableGenerationDispatcher.class);
    private final GenerationPollLifecycle pollLifecycle = new GenerationPollLifecycle();

    private final GenerationJobStore store;
    private final TaskScheduler scheduler;
    private final WorkerManager workerManager;
    private final TaskDispatcher taskDispatcher;
    private final CoordinatorTaskTracker taskTracker;
    private final String coordinatorId;
    private final LongSupplier epochSupplier;
    private final GenerationExecutionOwnership executionOwnership;
    private final ConcurrentHashMap<org.pangrova.generation.api.identity.GenerationJobKey,
            org.pangrova.execution.api.ownership.ExecutionAnchor> ownedJobs = new ConcurrentHashMap<>();
    private final Duration leaseDuration;
    private final Duration retryBackoff;
    private final int maxInFlight;
    private final Clock clock;
    private final GenerationDispatchRequestFactory requestFactory;
    private final org.pangrova.coordinator.generation.runtime.DurableGenerationGraphs graphs;
    private final org.pangrova.coordinator.query.execution.dispatch.dag.runtime.StagePartitionDispatcher partitions;
    private final ConcurrentHashMap<String, ActiveAttempt> active =
            new ConcurrentHashMap<>();

    /** Runtime collaborators for durable artifact dispatch. */
    public record Dependencies(
            GenerationJobStore store,
            TaskScheduler scheduler,
            WorkerManager workerManager,
            TaskDispatcher taskDispatcher,
            CoordinatorTaskTracker taskTracker,
            String coordinatorId,
            LongSupplier epochSupplier,
            GenerationExecutionOwnership executionOwnership,
            org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore stages,
            org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore exchanges,
            org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine dagEngine) {}

    /** Bounded lease and concurrency settings for durable artifact dispatch. */
    public record Settings(
            Duration leaseDuration,
            Duration retryBackoff,
            int maxInFlight) {}

    public DurableGenerationDispatcher(
            Dependencies dependencies, Settings settings, ExecutionTraceContextFactory traceContexts) {
        this(dependencies, settings, Clock.systemUTC(), traceContexts);
    }

    DurableGenerationDispatcher(
            Dependencies dependencies,
            Settings settings,
            Clock clock, ExecutionTraceContextFactory traceContexts) {
        java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        Objects.requireNonNull(dependencies, "dependencies must not be null");
        Objects.requireNonNull(settings, "settings must not be null");
        this.store = Objects.requireNonNull(
                dependencies.store(), "store must not be null");
        this.scheduler = Objects.requireNonNull(
                dependencies.scheduler(), "scheduler must not be null");
        this.workerManager = Objects.requireNonNull(
                dependencies.workerManager(), "workerManager must not be null");
        this.taskDispatcher = Objects.requireNonNull(
                dependencies.taskDispatcher(), "taskDispatcher must not be null");
        this.taskTracker = Objects.requireNonNull(
                dependencies.taskTracker(), "taskTracker must not be null");
        if (dependencies.coordinatorId() == null
                || dependencies.coordinatorId().isBlank()) {
            throw new IllegalArgumentException("coordinatorId must not be blank");
        }
        this.coordinatorId = dependencies.coordinatorId();
        this.epochSupplier = Objects.requireNonNull(
                dependencies.epochSupplier(), "epochSupplier must not be null");
        this.executionOwnership = Objects.requireNonNull(
                dependencies.executionOwnership(), "executionOwnership");
        if (!coordinatorId.equals(executionOwnership.coordinatorId())) {
            throw new IllegalArgumentException(
                    "artifact dispatcher and execution owner must name the same Coordinator");
        }
        this.leaseDuration = requirePositiveDuration(
                settings.leaseDuration(), "leaseDuration");
        this.retryBackoff = requirePositiveDuration(
                settings.retryBackoff(), "retryBackoff");
        if (settings.maxInFlight() <= 0) {
            throw new IllegalArgumentException("maxInFlight must be positive");
        }
        this.maxInFlight = settings.maxInFlight();
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.requestFactory = new GenerationDispatchRequestFactory(dependencies.stages(), traceContexts);
        var runtime = new org.pangrova.coordinator.query.execution.dispatch.dag.control
                .DurableStageRuntime.Dependencies(
                dependencies.stages(), workerManager, scheduler, epochSupplier, leaseDuration,
                this::registerGraphAttempt, dependencies.exchanges(), traceContexts);
        this.graphs = new org.pangrova.coordinator.generation.runtime.DurableGenerationGraphs(
                new org.pangrova.coordinator.generation.runtime.DurableGenerationGraphs.Dependencies(
                        store, runtime, dependencies.dagEngine(), retryBackoff), clock);
        this.partitions = new org.pangrova.coordinator.query.execution.dispatch.dag.runtime.StagePartitionDispatcher(
                taskDispatcher, new org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller(
                        taskDispatcher, taskTracker), java.util.function.UnaryOperator.identity());
    }

    /** Executes one bounded, non-blocking reconciliation and dispatch pass. */
    public int pollOnce() {
        return pollLifecycle.poll(this::reconcileOnce);
    }

    private int reconcileOnce() {
        Instant now = clock.instant();
        reapAndRenew(now);
        settleOwnedJobs();
        int capacity = maxInFlight - active.size();
        if (capacity <= 0) {
            return 0;
        }
        long epoch = epochSupplier.getAsLong();
        if (epoch <= 0) {
            LOG.debug("event=artifact_dispatch_paused reason=no_active_owner_epoch");
            return 0;
        }
        int dispatched = 0;
        Map<org.pangrova.generation.api.identity.GenerationJobKey, Long> memoryGrants =
                new java.util.HashMap<>();
        boolean madeProgress;
        do {
            madeProgress = false;
            for (var jobKey : store.listRunnableJobs(capacity - dispatched, now)) {
                long requestedMemory = memoryGrants.computeIfAbsent(jobKey, key ->
                        store.findDispatchSpec(key.tenantId(), key.jobId())
                                .orElseThrow().maxMemoryBytes());
                if (workerManager.getActiveWorkers().stream().noneMatch(
                        worker -> hasUnreservedMemory(worker, requestedMemory))) {
                    continue;
                }
                var claimed = claimAuthorizedPage(jobKey, epoch, now);
                if (claimed.isEmpty()) {
                    continue;
                }
                GenerationLease lease = claimed.orElseThrow();
                try {
                    dispatch(lease, now);
                    dispatched++;
                    madeProgress = true;
                    if (dispatched == capacity) {
                        break;
                    }
                } catch (org.pangrova.security.api.exception.AuthorizationDeniedException revoked) {
                    store.cancelJob(lease.tenantId(), lease.jobId(), revoked.getMessage(), now);
                    LOG.warn("event=artifact_dispatch_revoked jobId={} tenantId={} reason={}",
                            lease.jobId(), lease.tenantId(), revoked.getMessage());
                } catch (RuntimeException failure) {
                    retryLease(lease, failure.getMessage(), now);
                    LOG.warn("event=artifact_dispatch_prepare_failed jobId={} pageId={} reason={}",
                            lease.jobId(), lease.pageId(), failure.getMessage());
                }
            }
        } while (madeProgress && dispatched < capacity);
        return dispatched;
    }

    private java.util.Optional<GenerationLease> claimAuthorizedPage(
            org.pangrova.generation.api.identity.GenerationJobKey job, long epoch, Instant now) {
        try {
            var anchor = executionOwnership.acquire(job);
            if (anchor.isEmpty()) {
                return java.util.Optional.empty();
            }
            ownedJobs.put(job, anchor.orElseThrow());
            if (graphs.resume(job, anchor.orElseThrow())) {
                return java.util.Optional.empty();
            }
            return store.claimNextPage(job.tenantId(), job.jobId(), anchor.orElseThrow(), epoch, leaseDuration, now);
        } catch (org.pangrova.security.api.exception.AuthorizationDeniedException revoked) {
            store.cancelJob(job.tenantId(), job.jobId(), revoked.getMessage(), now);
            LOG.warn("event=artifact_claim_revoked jobId={} tenantId={} reason={}",
                    job.jobId(), job.tenantId(), revoked.getMessage());
            return java.util.Optional.empty();
        }
    }

    private void settleOwnedJobs() {
        for (var entry : ownedJobs.entrySet()) {
            settleOwnedJob(entry.getKey(), entry.getValue());
        }
    }

    private void settleOwnedJob(org.pangrova.generation.api.identity.GenerationJobKey key,
            org.pangrova.execution.api.ownership.ExecutionAnchor anchor) {
        boolean activePage = active.values().stream().anyMatch(attempt ->
                attempt.lease.tenantId().equals(key.tenantId()) && attempt.lease.jobId().equals(key.jobId()));
        if (activePage || graphs.isActive(key)) {
            return;
        }
        var job = store.findJob(key.tenantId(), key.jobId()).orElseThrow(() ->
                new IllegalStateException("owned artifact execution lost its exact durable job"));
        switch (job.state()) {
            case READY_TO_PUBLISH -> {
                executionOwnership.yield(anchor);
                ownedJobs.remove(key, anchor);
            }
            case PUBLISHED, FAILED, CANCELLED -> {
                executionOwnership.release(key);
                ownedJobs.remove(key, anchor);
            }
            default -> graphs.resume(key, anchor);
        }
    }

    public int activeAttempts() {
        return active.size();
    }

    private boolean hasUnreservedMemory(WorkerInfo worker, long requestedMemory) {
        long reserved = active.values().stream()
                .filter(attempt -> attempt.worker.workerId().equals(worker.workerId()))
                .mapToLong(attempt -> attempt.memoryGrantBytes)
                .sum();
        return worker.availableMemoryBytes() >= requestedMemory
                && worker.availableMemoryBytes() - requestedMemory >= reserved;
    }

    private void registerGraphAttempt(
            org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime.PreparedTask task,
            java.util.concurrent.CompletableFuture<DispatchResult> acknowledgement) {
        String taskId = task.request().taskRequest().getAttempt().getTaskId();
        var attempt = new ActiveAttempt(task.lease(), task.worker(), task.lease().expiresAt(),
                task.request().taskRequest().getResourceGrant().getNativeMemoryBytes());
        if (active.putIfAbsent(taskId, attempt) != null) {
            throw new IllegalStateException("Duplicate exact durable graph task identity");
        }
        acknowledgement.whenComplete((result, failure) -> onDispatchSettled(taskId, result, failure));
    }

    private void dispatch(GenerationLease lease, Instant now) {
        GenerationDispatchRequestFactory.PreparedRequest prepared =
                requestFactory.prepare(lease, now);
        List<WorkerInfo> candidates = scheduler.eligibleWorkersForTenant(
                prepared.maxMemoryBytes(), workerManager.getActiveWorkers(), lease.tenantId()).stream()
                .filter(candidate -> hasUnreservedMemory(candidate, prepared.maxMemoryBytes()))
                .toList();
        WorkerInfo worker = scheduler.selectWorkerWithBindings(
                prepared.maxMemoryBytes(), candidates,
                prepared.bindings(), WorkerInfo::capabilityInventory)
                .orElseThrow(() -> workerSelectionFailure(
                        candidates, prepared.bindings(), prepared.maxMemoryBytes()));
        String taskId = prepared.taskId();
        ActiveAttempt attempt = new ActiveAttempt(
                lease, worker, lease.expiresAt(), prepared.maxMemoryBytes());
        try {
            if (active.putIfAbsent(taskId, attempt) != null) {
                throw new IllegalStateException("duplicate active artifact task identity");
            }
            partitions.dispatch(prepared.request(), worker).acknowledgement().whenComplete(
                    (result, failure) -> onDispatchSettled(taskId, result, failure));
        } catch (RuntimeException failure) {
            if (taskTracker.getTaskRequest(taskId) != null) {
                attempt.retryReason = rootMessage(failure);
                attempt.cancelling = true;
                cancelActiveTask(taskId, attempt);
                return;
            }
            // The sole partition boundary has not registered/sent any request.
            // Only this locally prepared candidate may be discarded here.
            active.remove(taskId, attempt);
            throw failure;
        }
    }

    static IllegalStateException workerSelectionFailure(
            List<WorkerInfo> candidates,
            List<org.pangrova.protocol.v2.OperatorBinding> bindings,
            long requestedMemoryBytes) {
        List<WorkerInfo> capable = candidates.stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(worker.capabilityInventory(), bindings))
                .toList();
        if (!capable.isEmpty() && capable.stream().allMatch(
                worker -> worker.availableMemoryBytes() < requestedMemoryBytes)) {
            long maximumAvailable = capable.stream()
                    .mapToLong(WorkerInfo::availableMemoryBytes)
                    .max().orElse(0);
            return new IllegalStateException(
                    "RESOURCE_EXHAUSTED: Native artifact memory grant "
                            + requestedMemoryBytes
                            + " bytes exceeds every capable Worker's available memory "
                            + "(maximum " + maximumAvailable + " bytes)");
        }
        return new IllegalStateException(
                "no active Worker satisfies exact capability, model, format, "
                        + "health and resource placement");
    }

    private void onDispatchSettled(
            String taskId, DispatchResult result, Throwable failure) {
        ActiveAttempt attempt = active.get(taskId);
        if (attempt == null) {
            return;
        }
        if (failure == null && result != null && taskId.equals(result.taskId())) {
            attempt.acknowledged = true;
            LOG.info("event=artifact_page_dispatched jobId={} pageId={} taskId={} workerId={}",
                    attempt.lease.jobId(), attempt.lease.pageId(), taskId,
                    attempt.worker.workerId());
            return;
        }
        String reason = failure != null ? rootMessage(failure)
                : "Worker acknowledged a different task identity";
        attempt.retryReason = reason;
        attempt.cancelling = true;
        cancelActiveTask(taskId, attempt);
    }

    private void reapAndRenew(Instant now) {
        for (Map.Entry<String, ActiveAttempt> entry : active.entrySet()) {
            reconcileAttempt(entry.getKey(), entry.getValue(), now);
        }
    }

    private void reconcileAttempt(
            String taskId, ActiveAttempt attempt, Instant now) {
        var receipt = taskTracker.executionDrainReceipt(taskId);
        if (receipt.isPresent()) {
            retireDrainedAttempt(taskId, attempt, receipt.orElseThrow(), now);
            return;
        }
        if (attempt.cancelling) {
            cancelActiveTask(taskId, attempt);
            return;
        }
        TaskStateMachine.State state = findTaskState(taskId, attempt);
        if (state == TaskStateMachine.State.FINISHED || state == TaskStateMachine.State.FAILED
                || state == TaskStateMachine.State.ABANDONED) return;
        renewAttemptIfDue(taskId, attempt, now);
    }

    private TaskStateMachine.State findTaskState(
            String taskId, ActiveAttempt attempt) {
        try {
            return taskTracker.getTaskState(attempt.lease.executionAnchor().executionId(), taskId);
        } catch (IllegalArgumentException gone) {
            throw new IllegalStateException(
                    "Exact artifact attempt disappeared without a drain receipt: " + taskId, gone);
        }
    }

    private void retireDrainedAttempt(String taskId, ActiveAttempt attempt,
            org.pangrova.protocol.v2.TaskReport receipt, Instant now) {
        if (receipt.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
            // The physical drain can precede the terminal HTTP publication. Keep
            // the issued identity until the same FINISHED fact is durably accepted.
            if (!attempt.acknowledged || !taskTracker.finishedPublicationComplete(receipt)) return;
        }
        if (attempt.retryReason != null
                || receipt.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
            retryLease(attempt.lease, attempt.retryReason == null
                    ? "Native background task terminated: " + receipt.getState() : attempt.retryReason, now);
        }
        taskTracker.removeDeadline(taskId);
        taskTracker.unregisterDrainedTask(receipt);
        active.remove(taskId, attempt);
    }

    private void renewAttemptIfDue(
            String taskId, ActiveAttempt attempt, Instant now) {
        Instant renewAt = attempt.expiresAt.minus(leaseDuration.dividedBy(3));
        if (now.isBefore(renewAt)) {
            return;
        }
        boolean renewed;
        try {
            renewed = store.renewPage(attempt.lease, leaseDuration, now);
        } catch (RuntimeException unavailableOrRevoked) {
            if (unavailableOrRevoked instanceof org.pangrova.security.api.exception.AuthorizationDeniedException) {
                cancelRevokedJob(attempt.lease, unavailableOrRevoked.getMessage(), now);
            }
            LOG.warn("event=artifact_renewal_denied taskId={} reason={}", taskId, unavailableOrRevoked.getMessage());
            renewed = false;
        }
        if (renewed) {
            attempt.expiresAt = now.plus(leaseDuration);
        } else {
            attempt.cancelling = true;
            cancelActiveTask(taskId, attempt);
        }
    }

    private void cancelRevokedJob(GenerationLease lease, String reason, Instant now) {
        try {
            store.cancelJob(lease.tenantId(), lease.jobId(), reason, now);
        } catch (RuntimeException unavailable) {
            LOG.error("event=artifact_revocation_record_failed jobId={}", lease.jobId(), unavailable);
        }
    }

    private void cancelActiveTask(String taskId, ActiveAttempt attempt) {
        try {
            var issued = taskTracker.getTaskRequest(taskId);
            if (issued == null) throw new IllegalStateException("Cannot seal an unissued artifact attempt: " + taskId);
            var cancelled = taskDispatcher.cancelTask(issued, attempt.worker.host(), attempt.worker.httpPort());
            if (cancelled.isSuccess()) taskTracker.recordExecutionDrain(issued, cancelled.executionDrainReceipt());
            if (cancelled.status() == org.pangrova.coordinator.scheduler.SchedulerCancelResult.Status.FAILED) {
                LOG.warn("event=artifact_native_cancel_rejected taskId={} deadline={} reason={}",
                        taskId, attempt.lease.expiresAt(), cancelled.errorMessage());
            }
        } catch (RuntimeException unavailable) {
            LOG.warn("event=artifact_native_cancel_failed taskId={} deadline={}",
                    taskId, attempt.lease.expiresAt(), unavailable);
        }
    }

    private void retryLease(GenerationLease lease, String reason, Instant now) {
        GenerationLeaseRetry.retry(store, lease, now.plus(retryBackoff), reason, now);
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String value = current.getMessage();
        return value == null || value.isBlank()
                ? current.getClass().getSimpleName() : value;
    }

    @Override
    public void close() {
        pollLifecycle.close(this::cleanupOwnedWork);
    }

    private void cleanupOwnedWork() {
        Throwable failure = null;
        for (var key : ownedJobs.keySet()) {
            try {
                graphs.cancel(key);
            } catch (RuntimeException | Error cleanup) {
                failure = appendFailure(failure, cleanup);
            }
        }
        for (Map.Entry<String, ActiveAttempt> entry : active.entrySet()) {
            ActiveAttempt attempt = entry.getValue();
            try {
                attempt.cancelling = true;
                cancelActiveTask(entry.getKey(), attempt);
                var receipt = taskTracker.executionDrainReceipt(entry.getKey());
                if (receipt.isPresent()) {
                    retireDrainedAttempt(entry.getKey(), attempt, receipt.orElseThrow(), clock.instant());
                }
            } catch (RuntimeException | Error cleanup) {
                failure = appendFailure(failure, cleanup);
            }
        }
        if (!active.isEmpty()) {
            failure = appendFailure(failure, new IllegalStateException(
                    "Artifact shutdown retains " + active.size()
                            + " exact attempts awaiting execution drain receipts"));
        }
        rethrowCleanupFailure(failure);
        for (var entry : ownedJobs.entrySet()) {
            try {
                executionOwnership.yield(entry.getValue());
                ownedJobs.remove(entry.getKey(), entry.getValue());
            } catch (RuntimeException | Error cleanup) {
                failure = appendFailure(failure, cleanup);
            }
        }
        rethrowCleanupFailure(failure);
    }

    private static Throwable appendFailure(Throwable existing, Throwable added) {
        if (existing == null) return added;
        existing.addSuppressed(added);
        return existing;
    }

    private static void rethrowCleanupFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }

    private static final class ActiveAttempt {
        private final GenerationLease lease;
        private final WorkerInfo worker;
        private final long memoryGrantBytes;
        private volatile Instant expiresAt;
        private volatile boolean acknowledged;
        private volatile boolean cancelling;
        private volatile String retryReason;

        private ActiveAttempt(
                GenerationLease lease, WorkerInfo worker, Instant expiresAt,
                long memoryGrantBytes) {
            this.lease = lease;
            this.worker = worker;
            this.expiresAt = expiresAt;
            this.memoryGrantBytes = memoryGrantBytes;
        }
    }
}
