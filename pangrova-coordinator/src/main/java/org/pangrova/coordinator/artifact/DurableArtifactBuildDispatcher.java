package org.pangrova.coordinator.artifact;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerCapabilityMatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactBuildStore;
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

/**
 * Query-role control-plane poller for replayable Native artifact work.
 *
 * <p>It moves only immutable plans, bindings, split descriptors, leases and
 * Worker identities. Source rows and artifact bytes flow directly between the
 * Native Worker and object storage.</p>
 */
public final class DurableArtifactBuildDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(
            DurableArtifactBuildDispatcher.class);
    private final ArtifactPollLifecycle pollLifecycle = new ArtifactPollLifecycle();

    private final ArtifactBuildStore store;
    private final TaskScheduler scheduler;
    private final WorkerManager workerManager;
    private final TaskDispatcher taskDispatcher;
    private final CoordinatorTaskTracker taskTracker;
    private final String coordinatorId;
    private final LongSupplier epochSupplier;
    private final ArtifactExecutionOwnership executionOwnership;
    private final ConcurrentHashMap<org.pangrova.metadata.artifact.build.ArtifactBuildJobKey,
            org.pangrova.spi.coordination.ExecutionAnchor> ownedJobs = new ConcurrentHashMap<>();
    private final Duration leaseDuration;
    private final Duration retryBackoff;
    private final int maxInFlight;
    private final Clock clock;
    private final ArtifactDispatchRequestFactory requestFactory;
    private final org.pangrova.coordinator.artifact.runtime.DurableArtifactGraphs graphs;
    private final org.pangrova.coordinator.query.execution.dispatch.dag.runtime.StagePartitionDispatcher partitions;
    private final ConcurrentHashMap<String, ActiveAttempt> active =
            new ConcurrentHashMap<>();

    /** Runtime collaborators for durable artifact dispatch. */
    public record Dependencies(
            ArtifactBuildStore store,
            TaskScheduler scheduler,
            WorkerManager workerManager,
            TaskDispatcher taskDispatcher,
            CoordinatorTaskTracker taskTracker,
            String coordinatorId,
            LongSupplier epochSupplier,
            ArtifactExecutionOwnership executionOwnership,
            org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactStageExecutionStore stages,
            org.pangrova.metadata.artifact.jdbc.JdbcArtifactStageExchangeStore exchanges,
            org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine dagEngine) {}

    /** Bounded lease and concurrency settings for durable artifact dispatch. */
    public record Settings(
            Duration leaseDuration,
            Duration retryBackoff,
            int maxInFlight) {}

    public DurableArtifactBuildDispatcher(
            Dependencies dependencies, Settings settings) {
        this(dependencies, settings, Clock.systemUTC());
    }

    DurableArtifactBuildDispatcher(
            Dependencies dependencies,
            Settings settings,
            Clock clock) {
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
        this.leaseDuration = requirePositive(
                settings.leaseDuration(), "leaseDuration");
        this.retryBackoff = requirePositive(
                settings.retryBackoff(), "retryBackoff");
        if (settings.maxInFlight() <= 0) {
            throw new IllegalArgumentException("maxInFlight must be positive");
        }
        this.maxInFlight = settings.maxInFlight();
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.requestFactory = new ArtifactDispatchRequestFactory(dependencies.stages());
        var runtime = new org.pangrova.coordinator.query.execution.dispatch.dag.control
                .DurableStageRuntime.Dependencies(
                dependencies.stages(), workerManager, scheduler, epochSupplier, leaseDuration,
                this::registerGraphAttempt, dependencies.exchanges());
        this.graphs = new org.pangrova.coordinator.artifact.runtime.DurableArtifactGraphs(
                new org.pangrova.coordinator.artifact.runtime.DurableArtifactGraphs.Dependencies(
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
        for (var jobKey : store.listRunnableJobs(capacity, now)) {
            if (dispatched >= capacity) {
                break;
            }
            var claimed = claimAuthorizedPage(jobKey, epoch, now);
            if (claimed.isEmpty()) {
                continue;
            }
            ArtifactBuildLease lease = claimed.orElseThrow();
            try {
                dispatch(lease, now);
                dispatched++;
            } catch (org.pangrova.security.authz.AuthorizationDeniedException revoked) {
                store.cancelJob(lease.tenantId(), lease.jobId(), revoked.getMessage(), now);
                LOG.warn("event=artifact_dispatch_revoked jobId={} tenantId={} reason={}",
                        lease.jobId(), lease.tenantId(), revoked.getMessage());
            } catch (RuntimeException failure) {
                retryLease(lease, failure.getMessage(), now);
                LOG.warn("event=artifact_dispatch_prepare_failed jobId={} pageId={} reason={}",
                        lease.jobId(), lease.pageId(), failure.getMessage());
            }
        }
        return dispatched;
    }

    private java.util.Optional<ArtifactBuildLease> claimAuthorizedPage(
            org.pangrova.metadata.artifact.build.ArtifactBuildJobKey job, long epoch, Instant now) {
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
        } catch (org.pangrova.security.authz.AuthorizationDeniedException revoked) {
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

    private void settleOwnedJob(org.pangrova.metadata.artifact.build.ArtifactBuildJobKey key,
            org.pangrova.spi.coordination.ExecutionAnchor anchor) {
        boolean activePage = active.values().stream().anyMatch(attempt ->
                attempt.lease.tenantId().equals(key.tenantId()) && attempt.lease.jobId().equals(key.jobId()));
        if (activePage || graphs.isActive(key)) {
            return;
        }
        var job = store.findJob(key.tenantId(), key.jobId()).orElseThrow(() ->
                new IllegalStateException("owned artifact execution lost its exact durable job"));
        switch (job.state()) {
            case READY_TO_PUBLISH -> {
                if (ownedJobs.remove(key, anchor)) executionOwnership.yield(anchor);
            }
            case PUBLISHED, FAILED, CANCELLED -> {
                if (ownedJobs.remove(key, anchor)) executionOwnership.release(key);
            }
            default -> graphs.resume(key, anchor);
        }
    }

    public int activeAttempts() {
        return active.size();
    }

    private void registerGraphAttempt(
            org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime.PreparedTask task,
            java.util.concurrent.CompletableFuture<DispatchResult> acknowledgement) {
        String taskId = task.request().taskRequest().getAttempt().getTaskId();
        var attempt = new ActiveAttempt(task.lease(), task.worker(), task.lease().expiresAt());
        if (active.putIfAbsent(taskId, attempt) != null) {
            throw new IllegalStateException("Duplicate exact durable graph task identity");
        }
        acknowledgement.whenComplete((result, failure) -> onDispatchSettled(taskId, result, failure));
    }

    private void dispatch(ArtifactBuildLease lease, Instant now) {
        ArtifactDispatchRequestFactory.PreparedRequest prepared =
                requestFactory.prepare(lease, now);
        List<WorkerInfo> candidates = workerManager.getActiveWorkers();
        WorkerInfo worker = scheduler.selectWorkerWithBindings(
                prepared.maxMemoryBytes(), candidates,
                prepared.bindings())
                .orElseThrow(() -> workerSelectionFailure(
                        candidates, prepared.bindings(), prepared.maxMemoryBytes()));
        String taskId = prepared.taskId();
        ActiveAttempt attempt = new ActiveAttempt(lease, worker, lease.expiresAt());
        try {
            if (active.putIfAbsent(taskId, attempt) != null) {
                throw new IllegalStateException("duplicate active artifact task identity");
            }
            partitions.dispatch(prepared.request(), worker).acknowledgement().whenComplete(
                    (result, failure) -> onDispatchSettled(taskId, result, failure));
        } catch (RuntimeException failure) {
            active.remove(taskId, attempt);
            failPreparedTask(lease.executionAnchor().executionId(), taskId);
            throw failure;
        }
    }

    static IllegalStateException workerSelectionFailure(
            List<WorkerInfo> candidates,
            List<org.pangrova.protocol.v2.OperatorBinding> bindings,
            long requestedMemoryBytes) {
        List<WorkerInfo> capable = candidates.stream()
                .filter(worker -> WorkerCapabilityMatcher.supports(worker, bindings))
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
        if (active.remove(taskId, attempt)) {
            cancelActiveTask(taskId, attempt);
            retryLease(attempt.lease, reason, clock.instant());
        }
    }

    private void reapAndRenew(Instant now) {
        for (Map.Entry<String, ActiveAttempt> entry : active.entrySet()) {
            reconcileAttempt(entry.getKey(), entry.getValue(), now);
        }
    }

    private void reconcileAttempt(
            String taskId, ActiveAttempt attempt, Instant now) {
        TaskStateMachine.State state = findTaskState(taskId, attempt);
        if (state == null || removeTerminalAttempt(taskId, attempt, state, now)) {
            return;
        }
        renewAttemptIfDue(taskId, attempt, now);
    }

    private TaskStateMachine.State findTaskState(
            String taskId, ActiveAttempt attempt) {
        try {
            return taskTracker.getTaskState(attempt.lease.executionAnchor().executionId(), taskId);
        } catch (IllegalArgumentException gone) {
            active.remove(taskId, attempt);
            return null;
        }
    }

    private boolean removeTerminalAttempt(
            String taskId, ActiveAttempt attempt,
            TaskStateMachine.State state, Instant now) {
        if (state == TaskStateMachine.State.FINISHED) {
            // The shared DAG still needs the issued attempt until its late HTTP acknowledgement is processed.
            if (attempt.acknowledged && active.remove(taskId, attempt)) {
                cleanupTask(attempt.lease.executionAnchor().executionId(), taskId);
            }
            return true;
        }
        if (state != TaskStateMachine.State.FAILED
                && state != TaskStateMachine.State.ABANDONED) {
            return false;
        }
        if (active.remove(taskId, attempt)) {
            retryLease(attempt.lease, "Native background task terminated: " + state, now);
            cleanupTask(attempt.lease.executionAnchor().executionId(), taskId);
        }
        return true;
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
            if (unavailableOrRevoked instanceof org.pangrova.security.authz.AuthorizationDeniedException) {
                cancelRevokedJob(attempt.lease, unavailableOrRevoked.getMessage(), now);
            }
            LOG.warn("event=artifact_renewal_denied taskId={} reason={}", taskId, unavailableOrRevoked.getMessage());
            renewed = false;
        }
        if (renewed) {
            attempt.expiresAt = now.plus(leaseDuration);
        } else if (active.remove(taskId, attempt)) {
            cancelActiveTask(taskId, attempt);
        }
    }

    private void cancelRevokedJob(ArtifactBuildLease lease, String reason, Instant now) {
        try {
            store.cancelJob(lease.tenantId(), lease.jobId(), reason, now);
        } catch (RuntimeException unavailable) {
            LOG.error("event=artifact_revocation_record_failed jobId={}", lease.jobId(), unavailable);
        }
    }

    private void cancelActiveTask(String taskId, ActiveAttempt attempt) {
        try {
            var cancelled = taskDispatcher.cancelTask(taskId, attempt.worker.host(), attempt.worker.httpPort());
            if (cancelled.status() == org.pangrova.coordinator.scheduler.SchedulerCancelResult.Status.FAILED) {
                LOG.warn("event=artifact_native_cancel_rejected taskId={} deadline={} reason={}",
                        taskId, attempt.lease.expiresAt(), cancelled.errorMessage());
            }
        } catch (RuntimeException unavailable) {
            LOG.warn("event=artifact_native_cancel_failed taskId={} deadline={}",
                    taskId, attempt.lease.expiresAt(), unavailable);
        } finally {
            markTaskFailed(attempt.lease.executionAnchor().executionId(), taskId);
            cleanupTask(attempt.lease.executionAnchor().executionId(), taskId);
        }
    }

    private void retryLease(ArtifactBuildLease lease, String reason, Instant now) {
        try {
            store.retryPage(lease, now.plus(retryBackoff), reason, now);
        } catch (org.pangrova.metadata.artifact.jdbc.ArtifactExecutionFence.LostOwnerException staleOwner) {
            LOG.warn("event=artifact_retry_rejected_owner_lost jobId={} pageId={}", lease.jobId(), lease.pageId());
        }
    }

    private void markTaskFailed(String jobId, String taskId) {
        try {
            taskTracker.updateTaskState(jobId, taskId, "FAILED");
            taskTracker.signalTaskTerminal(jobId, taskId);
        } catch (RuntimeException ignored) {
            LOG.debug("event=artifact_task_failure_tracking_skipped taskId={}", taskId);
        }
    }

    private void failPreparedTask(String jobId, String taskId) {
        markTaskFailed(jobId, taskId);
        cleanupTask(jobId, taskId);
    }

    private void cleanupTask(String jobId, String taskId) {
        taskTracker.removeDeadline(taskId);
        taskTracker.unregisterSettledTask(jobId, taskId);
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
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
        ownedJobs.keySet().forEach(graphs::cancel);
        for (Map.Entry<String, ActiveAttempt> entry : active.entrySet()) {
            ActiveAttempt attempt = entry.getValue();
            if (active.remove(entry.getKey(), attempt)) {
                cancelActiveTask(entry.getKey(), attempt);
            }
        }
        ownedJobs.forEach((job, anchor) -> executionOwnership.yield(anchor));
        ownedJobs.clear();
    }

    private static final class ActiveAttempt {
        private final ArtifactBuildLease lease;
        private final WorkerInfo worker;
        private volatile Instant expiresAt;
        private volatile boolean acknowledged;

        private ActiveAttempt(
                ArtifactBuildLease lease, WorkerInfo worker, Instant expiresAt) {
            this.lease = lease;
            this.worker = worker;
            this.expiresAt = expiresAt;
        }
    }
}
