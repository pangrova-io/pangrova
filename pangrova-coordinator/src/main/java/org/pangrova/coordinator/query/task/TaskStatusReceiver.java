package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.report.ArtifactWorkReportSink;
import org.pangrova.coordinator.query.task.report.CanonicalTaskReportValidator;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.speculation.DuplicateTaskResolver;
import org.pangrova.coordinator.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Receives task status reports from Workers and drives Coordinator-side state transitions.
 *
 * <p>Guards:</p>
 * <ul>
 *   <li>Rejects reports from unregistered Workers with {@link UnregisteredWorkerException}.</li>
 *   <li>Rejects stale reports whose {@code reportSequence} is not greater than the
 *       last accepted sequence for the same task (sequence-based deduplication).</li>
 * </ul>
 *
 * <p>When speculative execution is active, integrates with {@link DuplicateTaskResolver}
 * for winner/loser resolution and {@link SpeculativeTaskLauncher} for pair tracking.</p>
 */
public class TaskStatusReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(TaskStatusReceiver.class);

    private final QueryManager queryManager;
    private final CoordinatorTaskTracker taskTracker;
    private final WorkerManager workerManager;
    private final CanonicalTaskReportValidator taskReportValidator =
            new CanonicalTaskReportValidator();

    // Optional speculation dependencies (null when speculation is disabled)
    private final SpeculativeTaskLauncher speculativeLauncher;
    private final DuplicateTaskResolver duplicateResolver;
    private final TaskDispatcher taskDispatcher;
    private final QueryStateJournal stateJournal;

    // Optional native write commit dependencies (null when native write commit is disabled)
    private final CatalogRegistry catalogRegistry;
    private final NativeWriteCommitTracker writeCommitTracker;

    // Optional DAG task→stage bridge (null when DAG dispatch is not active)
    private final StageCompletionCallback stageCompletionCallback;

    // Optional lineage emitter (null when governance is disabled)
    private final org.pangrova.governance.lineage.LineageEventEmitter lineageEmitter;

    // Write commit journal for idempotent distributed write commits
    private final WriteCommitJournal writeCommitJournal;
    private final ArtifactWorkReportSink artifactWorkReportSink;

    /** Optional durable execution fence for the catalog-commit path. */
    private volatile WriteCommitFencer writeCommitFencer;

    /** Serializes report side effects and advances sequence only after durable prerequisites. */
    private final ConcurrentHashMap<String, ReportSequenceGate> reportSequenceGates =
            new ConcurrentHashMap<>();

    /** Single-finalizer guard: prevents concurrent finalization of the same query. */
    private final ConcurrentHashMap<String, Boolean> finalizationGuard = new ConcurrentHashMap<>();

    /** Physical failures are held until both attempts of a logical partition fail. */
    private final ConcurrentHashMap<String, Set<String>> speculativeFailures =
            new ConcurrentHashMap<>();

    public TaskStatusReceiver(TaskStatusReceiverContext deps) {
        this.queryManager = deps.queryManager();
        this.taskTracker = deps.taskTracker();
        this.workerManager = deps.workerManager();
        this.speculativeLauncher = deps.speculativeLauncher();
        this.duplicateResolver = deps.duplicateResolver();
        this.taskDispatcher = deps.taskDispatcher();
        this.stateJournal = deps.stateJournal() != null
                ? deps.stateJournal() : InMemoryQueryStateJournal.INSTANCE;
        this.catalogRegistry = deps.catalogRegistry();
        this.writeCommitTracker = deps.writeCommitTracker();
        this.stageCompletionCallback = deps.stageCompletionCallback();
        this.lineageEmitter = deps.lineageEmitter();
        this.writeCommitJournal = deps.writeCommitJournal() != null
                ? deps.writeCommitJournal() : InMemoryWriteCommitJournal.INSTANCE;
        this.artifactWorkReportSink = deps.artifactWorkReportSink();
    }

    /**
     * Installs the durable query-owner fence used to reject catalog commits
     * from a stale Coordinator process and stamp replayable write intents.
     * Optional — when unset (single-node / no HA), commits are never fenced.
     */
    public void setWriteCommitFencer(WriteCommitFencer fencer) {
        this.writeCommitFencer = fencer;
    }

    /**
     * Process a task status report from a Worker.
     * Rejects reports from unregistered Workers and stale (out-of-order) reports;
     * otherwise updates task tracker and triggers query-level transitions.
     *
     * @throws UnregisteredWorkerException if the reporting Worker is not registered
     * @return true if the report was accepted, false if rejected as stale
     */
    public boolean onTaskStatus(TaskStatusReport report) {
        checkNotNull(report, "report must not be null");
        rejectIfUnregistered(report.workerId());
        var expectedRequest = taskTracker.getTaskRequest(report.taskId());
        StatusValidation validation = validateBoundReport(report, expectedRequest);
        if (!validation.accepted()) {
            return false;
        }
        boolean durableWork = isBackgroundTask(expectedRequest);

        ReportOwnership ownership = reportOwnership(report, durableWork);
        if (!ownership.accepted()) return false;

        ReportSequencePermit sequence = reserveSequence(
                report.taskId(), report.reportSequence());
        if (sequence == null) {
            LOG.warn("event=status_report_rejected_stale taskId={} reportSeq={}",
                    report.taskId(), report.reportSequence());
            return false;
        }
        try (sequence) {
            boolean accepted = processReservedReport(report, expectedRequest, validation,
                    ownership.owner(), durableWork);
            if (accepted) {
                sequence.accept();
            }
            return accepted;
        }
    }

    private StatusValidation validateBoundReport(TaskStatusReport report, SubmitTaskRequest expected) {
        if (("BACKGROUND_BUILD".equals(report.taskType()) || "BACKGROUND_EXPORT".equals(report.taskType()))
                && expected == null) {
            LOG.warn("event=status_report_rejected_unbound_background_task taskId={}", report.taskId());
            return StatusValidation.rejected();
        }
        return validateRegisteredTask(report);
    }

    private ReportOwnership reportOwnership(TaskStatusReport report, boolean durable) {
        if (writeCommitFencer == null || durable) return new ReportOwnership(true, null);
        try {
            ExecutionAnchor anchor = writeCommitFencer.currentAnchor(report.queryId());
            return new ReportOwnership(anchor != null, anchor == null ? null : anchor.ownerId());
        } catch (IllegalStateException staleOwner) {
            LOG.info("event=status_report_rejected_stale_owner queryId={} taskId={}",
                    report.queryId(), report.taskId());
            return new ReportOwnership(false, null);
        }
    }

    private record ReportOwnership(boolean accepted, String owner) {}

    private boolean processReservedReport(TaskStatusReport report, SubmitTaskRequest expected,
            StatusValidation validation, String owner, boolean durable) {
        if (taskAttemptIsNoLongerActive(report)) {
            clearTaskSequence(report.taskId());
            LOG.warn("event=status_report_rejected_settled taskId={} reportSeq={}",
                    report.taskId(), report.reportSequence());
            return false;
        }
        if (durable && isFinishedState(report.state())
                && !commitDurableReport(expected, report, validation.canonicalStatus().orElseThrow())) {
            LOG.warn("event=artifact_report_rejected_durable taskId={} attempt={}", report.taskId(), report.attempt());
            return false;
        }
        return processAcceptedStatus(report, owner, durable);
    }

    private boolean commitDurableReport(SubmitTaskRequest request, TaskStatusReport report,
            org.pangrova.protocol.v2.TaskReport status) {
        try {
            return artifactWorkReportSink.commit(request.taskRequest().getSecurity().getTenantId(),
                    status, report.canonicalTaskReport());
        } catch (org.pangrova.security.authz.AuthorizationDeniedException
                | org.pangrova.metadata.artifact.jdbc.ArtifactExecutionFence.LostOwnerException fenced) {
            LOG.warn("event=durable_report_rejected_authority queryId={} taskId={} reason={}",
                    report.queryId(), report.taskId(), fenced.getMessage());
            return false;
        }
    }

    private boolean processAcceptedStatus(
            TaskStatusReport report, String owningCoordinator, boolean durableWork) {

        String queryId = report.queryId();
        String taskId = report.taskId();
        if (shouldHandleSpeculativeFailure(report, durableWork)) {
            return handleSpeculativeFailure(
                    queryId, taskId, report, owningCoordinator);
        }
        if (!updateTaskState(queryId, taskId, report)) {
            return false;
        }

        // Durable producer and export reports are committed before the shared
        // DAG sees their terminal state. They never enter user-query, DML,
        // speculation, audit, or result-row transitions.
        if (durableWork) {
            processDurableTerminal(report);
            return true;
        }

        // A speculative FINISHED report has not won its logical partition yet.
        // Route only the elected completion to the DAG's original task slot.
        notifyAcceptedTerminal(report);

        // Journal: sync for terminal states, async for non-terminal
        journalTaskEvent(report, owningCoordinator);

        if (isFinishedState(report.state())) {
            return handleTaskCompletion(queryId, taskId, report);
        }

        evaluateQueryTransitions(queryId, report);
        return true;
    }

    private boolean shouldHandleSpeculativeFailure(TaskStatusReport report, boolean durable) {
        return !durable && isSpeculationActive() && isUnsuccessfulState(report.state())
                && findCounterpartTaskId(report.taskId()) != null;
    }

    private void processDurableTerminal(TaskStatusReport report) {
        if (isTerminalTaskState(report.state())) {
            notifyStageTerminal(report.queryId(), report.taskId(), report);
            taskTracker.removeDeadline(report.taskId());
            taskTracker.signalTaskTerminal(report.queryId(), report.taskId());
            clearTaskSequence(report.taskId());
        }
    }

    private void notifyAcceptedTerminal(TaskStatusReport report) {
        if (isTerminalTaskState(report.state())
                && (!isFinishedState(report.state()) || !isSpeculationActive())) {
            notifyStageTerminal(report.queryId(), report.taskId(), report);
        }
    }

    private StatusValidation validateRegisteredTask(TaskStatusReport report) {
        var expectedAttempt = taskTracker.getTaskAttempt(report.taskId());
        if (expectedAttempt == null) {
            LOG.warn("event=status_report_rejected_unknown_task taskId={} attempt={}",
                    report.taskId(), report.attempt());
            return StatusValidation.rejected();
        }
        if (!expectedAttempt.equals(report.attempt())) {
            LOG.warn("event=status_report_rejected_fence taskId={} expected={} actual={}",
                    report.taskId(), expectedAttempt, report.attempt());
            return StatusValidation.rejected();
        }
        String expectedWorker = taskTracker.getWorkerForTask(report.taskId());
        if (expectedWorker != null && !expectedWorker.equals(report.workerId())) {
            LOG.warn("event=status_report_rejected_worker taskId={} expected={} actual={}",
                    report.taskId(), expectedWorker, report.workerId());
            return StatusValidation.rejected();
        }
        try {
            return StatusValidation.accepted(taskReportValidator.validate(
                    report, taskTracker.getTaskRequest(report.taskId())));
        } catch (IllegalArgumentException e) {
            LOG.warn("event=status_report_rejected_canonical taskId={} reason={}",
                    report.taskId(), e.getMessage());
            return StatusValidation.rejected();
        }
    }

    private boolean taskAttemptIsNoLongerActive(TaskStatusReport report) {
        var currentAttempt = taskTracker.getTaskAttempt(report.taskId());
        if (currentAttempt == null || !currentAttempt.equals(report.attempt())) {
            return true;
        }
        try {
            var state = taskTracker.getTaskState(report.queryId(), report.taskId());
            return settledOrUnsubmittedTerminal(state, report.state());
        } catch (IllegalArgumentException notRegisteredYet) {
            // Identity preparation alone is not proof that a task was submitted.
            // Nonterminal reports retain the existing registration/retry behavior.
            return isTerminalTaskState(report.state());
        }
    }

    private static boolean settledOrUnsubmittedTerminal(
            org.pangrova.coordinator.task.TaskStateMachine.State current, String reported) {
        return current == org.pangrova.coordinator.task.TaskStateMachine.State.FINISHED
                || current == org.pangrova.coordinator.task.TaskStateMachine.State.FAILED
                || current == org.pangrova.coordinator.task.TaskStateMachine.State.ABANDONED
                || current == org.pangrova.coordinator.task.TaskStateMachine.State.PENDING
                        && isTerminalTaskState(reported);
    }

    private record StatusValidation(
            boolean accepted,
            Optional<org.pangrova.protocol.v2.TaskReport> canonicalStatus) {

        private static StatusValidation accepted(
                Optional<org.pangrova.protocol.v2.TaskReport> status) {
            return new StatusValidation(true, status);
        }

        private static StatusValidation rejected() {
            return new StatusValidation(false, Optional.empty());
        }
    }

    private ReportSequencePermit reserveSequence(String taskId, long sequence) {
        while (true) {
            ReportSequenceGate gate = reportSequenceGates.computeIfAbsent(
                    taskId, ReportSequenceGate::new);
            SequenceReservation reservation = gate.tryReserve(sequence);
            if (reservation.retired()) {
                reportSequenceGates.remove(taskId, gate);
                continue;
            }
            if (reservation.stale()) {
                return null;
            }
            if (reservation.permit() != null) {
                return reservation.permit();
            }
            reservation.waitFor().join();
        }
    }

    /** Clears sequence tracking for a completed/cancelled task to prevent memory leak. */
    public void clearTaskSequence(String taskId) {
        ReportSequenceGate gate = reportSequenceGates.get(taskId);
        if (gate != null && gate.retire()) {
            reportSequenceGates.remove(taskId, gate);
        }
    }

    private final class ReportSequencePermit implements AutoCloseable {
        private final String taskId;
        private final ReportSequenceGate gate;
        private final long sequence;
        private boolean closed;
        private boolean accepted;

        private ReportSequencePermit(
                String taskId, ReportSequenceGate gate, long sequence) {
            this.taskId = taskId;
            this.gate = gate;
            this.sequence = sequence;
        }

        private void accept() {
            gate.accept(sequence);
            accepted = true;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (gate.release(sequence, accepted)) {
                reportSequenceGates.remove(taskId, gate);
            }
        }
    }

    private final class ReportSequenceGate {
        private final String taskId;
        private long acceptedSequence = -1L;
        private long highestSeenSequence = -1L;
        private long activeSequence = -1L;
        private CompletableFuture<Void> activeCompletion;
        private boolean retired;

        private ReportSequenceGate(String taskId) {
            this.taskId = taskId;
        }

        private synchronized SequenceReservation tryReserve(long sequence) {
            if (retired) {
                return SequenceReservation.retiredReservation();
            }
            if (activeCompletion != null) {
                return SequenceReservation.waiting(activeCompletion);
            }
            if (sequence <= acceptedSequence || sequence < highestSeenSequence) {
                return SequenceReservation.staleReservation();
            }
            highestSeenSequence = sequence;
            activeSequence = sequence;
            activeCompletion = new CompletableFuture<>();
            return SequenceReservation.acquired(
                    new ReportSequencePermit(taskId, this, sequence));
        }

        private synchronized void accept(long sequence) {
            requireActive(sequence);
            acceptedSequence = sequence;
        }

        private boolean release(long sequence, boolean accepted) {
            CompletableFuture<Void> completion;
            boolean remove;
            synchronized (this) {
                requireActive(sequence);
                // A failed durable commit may retry the same sequence, but an
                // older report must not overtake the attempt that reached us.
                completion = activeCompletion;
                activeCompletion = null;
                activeSequence = -1L;
                remove = retired;
            }
            completion.complete(null);
            return remove;
        }

        private synchronized boolean retire() {
            retired = true;
            return activeCompletion == null;
        }

        private void requireActive(long sequence) {
            if (activeCompletion == null || activeSequence != sequence) {
                throw new IllegalStateException("report sequence permit is not active");
            }
        }
    }

    private record SequenceReservation(
            ReportSequencePermit permit,
            CompletableFuture<Void> waitFor,
            boolean stale,
            boolean retired) {

        private static SequenceReservation acquired(ReportSequencePermit permit) {
            return new SequenceReservation(permit, null, false, false);
        }

        private static SequenceReservation waiting(CompletableFuture<Void> waitFor) {
            return new SequenceReservation(null, waitFor, false, false);
        }

        private static SequenceReservation staleReservation() {
            return new SequenceReservation(null, null, true, false);
        }

        private static SequenceReservation retiredReservation() {
            return new SequenceReservation(null, null, false, true);
        }
    }

    private void rejectIfUnregistered(String workerId) {
        if (workerManager.getWorker(workerId) == null) {
            LOG.warn("event=status_report_rejected_unregistered workerId={}",
                    workerId);
            throw new UnregisteredWorkerException(workerId);
        }
    }

    private boolean updateTaskState(String queryId, String taskId, TaskStatusReport report) {
        try {
            return taskTracker.updateTaskState(queryId, taskId, report.state());
        } catch (IllegalArgumentException e) {
            // The elected attempt can retire this loser after validation but
            // before its in-flight status reaches the state machine.
            if (taskTracker.getTaskAttempt(taskId) == null) {
                return false;
            }
            throw e;
        }
    }

    private void notifyStageTerminal(
            String queryId, String logicalTaskId, TaskStatusReport report) {
        if (stageCompletionCallback == null) {
            return;
        }
        try {
            stageCompletionCallback.onTaskTerminal(queryId, logicalTaskId,
                    report.state(), report.error());
        } catch (Exception e) {
            LOG.error("event=stage_completion_callback_failed queryId={} taskId={}"
                    + " state={} error={}", queryId, logicalTaskId, report.state(),
                    e.getMessage(), e);
        }
    }

    private boolean handleSpeculativeFailure(
            String queryId, String taskId, TaskStatusReport report,
            String owningCoordinator) {
        if (!report.attempt().equals(taskTracker.getTaskAttempt(taskId))) {
            return false;
        }
        String counterpart = findCounterpartTaskId(taskId);
        if (counterpart == null) {
            return false;
        }
        // Keep the logical task pending while its other physical attempt can
        // still succeed. In particular, do not invoke the global failure callback.
        journalTaskEvent(report, owningCoordinator);
        Set<String> failed = speculativeFailures.computeIfAbsent(
                queryId, ignored -> ConcurrentHashMap.newKeySet());
        failed.add(taskId);
        taskTracker.clearTaskTracking(taskId);
        clearTaskSequence(taskId);
        if (!failed.contains(counterpart)) {
            return true;
        }

        String logicalTaskId = resolveOriginalTaskId(taskId);
        String copyTaskId = taskId.equals(logicalTaskId) ? counterpart : taskId;
        notifyStageTerminal(queryId, logicalTaskId, report);
        taskTracker.updateTaskState(queryId, logicalTaskId, "FAILED");
        taskTracker.retireSpeculativeLoser(queryId, copyTaskId);
        clearSpeculativeFailures(queryId, logicalTaskId, copyTaskId);
        speculativeLauncher.removePair(logicalTaskId);
        evaluateQueryTransitions(queryId, report);
        return true;
    }

    private void clearSpeculativeFailures(
            String queryId, String originalTaskId, String copyTaskId) {
        Set<String> failed = speculativeFailures.get(queryId);
        if (failed == null) {
            return;
        }
        failed.remove(originalTaskId);
        failed.remove(copyTaskId);
    }

    private boolean handleTaskCompletion(String queryId, String taskId, TaskStatusReport report) {
        if (!isSpeculationActive()) {
            collectWriteCommitMetadata(queryId, report);
            evaluateQueryTransitions(queryId, report);
            return true;
        }
        return resolveSpeculativeCompletion(queryId, taskId, report);
    }

    private boolean resolveSpeculativeCompletion(String queryId, String taskId,
                                                  TaskStatusReport report) {
        String stageId = String.valueOf(report.stageId());
        boolean isWinner = duplicateResolver.onTaskComplete(
                queryId, taskId, stageId, report.partitionId());

        if (isWinner) {
            if (!report.attempt().equals(taskTracker.getTaskAttempt(taskId))) {
                // The other attempt may have fenced this one after the first
                // status check but before it entered the winner election.
                return false;
            }
            // A speculative loser may finish concurrently with its winner.
            // Only the elected attempt contributes objects to the catalog commit.
            collectWriteCommitMetadata(queryId, report);
            String logicalTaskId = resolveOriginalTaskId(taskId);
            String counterpart = findCounterpartTaskId(taskId);
            cancelLoserTask(queryId, taskId, report);
            notifyStageTerminal(queryId, logicalTaskId, report);
            if (counterpart != null) {
                clearSpeculativeFailures(queryId, logicalTaskId,
                        taskId.equals(logicalTaskId) ? counterpart : taskId);
            }
            speculativeLauncher.removePair(logicalTaskId);
            LOG.info("event=speculation_winner queryId={} taskId={} stageId={} partitionId={}",
                    queryId, taskId, report.stageId(), report.partitionId());
        } else {
            cleanupLoserTask(queryId, taskId);
            LOG.info("event=speculation_loser queryId={} taskId={} stageId={} partitionId={}",
                    queryId, taskId, report.stageId(), report.partitionId());
            return true;
        }

        evaluateQueryTransitions(queryId, report);
        return true;
    }

    private void cancelLoserTask(String queryId, String taskId, TaskStatusReport report) {
        String loserId = findCounterpartTaskId(taskId);
        if (loserId == null) {
            return;
        }
        cancelAndCleanup(queryId, loserId);
    }

    private void cleanupLoserTask(String queryId, String taskId) {
        taskTracker.retireSpeculativeLoser(queryId, taskId);
        clearTaskSequence(taskId);
    }

    private void cancelAndCleanup(String queryId, String taskId) {
        sendCancelToWorker(taskId);
        taskTracker.retireSpeculativeLoser(queryId, taskId);
        clearTaskSequence(taskId);
    }

    private void sendCancelToWorker(String taskId) {
        if (taskDispatcher == null) {
            return;
        }
        WorkerInfo worker = resolveWorker(taskId);
        if (worker == null) {
            return;
        }
        try {
            taskDispatcher.cancelTask(taskId, worker.host(), worker.httpPort());
        } catch (Exception e) {
            LOG.debug("event=loser_cancel_dispatch_failed taskId={} error={}",
                    taskId, e.getMessage());
        }
    }

    private WorkerInfo resolveWorker(String taskId) {
        String workerId = taskTracker.getWorkerForTask(taskId);
        return workerId != null ? workerManager.getWorker(workerId) : null;
    }

    /**
     * Finds the counterpart (original or speculative) task ID for a given task.
     */
    private String findCounterpartTaskId(String taskId) {
        if (speculativeLauncher.isSpeculative(taskId)) {
            return speculativeLauncher.getOriginalTaskId(taskId).orElse(null);
        }
        return speculativeLauncher.getSpeculativeTaskId(taskId).orElse(null);
    }

    private String resolveOriginalTaskId(String taskId) {
        if (speculativeLauncher.isSpeculative(taskId)) {
            return speculativeLauncher.getOriginalTaskId(taskId).orElse(taskId);
        }
        return taskId;
    }

    private boolean isSpeculationActive() {
        return speculativeLauncher != null && duplicateResolver != null;
    }

    private static boolean isFinishedState(String state) {
        return "FINISHED".equals(state);
    }

    private static boolean isUnsuccessfulState(String state) {
        return "FAILED".equals(state) || "CANCELLED".equals(state);
    }

    private static boolean isTerminalTaskState(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state);
    }

    private void journalTaskEvent(TaskStatusReport report, String owningCoordinator) {
        TaskStateEvent event = new TaskStateEvent(
                report.queryId(), report.taskId(), report.workerId(),
                report.state(), report.stageId(), report.reportSequence(),
                System.currentTimeMillis(), owningCoordinator);
        if (isTerminalTaskState(report.state())) {
            stateJournal.appendTaskEventSync(event);
        } else {
            stateJournal.appendTaskEvent(event);
        }
    }

    private boolean isBackgroundTask(SubmitTaskRequest expectedRequest) {
        if (expectedRequest == null) {
            return false;
        }
        TaskKind kind = expectedRequest.taskRequest().getTaskKind();
        return kind == TaskKind.TASK_KIND_BACKGROUND_BUILD
                || kind == TaskKind.TASK_KIND_BACKGROUND_EXPORT
                || kind == TaskKind.TASK_KIND_SHUFFLE_WRITE && artifactWorkReportSink.ownsExecution(
                        expectedRequest.taskRequest().getSecurity().getTenantId(),
                        expectedRequest.taskRequest().getAttempt().getQueryOrJobId());
    }

    private void evaluateQueryTransitions(String queryId, TaskStatusReport report) {
        boolean allFinished = taskTracker.allTasksFinished(queryId);
        boolean hasFailed = taskTracker.hasFailedTask(queryId);

        if (allFinished) {
            // When DAG dispatch is active, taskTracker may report allTasksFinished
            // before downstream stage tasks are registered (async dispatch race).
            // Consult the DAG engine to confirm all stages have truly completed.
            if (stageCompletionCallback != null
                    && !stageCompletionCallback.isQueryComplete(queryId)) {
                LOG.debug("event=query_finalization_deferred queryId={}"
                        + " reason=dag_stages_pending", queryId);
                // Schedule a deferred re-check. The DAG engine may complete
                // the stage during collectResults (early terminal replay),
                // after which isQueryComplete returns true. Without this
                // retry, the query hangs because no one re-triggers
                // evaluateQueryTransitions after the DAG completes.
                scheduleQueryCompletionRecheck(queryId);
                return;
            }
            finalizeCompletedQuery(queryId);
        } else if (hasFailed) {
            cleanupWriteStateOnFailure(queryId);
            clearQuerySequences(queryId);
            queryManager.onTaskFailed(queryId, report.error());
        }
    }

    /**
     * Schedules a deferred re-check of query completion after DAG stages finish.
     * Polls isQueryComplete at short intervals until true, then finalizes.
     */
    private void scheduleQueryCompletionRecheck(String queryId) {
        Thread t = new Thread(() -> pollUntilQueryComplete(queryId),
                "query-recheck-" + queryId);
        t.setDaemon(true);
        t.start();
    }

    /** Polls query completion at 50ms intervals, finalizing when all tasks finish. */
    private void pollUntilQueryComplete(String queryId) {
        for (int i = 0; i < 600; i++) {
            if (!sleepQuietly()) {
                return;
            }
            if (stageCompletionCallback == null
                    || stageCompletionCallback.isQueryComplete(queryId)) {
                if (taskTracker.allTasksFinished(queryId)) {
                    LOG.info("event=query_finalization_recheck_success queryId={} attempts={}",
                            queryId, i + 1);
                    finalizeCompletedQuery(queryId);
                }
                return;
            }
        }
        LOG.warn("event=query_finalization_recheck_timeout queryId={}", queryId);
    }

    /** Sleeps 50ms; returns false if interrupted. */
    private static boolean sleepQuietly() {
        try {
            Thread.sleep(50);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void finalizeCompletedQuery(String queryId) {
        // Single-finalizer guard: only one thread may finalize a given query.
        // Prevents concurrent finalization race where a non-winner thread drives
        // the query to FINISHED while the commit-winner is still in flight.
        if (finalizationGuard.putIfAbsent(queryId, Boolean.TRUE) != null) {
            LOG.debug("event=finalization_skipped_concurrent queryId={}", queryId);
            return;
        }
        try {
            try {
                if (!commitDistributedWriteIfNeeded(queryId)) {
                    LOG.warn("event=query_finalization_deferred queryId={} "
                            + "reason=execution_owner_fenced", queryId);
                    return;
                }
            } catch (Exception e) {
                LOG.error("event=distributed_write_commit_failed queryId={} error={}",
                        queryId, e.getMessage(), e);
                cleanupWriteStateAfterCommitFailure(queryId);
                clearQuerySequences(queryId);
                queryManager.onTaskFailed(queryId,
                        new org.pangrova.protocol.rest.error.ErrorDetail(
                                "WRITE_COMMIT_FAILED", e.getClass().getSimpleName(),
                                "Distributed write commit failed: " + e.getMessage()));
                return;
            }
            emitTaskCompletedLineage(queryId);
            clearQuerySequences(queryId);
            queryManager.onAllTasksFinished(queryId);
            // Complete waiters only after any deferred distributed write commit
            // is visible to the next sequential DML phase.
            for (String taskId : taskTracker.getTaskIds(queryId)) {
                taskTracker.signalTaskTerminal(queryId, taskId);
            }
        } finally {
            finalizationGuard.remove(queryId);
        }
    }

    /**
     * Removes all sequence tracking entries for tasks belonging to the given query.
     * Prevents unbounded growth of the report-sequence gate map.
     */
    private void clearQuerySequences(String queryId) {
        for (String taskId : taskTracker.getTaskIds(queryId)) {
            clearTaskSequence(taskId);
        }
        if (duplicateResolver != null) {
            duplicateResolver.clearQuery(queryId);
        }
        speculativeFailures.remove(queryId);
    }

    /**
     * Collects commit metadata from a completed write task's status report.
     * When a worker completes a native write task, the report contains a
     * WriteResultDescriptor with serialized commit metadata. This method
     * extracts and stores the metadata for later aggregation.
     */
    private void collectWriteCommitMetadata(String queryId, TaskStatusReport report) {
        if (writeCommitTracker == null) {
            return;
        }
        WriteResultDescriptor writeResult = report.writeResult();
        byte[] commitMetadata = writeResult != null ? writeResult.commitMetadata() : null;
        if (commitMetadata == null || commitMetadata.length == 0) {
            LOG.debug("event=write_task_no_commit_metadata queryId={} taskId={}",
                    queryId, report.taskId());
            return;
        }
        if (!writeCommitTracker.hasWriteContext(queryId)) {
            LOG.debug("event=write_commit_metadata_skipped queryId={} taskId={} reason=no_write_context",
                    queryId, report.taskId());
            return;
        }
        writeCommitTracker.addCommitFragment(queryId, commitMetadata);
        LOG.info("event=write_commit_metadata_collected queryId={} taskId={} metadataBytes={}",
                queryId, report.taskId(), commitMetadata.length);
    }

    /**
     * Executes the distributed write commit when all tasks for a write query have finished.
     * Retrieves the accumulated commit metadata fragments and calls
     * ConnectorMetadata.finishDistributedWrite() for a single atomic catalog commit.
     *
     * <p>Integrates with {@link WriteCommitJournal} for idempotent recovery:
     * already-committed writes (detected via journal) are skipped.</p>
     */
    private boolean commitDistributedWriteIfNeeded(String queryId) {
        if (writeCommitTracker == null || catalogRegistry == null) {
            return true;
        }

        WriteCommitFencer fencer = this.writeCommitFencer;
        ExecutionAnchor anchor = fencer != null
                ? fencer.currentAnchor(queryId) : null;
        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_journal_read")) {
            return false;
        }

        // Idempotent recovery: skip if the journal says this write is already committed
        if (writeCommitJournal.isCommitted(queryId)) {
            LOG.info("event=distributed_write_already_committed queryId={}", queryId);
            acknowledgeOrdinaryWrite(queryId);
            writeCommitTracker.removeWriteState(queryId);
            writeCommitJournal.purge(queryId);
            return true;
        }

        // WAL ordering (F-WRITE-001 / A-COMMIT-01): peek — do NOT remove the tracker
        // state yet. The tracker entry holds the only copy of the write context
        // (session / output handle / fragments); it must outlive the durable intent
        // record and the catalog commit so that any failure before COMMITTED leaves a
        // recoverable state instead of destroying the write context prematurely.
        NativeWriteCommitTracker.WriteCommitState writeState =
                writeCommitTracker.peekWriteState(queryId);
        if (writeState == null) {
            return true;
        }

        // 1. Record a durable, REPLAYABLE PENDING intent BEFORE touching the catalog.
        //    A-COMMIT-01: the intent now carries the worker commit fragments and an
        //    idempotency key (queryId) so a replacement coordinator can reconstruct and
        //    finish — or safely skip — the commit after a crash, rather than only observing
        //    a state marker. If this throws (journal store unavailable), the tracker state
        //    is still intact for a subsequent retry.
        List<byte[]> fragments = writeState.fragments();
        WriteCommitEntry intent = createWriteCommitIntent(queryId, writeState, fragments, anchor);
        writeCommitJournal.recordIntent(intent);

        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_catalog_commit")) {
            return false;
        }

        // 2. Perform the single atomic catalog commit.
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(writeState.catalogName());
        if (!(catalogHandle instanceof ConnectorCatalogHandle connectorCatalog)) {
            throw new IllegalStateException(
                    "Distributed write catalog is no longer a connector catalog");
        }
        var boundHandle = DefaultWriteCommitReconciler.decodeExactHandle(
                intent, writeState.outputHandle().targetTable().orElseThrow(),
                connectorCatalog);
        ConnectorMetadata metadata = catalogHandle.getMetadata(writeState.session());
        metadata.finishDistributedWrite(
                writeState.session(), boundHandle, fragments, intent.commitContext());

        // The external catalog commit is required to be idempotent. If ownership changed while it
        // was in flight, leave the WAL entry PENDING: the new owner will reconcile the same
        // idempotency key and determine/confirm the catalog outcome.
        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "after_catalog_commit")) {
            return false;
        }

        // 3. Durably mark COMMITTED. Only after this record is it safe to drop the
        //    in-memory tracker state; a crash before this point leaves a PENDING
        //    journal entry plus the tracker state for the recovery path to reconcile.
        writeCommitJournal.markCommitted(queryId);
        acknowledgeOrdinaryWrite(queryId);

        // 4. Commit is durable — release the in-memory tracker state and purge.
        writeCommitTracker.removeWriteState(queryId);
        writeCommitJournal.purge(queryId);

        LOG.info("event=distributed_write_committed queryId={} catalog={} fragmentCount={}",
                queryId, writeState.catalogName(), fragments.size());
        return true;
    }

    private void acknowledgeOrdinaryWrite(String queryId) {
        var entry = writeCommitJournal.getEntry(queryId).orElseThrow(() ->
                new IllegalStateException("COMMITTED write lost its declaration receipt"));
        if (!entry.authorizationReceipt().declaration().hasGeneratedColumns()) {
            writeCommitJournal.acknowledgeNoDeferredWork(entry.commitFingerprint());
        }
    }

    private static WriteCommitEntry createWriteCommitIntent(
            String queryId,
            NativeWriteCommitTracker.WriteCommitState writeState,
            List<byte[]> fragments,
            ExecutionAnchor anchor) {
        var outputHandle = writeState.outputHandle();
        String tableName = outputHandle.targetTable()
                .orElseThrow(() -> new IllegalStateException(
                        "Distributed write handle lacks a recoverable target table"))
                .toString();
        String tenantId = writeState.session().getProperties().get("tenant_id");
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalStateException(
                    "Distributed write commit requires tenant_id in the connector session");
        }
        long baseSnapshotId = outputHandle.writeBaseSnapshotId().orElse(-1L);
        byte[] writeOutputBinding = writeState.writeOutputBinding();
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        queryId,
                        tenantId,
                        writeState.catalogName(),
                        tableName,
                        outputHandle.writeMode(),
                        baseSnapshotId,
                        writeOutputBinding,
                        writeState.session().getUser(),
                        writeState.authorizationReceipt()),
                fragments);
        return new WriteCommitEntry(
                queryId,
                writeState.catalogName(),
                tableName,
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                outputHandle.writeMode(),
                fingerprint,
                tenantId,
                writeState.session().getUser(),
                writeState.authorizationReceipt(),
                baseSnapshotId,
                writeOutputBinding,
                fingerprint,
                anchor != null ? anchor.ownerId() : null,
                anchor != null ? anchor.ownerIncarnation() : null,
                anchor != null ? anchor.fencingToken() : 0L,
                null);
    }

    private static boolean isCurrentExecutionOwner(
            String queryId,
            WriteCommitFencer fencer,
            ExecutionAnchor anchor,
            String phase) {
        if (fencer == null) {
            return true;
        }
        if (anchor != null && fencer.isCurrent(queryId, anchor.fencingToken())) {
            return true;
        }
        LOG.error("event=distributed_write_fenced queryId={} owner={} incarnation={} "
                        + "fencingToken={} phase={} reason=stale_execution_owner",
                queryId,
                anchor != null ? anchor.ownerId() : "missing",
                anchor != null ? anchor.ownerIncarnation() : "missing",
                anchor != null ? anchor.fencingToken() : 0L,
                phase);
        return false;
    }

    /**
     * An ambiguous catalog-commit failure must retain its durable PENDING intent for recovery.
     * Only the JVM-only handle is released; purging the redo record here would turn a crash or
     * timeout after catalog publication into silent data loss or duplicate publication.
     */
    private void cleanupWriteStateAfterCommitFailure(String queryId) {
        if (writeCommitTracker != null) {
            writeCommitTracker.clearWriteState(queryId);
        }
    }

    /**
     * Cleans up write state when a query fails, preventing memory leaks.
     * Retains pending redo evidence because task failure cannot prove catalog abort.
     */
    private void cleanupWriteStateOnFailure(String queryId) {
        if (writeCommitTracker != null) {
            writeCommitTracker.clearWriteState(queryId);
        }
        writeCommitJournal.purge(queryId);
    }

    /**
     * Emits a lineage taskCompleted event when all tasks for a query finish successfully.
     * Fault-isolated: lineage failures never propagate to query completion.
     */
    private void emitTaskCompletedLineage(String queryId) {
        if (lineageEmitter == null) {
            return;
        }
        try {
            lineageEmitter.emitCompleteEvent(queryId);
        } catch (Exception e) {
            LOG.debug("event=lineage_task_completed_failed queryId={} error={}",
                    queryId, e.getMessage());
        }
    }
}
