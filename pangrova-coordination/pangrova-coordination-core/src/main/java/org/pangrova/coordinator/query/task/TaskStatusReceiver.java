package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.write.WriteCommitFencer;

import java.time.Instant;
import java.util.Optional;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.query.task.report.GenerationWorkReportSink;
import org.pangrova.coordinator.query.task.report.CanonicalTaskReportValidator;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.spi.query.coordination.ExecutionAnchor;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.query.state.TaskStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/** Validates exact issued reports before required journal, publication, sequencing and drain notifications. */
@ThreadSafe
public final class TaskStatusReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStatusReceiver.class);
    private final CoordinatorTaskTracker taskTracker;
    private final WorkerManager workerManager;
    private final CanonicalTaskReportValidator taskReportValidator = new CanonicalTaskReportValidator();
    private final QueryStateJournal stateJournal;
    private final StageCompletionCallback stageCompletionCallback;
    private final GenerationWorkReportSink artifactWorkReportSink;
    private final TaskReportSequences sequences = new TaskReportSequences();
    private final NativeTaskWriteCompletion writes;
    private final SpeculativeTaskReportCompletion speculation;
    private final TaskQueryCompletion queryCompletion;

    /** Creates a receiver for task reports and completion notifications. */
    public TaskStatusReceiver(TaskStatusReceiverContext deps) {
        taskTracker = deps.taskTracker();
        workerManager = deps.workerManager();
        stateJournal = deps.queryManager().getStateJournal();
        if (deps.stateJournal() != null && deps.stateJournal() != stateJournal) {
            throw new IllegalArgumentException("Task reports must use their QueryManager's exact journal owner");
        }
        stageCompletionCallback = deps.stageCompletionCallback();
        artifactWorkReportSink = deps.artifactWorkReportSink();
        writes = new NativeTaskWriteCompletion(deps);
        speculation = new SpeculativeTaskReportCompletion(deps, writes, sequences, this::evaluateQueryTransitions);
        taskTracker.addQueryRetirementListener(speculation::clearQuery);
        queryCompletion = new TaskQueryCompletion(deps, writes, sequences);
    }

    private void evaluateQueryTransitions(String queryId, TaskStatusReport report) {
        queryCompletion.evaluateQueryTransitions(queryId, report);
    }

    /**
     * Installs the durable query-owner fence used to reject catalog commits
     * from a stale Coordinator process and stamp replayable write intents.
     * Optional — when unset (single-node / no HA), commits are never fenced.
     */
    public void setWriteCommitFencer(WriteCommitFencer fencer) {
        writes.setFencer(fencer);
    }

    /**
     * Installs the governance quality gate evaluated before a distributed write
     * commit. Optional — when unset, writes are committed without a pre-commit
     * quality check.
     */
    public void setWriteQualityGate(
            org.pangrova.governance.api.quality.WriteQualityGate gate) {
        writes.setWriteQualityGate(gate);
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
        // A lost HTTP ACK must not make the Worker discard/recreate its actual
        // exit owner. This branch only acknowledges retained drain metadata;
        // it cannot publish outputs, select a winner, or re-run DAG callbacks.
        if (taskTracker.acknowledgesExecutionDrain(report)
                || taskTracker.acknowledgesReportPublication(report)) return true;
        rejectIfUnregistered(report.workerId());
        var expectedRequest = taskTracker.getTaskRequest(report.taskId());
        StatusValidation validation = validateBoundReport(report, expectedRequest);
        if (!validation.accepted()) {
            return false;
        }
        boolean durableWork = isBackgroundTask(expectedRequest);

        ReportOwnership ownership = reportOwnership(report, durableWork);
        if (!ownership.accepted()) return false;
        if (durableWork && isFinishedState(report.state()) && !isScheduledDurableTask(report)) {
            LOG.warn("event=durable_report_rejected_unsubmitted taskId={} attempt={}",
                    report.taskId(), report.attempt());
            return false;
        }

        var sequence = sequences.reserveSequence(
                report.taskId(), report.reportSequence());
        if (sequence == null) {
            LOG.warn("event=status_report_rejected_stale taskId={} reportSeq={}",
                    report.taskId(), report.reportSequence());
            return false;
        }
        try (sequence) {
            boolean accepted = publishReport(expectedRequest, report, validation, ownership, durableWork);
            if (accepted) sequence.accept();
            return accepted;
        }
    }

    private boolean publishReport(SubmitTaskRequest expectedRequest, TaskStatusReport report,
            StatusValidation validation, ReportOwnership ownership, boolean durableWork) {
        boolean resourceOnly = taskAttemptIsNoLongerActive(report);
        if (resourceOnly && validation.canonicalStatus().filter(status -> status.getExecutionDrainState()
                != org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED).isEmpty()
                && !taskTracker.hasPendingReportPublication(report)) {
            // Neither a new winner nor a physical-exit receipt can be acknowledged.
            // Do not freeze a rejected PENDING report: its exact retry must remain
            // eligible after real scheduling. Already-owned publication retries
            // preserve their first disposition and required observer obligations.
            return false;
        }
        try (var publication = taskTracker.enterReportPublication(expectedRequest, report, resourceOnly)) {
            report = publication.report();
            recordExecutionDrain(expectedRequest, validation);
            boolean accepted = publication.publicationCompleted();
            if (!accepted) {
                journalTaskEvent(report, expectedRequest);
                accepted = publication.resourceOnly() || processReservedReport(report, expectedRequest, validation,
                        ownership.owner(), durableWork);
            }
            if (accepted) {
                publication.accepted();
            }
            return accepted;
        }
    }

    private StatusValidation validateBoundReport(TaskStatusReport report, SubmitTaskRequest expected) {
        if (expected == null) {
            LOG.warn("event=status_report_rejected_unbound_task taskId={}", report.taskId());
            return StatusValidation.rejected();
        }
        return validateRegisteredTask(report);
    }

    private ReportOwnership reportOwnership(TaskStatusReport report, boolean durable) {
        WriteCommitFencer fencer = writes.fencer();
        if (fencer == null || durable) return new ReportOwnership(true, null);
        try {
            ExecutionAnchor anchor = fencer.currentAnchor(report.queryId());
            return new ReportOwnership(anchor != null, anchor == null ? null : anchor.ownerId());
        } catch (IllegalStateException staleOwner) {
            LOG.info("event=status_report_rejected_stale_owner queryId={} taskId={}",
                    report.queryId(), report.taskId());
            return new ReportOwnership(false, null);
        }
    }

    private boolean isScheduledDurableTask(TaskStatusReport report) {
        try {
            var state = taskTracker.getTaskState(report.queryId(), report.taskId());
            return state == org.pangrova.coordinator.task.TaskStateMachine.State.SCHEDULED
                    || state == org.pangrova.coordinator.task.TaskStateMachine.State.RUNNING;
        } catch (IllegalArgumentException notRegistered) {
            return false;
        }
    }

    private record ReportOwnership(boolean accepted, String owner) {}

    private boolean processReservedReport(TaskStatusReport report, SubmitTaskRequest expected,
            StatusValidation validation, String owner, boolean durable) {
        if (durable && isFinishedState(report.state())
                && !commitDurableReport(expected, report, validation.canonicalStatus().orElseThrow())) {
            recordExecutionDrain(expected, validation);
            LOG.warn("event=artifact_report_rejected_durable taskId={} attempt={}", report.taskId(), report.attempt());
            return validation.canonicalStatus().filter(status -> status.getExecutionDrainState()
                    != org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED).isPresent();
        }
        // Resource termination is exact-issued-attempt evidence. Retain it
        // before notifying the DAG, whose terminal callbacks can retire identity.
        if (expected != null) recordExecutionDrain(expected, validation);
        if (expected != null && taskTracker.getTaskRequest(report.taskId()) == null) {
            return true; // exact drain observer retired a speculative loser, not a new winner
        }
        return processAcceptedStatus(report, owner, durable);
    }

    private void recordExecutionDrain(SubmitTaskRequest request, StatusValidation validation) {
        validation.canonicalStatus().filter(status -> status.getExecutionDrainState()
                != org.pangrova.protocol.v2.ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED)
                .ifPresent(status -> taskTracker.recordExecutionDrain(request, status));
    }

    private boolean commitDurableReport(SubmitTaskRequest request, TaskStatusReport report,
            org.pangrova.protocol.v2.TaskReport status) {
        try {
            return artifactWorkReportSink.commit(request.taskRequest().getSecurity().getTenantId(),
                    status, report.canonicalTaskReport());
        } catch (org.pangrova.spi.security.AuthorizationDeniedException
                | org.pangrova.generation.api.execution.GenerationExecutionOwnerLostException fenced) {
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
            return speculation.handleSpeculativeFailure(
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

        if (isFinishedState(report.state())) {
            return speculation.handleTaskCompletion(queryId, taskId, report);
        }

        notifyAcceptedTerminal(report);
        evaluateQueryTransitions(queryId, report);
        return true;
    }

    private boolean shouldHandleSpeculativeFailure(TaskStatusReport report, boolean durable) {
        return !durable && speculation.isSpeculationActive() && isUnsuccessfulState(report.state())
                && speculation.findCounterpartTaskId(report.taskId()) != null;
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
                && (!isFinishedState(report.state()) || !speculation.isSpeculationActive())) {
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

    /** Retires this task's sequence owner after its actual publication operation exits. */
    public void clearTaskSequence(String taskId) { sequences.clearTaskSequence(taskId); }

    private void rejectIfUnregistered(String workerId) {
        if (workerManager.getWorker(workerId) == null) {
            LOG.warn("event=status_report_rejected_unregistered workerId={}",
                    workerId);
            throw new UnregisteredWorkerException(workerId);
        }
    }

    private boolean updateTaskState(String queryId, String taskId, TaskStatusReport report) {
        // Capture the Coordinator observation before the state transition: a
        // concurrent FINISHED report can freeze its end time immediately after
        // RUNNING is accepted, before this thread records the start.
        Instant startObservedAt = Instant.now();
        try {
            boolean accepted = taskTracker.updateTaskState(queryId, taskId, report.state());
            if (accepted && "RUNNING".equals(report.state())) {
                // Only a fenced, canonical Worker report proves the original attempt
                // entered execution. The speculation scheduler needs this stage and
                // start time to compare it with completed peers.
                taskTracker.recordTaskStart(taskId, String.valueOf(report.stageId()), startObservedAt);
            }
            return accepted;
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
        stageCompletionCallback.onTaskTerminal(queryId, logicalTaskId,
                report.state(), report.error());
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

    private void journalTaskEvent(TaskStatusReport report, SubmitTaskRequest issued) {
        TaskStateEvent event = new TaskStateEvent(org.pangrova.coordinator.task.TaskJournalEventContext.create(
                issued.taskRequest().getAttempt(), report.reportSequence(), report.timestampMs()),
                report.queryId(), report.taskId(), report.workerId(), report.state(),
                report.stageId(), report.reportSequence());
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

    /**
     * Collects commit metadata from a completed write task's status report.
     * When a worker completes a native write task, the report contains a
     * WriteResultDescriptor with serialized commit metadata. This method
     * extracts and stores the metadata for later aggregation.
     */
}
