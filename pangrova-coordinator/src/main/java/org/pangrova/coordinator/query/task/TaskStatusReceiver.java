package org.pangrova.coordinator.query.task;

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
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.coordinator.coordination.inmemory.InMemoryQueryStateJournal;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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

    /** A-FENCE-01: optional leadership fence for the catalog-commit path (null = single-node/no HA). */
    private volatile WriteCommitFencer writeCommitFencer;

    /** Tracks the last accepted reportSequence per taskId for deduplication. */
    private final ConcurrentHashMap<String, AtomicLong> lastAcceptedSequence = new ConcurrentHashMap<>();

    /** Single-finalizer guard: prevents concurrent finalization of the same query. */
    private final ConcurrentHashMap<String, Boolean> finalizationGuard = new ConcurrentHashMap<>();

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
    }

    /**
     * A-FENCE-01: installs the leadership fence used to reject catalog commits from a demoted
     * (zombie) coordinator and to stamp the owning coordinator/term on durable write intents.
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

        if (!acceptSequence(report.taskId(), report.reportSequence())) {
            LOG.warn("event=status_report_rejected_stale taskId={} reportSeq={}",
                    report.taskId(), report.reportSequence());
            return false;
        }

        String queryId = report.queryId();
        String taskId = report.taskId();
        updateTaskState(queryId, taskId, report);

        // Notify DAG engine of terminal task states BEFORE query-level transitions,
        // so the DAG engine gets the event first and can drive stage state changes.
        if (stageCompletionCallback != null && isTerminalTaskState(report.state())) {
            try {
                stageCompletionCallback.onTaskTerminal(queryId, taskId,
                        report.state(), report.error());
            } catch (Exception e) {
                LOG.error("event=stage_completion_callback_failed queryId={} taskId={}"
                        + " state={} error={}", queryId, taskId, report.state(),
                        e.getMessage(), e);
            }
        }

        // Journal: sync for terminal states, async for non-terminal
        journalTaskEvent(report);

        if (isFinishedState(report.state())) {
            return handleTaskCompletion(queryId, taskId, report);
        }

        evaluateQueryTransitions(queryId, report);
        return true;
    }

    /**
     * Accepts the report sequence only if it is strictly greater than the last
     * accepted sequence for this task. Thread-safe via CAS loop.
     */
    private boolean acceptSequence(String taskId, long sequence) {
        AtomicLong last = lastAcceptedSequence.computeIfAbsent(taskId, k -> new AtomicLong(-1));
        while (true) {
            long current = last.get();
            if (sequence <= current) {
                return false;
            }
            if (last.compareAndSet(current, sequence)) {
                return true;
            }
        }
    }

    /** Clears sequence tracking for a completed/cancelled task to prevent memory leak. */
    public void clearTaskSequence(String taskId) {
        lastAcceptedSequence.remove(taskId);
    }

    private void rejectIfUnregistered(String workerId) {
        if (workerManager.getWorker(workerId) == null) {
            LOG.warn("event=status_report_rejected_unregistered workerId={}",
                    workerId);
            throw new UnregisteredWorkerException(workerId);
        }
    }

    private void updateTaskState(String queryId, String taskId, TaskStatusReport report) {
        try {
            taskTracker.updateTaskState(queryId, taskId, report.state());
        } catch (IllegalArgumentException e) {
            LOG.debug("Task not yet registered, registering on-the-fly: queryId={}, taskId={}",
                    queryId, taskId);
            ensureTaskRegistered(queryId, taskId);
            taskTracker.updateTaskState(queryId, taskId, report.state());
        }
    }

    private boolean handleTaskCompletion(String queryId, String taskId, TaskStatusReport report) {
        collectWriteCommitMetadata(queryId, report);
        if (!isSpeculationActive()) {
            evaluateQueryTransitions(queryId, report);
            return true;
        }
        return resolveSpeculativeCompletion(queryId, taskId, report);
    }

    private boolean resolveSpeculativeCompletion(String queryId, String taskId,
                                                  TaskStatusReport report) {
        boolean isWinner = duplicateResolver.onTaskComplete(
                taskId, String.valueOf(report.stageId()), report.partitionId());

        if (isWinner) {
            cancelLoserTask(queryId, taskId, report);
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
        taskTracker.removeDeadline(taskId);
        taskTracker.clearTaskTracking(taskId);
        clearTaskSequence(taskId);
        speculativeLauncher.removePair(resolveOriginalTaskId(taskId));
    }

    private void cancelAndCleanup(String queryId, String taskId) {
        sendCancelToWorker(taskId);
        try {
            taskTracker.updateTaskState(queryId, taskId, "CANCELLED");
        } catch (Exception e) {
            LOG.debug("event=loser_cancel_state_update_failed queryId={} taskId={} error={}",
                    queryId, taskId, e.getMessage());
        }
        taskTracker.removeDeadline(taskId);
        taskTracker.clearTaskTracking(taskId);
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

    private static boolean isTerminalTaskState(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state);
    }

    private void journalTaskEvent(TaskStatusReport report) {
        // SI-6: stamp the owning coordinator so owner-scoped failover recovery can
        // filter task rows; sourced from the fencer (same identity as write intents).
        WriteCommitFencer fencer = this.writeCommitFencer;
        String owningCoordinator = fencer != null ? fencer.coordinatorId() : null;
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
                commitDistributedWriteIfNeeded(queryId);
            } catch (Exception e) {
                LOG.error("event=distributed_write_commit_failed queryId={} error={}",
                        queryId, e.getMessage(), e);
                cleanupWriteStateOnFailure(queryId);
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
     * Prevents unbounded growth of {@link #lastAcceptedSequence} map.
     */
    private void clearQuerySequences(String queryId) {
        for (String taskId : taskTracker.getTaskIds(queryId)) {
            lastAcceptedSequence.remove(taskId);
        }
    }

    /**
     * Ensures a task is registered in the tracker. Handles the race where
     * Worker status arrives before Coordinator's registerDispatchedTask call.
     */
    private void ensureTaskRegistered(String queryId, String taskId) {
        try {
            taskTracker.registerTask(queryId, taskId);
            taskTracker.markScheduled(queryId, taskId);
        } catch (IllegalStateException e) {
            // Already registered by another thread — safe to ignore
        }
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
    private void commitDistributedWriteIfNeeded(String queryId) {
        if (writeCommitTracker == null || catalogRegistry == null) {
            return;
        }

        // Idempotent recovery: skip if the journal says this write is already committed
        if (writeCommitJournal.isCommitted(queryId)) {
            LOG.info("event=distributed_write_already_committed queryId={}", queryId);
            writeCommitTracker.removeWriteState(queryId);
            writeCommitJournal.purge(queryId);
            return;
        }

        // WAL ordering (F-WRITE-001 / A-COMMIT-01): peek — do NOT remove the tracker
        // state yet. The tracker entry holds the only copy of the write context
        // (session / output handle / fragments); it must outlive the durable intent
        // record and the catalog commit so that any failure before COMMITTED leaves a
        // recoverable state instead of destroying the write context prematurely.
        NativeWriteCommitTracker.WriteCommitState writeState =
                writeCommitTracker.peekWriteState(queryId);
        if (writeState == null) {
            return;
        }

        // 1. Record a durable, REPLAYABLE PENDING intent BEFORE touching the catalog.
        //    A-COMMIT-01: the intent now carries the worker commit fragments and an
        //    idempotency key (queryId) so a replacement coordinator can reconstruct and
        //    finish — or safely skip — the commit after a crash, rather than only observing
        //    a state marker. If this throws (journal store unavailable), the tracker state
        //    is still intact for a subsequent retry.
        List<byte[]> fragments = writeState.fragments();
        var outputHandle = writeState.outputHandle();
        // A-COMMIT-01: record STRUCTURED, rehydratable identity (schema.table + real write mode) so
        // a replacement coordinator can reconstruct the handle and re-drive/skip the commit after a
        // crash, instead of a lossy handle.toString() that cannot be reconstructed.
        String structuredTable = outputHandle.targetTable()
                .map(Object::toString)
                .orElseGet(outputHandle::toString);
        // A-FENCE-01: stamp the owning coordinator id + leadership term on the durable intent.
        WriteCommitFencer fencer = this.writeCommitFencer;
        String owningCoordinator = fencer != null ? fencer.coordinatorId() : null;
        long owningTerm = fencer != null ? fencer.currentTerm() : 0L;
        writeCommitJournal.recordIntent(new WriteCommitEntry(
                queryId, writeState.catalogName(), structuredTable,
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(),
                fragments, outputHandle.writeMode(), queryId, owningCoordinator, owningTerm));

        // A-FENCE-01: fence a DEMOTED (zombie) leader BEFORE touching the catalog. If this
        // coordinator is no longer the active leader, do NOT commit — leave the PENDING intent and
        // tracker state for the new leader's WriteCommitReconciler to auto-finish (A-COMMIT-01).
        if (fencer != null && !fencer.isActiveLeader()) {
            LOG.error("event=distributed_write_fenced queryId={} owningTerm={} coordinator={} "
                    + "reason=not_active_leader note=left_PENDING_for_new_leader_reconcile",
                    queryId, owningTerm, owningCoordinator);
            return;
        }

        // 2. Perform the single atomic catalog commit.
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(writeState.catalogName());
        ConnectorMetadata metadata = catalogHandle.getMetadata(writeState.session());
        metadata.finishDistributedWrite(writeState.session(), writeState.outputHandle(), fragments);

        // 3. Durably mark COMMITTED. Only after this record is it safe to drop the
        //    in-memory tracker state; a crash before this point leaves a PENDING
        //    journal entry plus the tracker state for the recovery path to reconcile.
        writeCommitJournal.markCommitted(queryId);

        // 4. Commit is durable — release the in-memory tracker state and purge.
        writeCommitTracker.removeWriteState(queryId);
        writeCommitJournal.purge(queryId);

        LOG.info("event=distributed_write_committed queryId={} catalog={} fragmentCount={}",
                queryId, writeState.catalogName(), fragments.size());
    }

    /**
     * Cleans up write state when a query fails, preventing memory leaks.
     * Also purges any pending journal entry for the query.
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
