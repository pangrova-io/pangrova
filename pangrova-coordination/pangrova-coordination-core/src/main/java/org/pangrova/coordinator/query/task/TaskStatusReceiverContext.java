package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.query.task.report.GenerationWorkReportSink;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.query.api.state.QueryStateJournal;
import org.pangrova.connector.spi.coordination.WriteCommitJournal;
import org.pangrova.scheduler.core.speculation.DuplicateTaskResolver;
import org.pangrova.scheduler.core.speculation.SpeculativeTaskLauncher;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Groups TaskStatusReceiver dependencies into a single parameter object.
 * Core dependencies (queryManager, taskTracker, workerManager) are required;
 * speculation dependencies (speculativeLauncher, duplicateResolver, taskDispatcher)
 * are nullable — speculation is disabled when null.
 * Write commit dependencies (catalogRegistry, writeCommitTracker) are nullable —
 * native write commit is disabled when null.
 * Stage completion callback (stageCompletionCallback) is nullable — DAG task→stage
 * bridge is disabled when null.
 * Write commit journal (writeCommitJournal) is nullable — defaults to
 * InMemoryWriteCommitJournal when null.
 */
public record TaskStatusReceiverContext(
        QueryManager queryManager,
        CoordinatorTaskTracker taskTracker,
        WorkerManager workerManager,
        SpeculativeTaskLauncher speculativeLauncher,
        DuplicateTaskResolver duplicateResolver,
        TaskDispatcher taskDispatcher,
        QueryStateJournal stateJournal,
        CatalogRegistry catalogRegistry,
        NativeWriteCommitTracker writeCommitTracker,
        StageCompletionCallback stageCompletionCallback,
        LineageEventEmitter lineageEmitter,
        WriteCommitJournal writeCommitJournal,
        GenerationWorkReportSink artifactWorkReportSink) {

    /** Compatibility constructor for callers that cannot dispatch background artifact work. */
    // The compatibility adapter retains exact fields for existing report producers.
    @SuppressWarnings("checkstyle:ParameterNumber")
    public TaskStatusReceiverContext(
            QueryManager queryManager,
            CoordinatorTaskTracker taskTracker,
            WorkerManager workerManager,
            SpeculativeTaskLauncher speculativeLauncher,
            DuplicateTaskResolver duplicateResolver,
            TaskDispatcher taskDispatcher,
            QueryStateJournal stateJournal,
            CatalogRegistry catalogRegistry,
            NativeWriteCommitTracker writeCommitTracker,
            StageCompletionCallback stageCompletionCallback,
            LineageEventEmitter lineageEmitter,
            WriteCommitJournal writeCommitJournal) {
        this(queryManager, taskTracker, workerManager, speculativeLauncher,
                duplicateResolver, taskDispatcher, stateJournal, catalogRegistry,
                writeCommitTracker, stageCompletionCallback, lineageEmitter,
                writeCommitJournal, GenerationWorkReportSink.rejecting());
    }

    public TaskStatusReceiverContext {
        checkNotNull(queryManager, "queryManager must not be null");
        checkNotNull(taskTracker, "taskTracker must not be null");
        checkNotNull(workerManager, "workerManager must not be null");
        // speculativeLauncher is nullable — speculation disabled when null
        // duplicateResolver is nullable — speculation disabled when null
        // taskDispatcher is nullable — speculation disabled when null
        // stateJournal is nullable — defaults to the QueryManager's exact journal owner
        // catalogRegistry is nullable — native write commit disabled when null
        // writeCommitTracker is nullable — native write commit disabled when null
        // stageCompletionCallback is nullable — DAG bridge disabled when null
        // lineageEmitter is nullable — lineage disabled when null
        // writeCommitJournal is nullable — defaults to InMemoryWriteCommitJournal
        artifactWorkReportSink = artifactWorkReportSink == null
                ? GenerationWorkReportSink.rejecting() : artifactWorkReportSink;
    }

    /** Factory for non-speculative mode (core dependencies only). */
    public static TaskStatusReceiverContext withoutSpeculation(
            QueryManager queryManager,
            CoordinatorTaskTracker taskTracker,
            WorkerManager workerManager) {
        return new TaskStatusReceiverContext(
                queryManager, taskTracker, workerManager, null, null, null,
                queryManager.getStateJournal(), null, null, null, null, null,
                GenerationWorkReportSink.rejecting());
    }
}
