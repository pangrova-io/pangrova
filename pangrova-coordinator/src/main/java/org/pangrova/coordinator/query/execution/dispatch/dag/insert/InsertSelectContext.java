package org.pangrova.coordinator.query.execution.dispatch.dag.insert;

import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.planner.SqlCompiler;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable dependency bundle for InsertSelectDispatchPipeline (worker-direct-write mode).
 *
 * <p>In this mode the worker writes SELECT output directly to the target
 * connector via NativeWriteExecutor. The coordinator never touches data rows,
 * but must register write context with {@code writeCommitTracker} so that
 * {@code TaskStatusReceiver} can collect commit metadata fragments from workers
 * and perform an atomic catalog commit via {@code finishDistributedWrite}.</p>
 */
public record InsertSelectContext(
        SqlCompiler compiler,
        CoordinatorResultService resultService,
        InsertSelectDispatchPipeline.DispatchFunction dispatchFn,
        CoordinatorTaskTracker taskTracker,
        Runnable schemaRefresh,
        java.util.function.Consumer<String> cacheInvalidator,
        org.pangrova.coordinator.query.task.NativeWriteCommitTracker writeCommitTracker,
        org.pangrova.metadata.catalog.CatalogRegistry catalogRegistry) {

    public InsertSelectContext {
        checkNotNull(compiler, "compiler");
        checkNotNull(dispatchFn, "dispatchFn");
        checkNotNull(taskTracker, "taskTracker");
        checkNotNull(schemaRefresh, "schemaRefresh");
        // resultService may be null (fallback path)
        // cacheInvalidator may be null (caching disabled)
        // writeCommitTracker may be null (native write commit disabled)
        // catalogRegistry may be null (when writeCommitTracker is null)
    }

    /** Convenience constructor without cache invalidator and write commit tracking. */
    public InsertSelectContext(
            SqlCompiler compiler,
            CoordinatorResultService resultService,
            InsertSelectDispatchPipeline.DispatchFunction dispatchFn,
            CoordinatorTaskTracker taskTracker,
            Runnable schemaRefresh) {
        this(compiler, resultService, dispatchFn, taskTracker, schemaRefresh, null, null, null);
    }
}
