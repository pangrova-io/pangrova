package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.index.IndexAwareSplitFetcher;
import org.pangrova.coordinator.query.cache.CacheWritebackConfig;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.resilience.ResilienceRegistry;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.coordinator.query.execution.dispatch.dag.SpillBudget;
import org.pangrova.shuffle.ShuffleLifecycleManager;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Groups QueryDispatcher dependencies into a single parameter object.
 * Fields are organized into sub-context bundles for readability:
 * {@link SecurityContextBundle}, {@link CacheContextBundle}, {@link SchedulingContextBundle}.
 * Keeps the constructor within the ≤4 param governance limit when combined with StageFragmenter.
 */
public record QueryDispatcherContext(
        SqlCompiler compiler,
        SecurityContextBundle security,
        SchedulingContextBundle scheduling,
        CacheContextBundle cache,
        CatalogRegistry catalogRegistry,
        LineageEventEmitter lineageEmitter,
        CoordinatorResultService coordinatorResultService,
        long taskTimeoutSeconds,
        RuntimeFilterRegistry runtimeFilterRegistry,
        AdaptiveQueryExecutor adaptiveQueryExecutor,
        AdaptiveQueryConfig adaptiveQueryConfig,
        ShuffleLifecycleManager shuffleLifecycleManager,
        HighAvailabilityCoordinationService haService,
        String coordinatorId,
        QueryStateJournal stateJournal,
        IndexAwareSplitFetcher indexAwareSplitFetcher) {

    /** Compact constructor — validates required dependencies. */
    public QueryDispatcherContext {
        checkNotNull(compiler, "compiler must not be null");
        checkNotNull(scheduling, "scheduling must not be null");
    }

    // ── Convenience accessors — delegate to sub-bundles ──

    public AccessControlEnforcer accessControlEnforcer() {
        return security != null ? security.accessControlEnforcer() : null;
    }

    public ColumnMaskApplier columnMaskApplier() {
        return security != null ? security.columnMaskApplier() : null;
    }

    public RowFilterApplier rowFilterApplier() {
        return security != null ? security.rowFilterApplier() : null;
    }

    public TaskDispatcher dispatcher() {
        return scheduling.taskDispatcher();
    }

    public TaskScheduler taskScheduler() {
        return scheduling.taskScheduler();
    }

    public WorkerManager workerManager() {
        return scheduling.workerManager();
    }

    public CoordinatorTaskTracker taskTracker() {
        return scheduling.taskTracker();
    }

    public SplitAssignmentManager splitAssignmentManager() {
        return scheduling.splitAssignmentManager();
    }

    public SpeculativeStageHandler speculativeStageHandler() {
        return scheduling.speculativeStageHandler();
    }

    public SpillBudget spillBudget() {
        return scheduling.spillBudget();
    }

    public ResilienceRegistry resilienceRegistry() {
        return scheduling.resilienceRegistry();
    }

    public CacheWritebackConfig cacheWritebackConfig() {
        return cache != null ? cache.cacheWritebackConfig() : null;
    }

    /**
     * Factory method for callers that only need the core parameters.
     * Optional fields default to null.
     */
    public static QueryDispatcherContext withDefaults(
            SqlCompiler compiler,
            AccessControlEnforcer accessControlEnforcer,
            ColumnMaskApplier columnMaskApplier,
            RowFilterApplier rowFilterApplier,
            TaskDispatcher dispatcher,
            TaskScheduler taskScheduler,
            WorkerManager workerManager,
            CoordinatorTaskTracker taskTracker,
            CatalogRegistry catalogRegistry,
            ResilienceRegistry resilienceRegistry,
            LineageEventEmitter lineageEmitter,
            CoordinatorResultService coordinatorResultService,
            long taskTimeoutSeconds) {
        var security = new SecurityContextBundle(accessControlEnforcer, columnMaskApplier, rowFilterApplier);
        var scheduling = new SchedulingContextBundle(
                dispatcher, taskScheduler, workerManager, taskTracker,
                null, null, null, resilienceRegistry);
        return new QueryDispatcherContext(
                compiler, security, scheduling, CacheContextBundle.none(),
                catalogRegistry, lineageEmitter, coordinatorResultService, taskTimeoutSeconds,
                null, null, null, null, null, null, null, null);
    }
}
