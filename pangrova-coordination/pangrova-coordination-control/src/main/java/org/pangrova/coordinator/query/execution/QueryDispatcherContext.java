package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.indexing.IndexAwareSplitFetcher;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.coordinator.scheduler.placement.CacheAffinityAssigner;
import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.ShuffleRetryPolicyValidator;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Groups QueryDispatcher dependencies into a single parameter object.
 * Fields are organized into sub-context bundles for readability:
 * {@link SecurityContextBundle} and {@link SchedulingContextBundle}.
 * Publication read services are explicit borrowed dependencies, independent of index pruning.
 */
public record QueryDispatcherContext(
        org.pangrova.shuffle.api.SkewDetector skewDetector,
        SqlCompiler compiler,
        SecurityContextBundle security,
        SchedulingContextBundle scheduling,
        CatalogRegistry catalogRegistry,
        LineageEventEmitter lineageEmitter,
        long taskTimeoutSeconds,
        AdaptiveQueryExecutor adaptiveQueryExecutor,
        AdaptiveQueryConfig adaptiveQueryConfig,
        ShuffleLifecycleManager shuffleLifecycleManager,
        HighAvailabilityCoordinationService haService,
        String coordinatorId,
        QueryStateJournal stateJournal,
        IndexAwareSplitFetcher indexAwareSplitFetcher,
        org.pangrova.ai.control.embedding.EmbeddingModelRegistry
                embeddingModelRegistry,
        org.pangrova.generation.publication.read.PublishedGenerationBindingResolver artifactBindingResolver,
        org.pangrova.coordinator.generation.read.GeneratedValueReadResolver generatedValueReadResolver) {

    /** Compact constructor — validates required dependencies. */
    public QueryDispatcherContext {
        checkNotNull(compiler, "compiler must not be null");
        checkNotNull(scheduling, "scheduling must not be null");
        if (shuffleLifecycleManager != null && scheduling.speculativeStageHandler() != null) {
            ShuffleRetryPolicyValidator.validateSpeculation(scheduling.speculativeStageHandler().isEnabled(),
                    shuffleLifecycleManager.capabilities());
        }
        if (adaptiveQueryConfig != null && adaptiveQueryExecutor != null) {
            checkNotNull(skewDetector, "AQE requires an explicitly configured skewDetector");
        }
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

    public CacheAffinityAssigner cacheAffinityAssigner() {
        return scheduling.cacheAffinityAssigner();
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

    /**
     * Factory method for callers that only need the core parameters.
     * Optional fields default to null.
     */
    // The compact test/standalone adapter mirrors the existing constructor contract.
    @SuppressWarnings("checkstyle:ParameterNumber")
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
            long taskTimeoutSeconds) {
        var security = new SecurityContextBundle(accessControlEnforcer, columnMaskApplier, rowFilterApplier);
        var scheduling = new SchedulingContextBundle(
                dispatcher, taskScheduler, workerManager, taskTracker,
                null, null, null, resilienceRegistry);
        return new QueryDispatcherContext(
                null,
                compiler, security, scheduling,
                catalogRegistry, lineageEmitter, taskTimeoutSeconds,
                null, null, null, null, null, null, null, null, null, null);
    }
}
