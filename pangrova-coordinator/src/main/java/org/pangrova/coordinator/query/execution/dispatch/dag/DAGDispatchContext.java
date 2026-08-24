package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.dispatch.shuffle.LocalShuffleLifecycleManager;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.resilience.ResilienceRegistry;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.shuffle.ShuffleLifecycleManager;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable dependency bundle for {@link DAGDispatchEngine}.
 * Replaces 12-param telescoping constructors with a single typed record.
 */
public record DAGDispatchContext(
        TaskDispatcher dispatcher,
        CoordinatorTaskTracker taskTracker,
        LineageEventEmitter lineageEmitter,
        CatalogRegistry catalogRegistry,
        long taskTimeoutSeconds,
        AdaptiveQueryExecutor adaptiveQueryExecutor,
        RuntimeFilterRegistry runtimeFilterRegistry,
        AdaptiveQueryConfig adaptiveQueryConfig,
        SpeculativeStageHandler speculativeStageHandler,
        PipelineConfig pipelineConfig,
        ShuffleLifecycleManager shuffleLifecycleManager,
        SplitAssignmentManager splitAssignmentManager,
        ResilienceRegistry resilienceRegistry,
        QueryStateJournal stateJournal
) {
    public DAGDispatchContext(
            TaskDispatcher dispatcher,
            CoordinatorTaskTracker taskTracker,
            LineageEventEmitter lineageEmitter,
            CatalogRegistry catalogRegistry,
            long taskTimeoutSeconds,
            AdaptiveQueryExecutor adaptiveQueryExecutor,
            RuntimeFilterRegistry runtimeFilterRegistry,
            AdaptiveQueryConfig adaptiveQueryConfig,
            SpeculativeStageHandler speculativeStageHandler,
            PipelineConfig pipelineConfig,
            ShuffleLifecycleManager shuffleLifecycleManager,
            SplitAssignmentManager splitAssignmentManager,
            ResilienceRegistry resilienceRegistry,
            QueryStateJournal stateJournal) {
        checkNotNull(dispatcher, "dispatcher must not be null");
        // catalogRegistry: nullable — absent in minimal bootstrap without metadata module
        checkNotNull(splitAssignmentManager, "splitAssignmentManager must not be null");
        this.dispatcher = dispatcher;
        this.taskTracker = taskTracker;
        this.lineageEmitter = lineageEmitter;
        this.catalogRegistry = catalogRegistry;
        this.taskTimeoutSeconds = taskTimeoutSeconds;
        this.adaptiveQueryExecutor = adaptiveQueryExecutor;
        this.runtimeFilterRegistry = runtimeFilterRegistry;
        this.adaptiveQueryConfig = adaptiveQueryConfig;
        this.speculativeStageHandler = speculativeStageHandler;
        this.pipelineConfig = pipelineConfig != null ? pipelineConfig : PipelineConfig.DISABLED;
        this.shuffleLifecycleManager = shuffleLifecycleManager != null
                ? shuffleLifecycleManager : LocalShuffleLifecycleManager.INSTANCE;
        this.splitAssignmentManager = splitAssignmentManager;
        // resilienceRegistry: nullable — absent when resilience module is disabled
        this.resilienceRegistry = resilienceRegistry;
        // stateJournal: nullable — absent when HA journaling is disabled
        this.stateJournal = stateJournal;
    }

    /**
     * Backward-compatible constructor without stateJournal.
     * Used by existing callers that don't need dispatch-level journaling.
     */
    public DAGDispatchContext(
            TaskDispatcher dispatcher,
            CoordinatorTaskTracker taskTracker,
            LineageEventEmitter lineageEmitter,
            CatalogRegistry catalogRegistry,
            long taskTimeoutSeconds,
            AdaptiveQueryExecutor adaptiveQueryExecutor,
            RuntimeFilterRegistry runtimeFilterRegistry,
            AdaptiveQueryConfig adaptiveQueryConfig,
            SpeculativeStageHandler speculativeStageHandler,
            PipelineConfig pipelineConfig,
            ShuffleLifecycleManager shuffleLifecycleManager,
            SplitAssignmentManager splitAssignmentManager,
            ResilienceRegistry resilienceRegistry) {
        this(dispatcher, taskTracker, lineageEmitter, catalogRegistry,
                taskTimeoutSeconds, adaptiveQueryExecutor, runtimeFilterRegistry,
                adaptiveQueryConfig, speculativeStageHandler, pipelineConfig,
                shuffleLifecycleManager, splitAssignmentManager, resilienceRegistry,
                null);
    }
}
