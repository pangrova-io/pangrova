package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.dispatch.shuffle.UnavailableExternalShuffleLifecycle;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;

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
    private static final long DEFAULT_TASK_TIMEOUT_SECONDS = 300;

    /** Validates the sole process configuration; zero means its explicit bounded default. */
    public DAGDispatchContext {
        checkNotNull(dispatcher, "dispatcher must not be null");
        // catalogRegistry: nullable — absent in minimal bootstrap without metadata module
        checkNotNull(splitAssignmentManager, "splitAssignmentManager must not be null");
        if (taskTimeoutSeconds < 0) {
            throw new IllegalArgumentException("Configured task timeout must not be negative");
        }
        // Zero denotes an unset process setting, not an unbounded task deadline.
        taskTimeoutSeconds = taskTimeoutSeconds == 0 ? DEFAULT_TASK_TIMEOUT_SECONDS : taskTimeoutSeconds;
        pipelineConfig = pipelineConfig != null ? pipelineConfig : PipelineConfig.DISABLED;
        shuffleLifecycleManager = shuffleLifecycleManager != null
                ? shuffleLifecycleManager : UnavailableExternalShuffleLifecycle.INSTANCE;
        // resilienceRegistry: nullable — absent when resilience module is disabled
        // stateJournal: nullable — absent when HA journaling is disabled
    }

}
