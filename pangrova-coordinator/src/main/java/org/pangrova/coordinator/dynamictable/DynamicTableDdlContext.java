package org.pangrova.coordinator.dynamictable;

import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.streaming.StreamScheduler;
import org.pangrova.metadata.dynamictable.DynamicTableStateStore;
import org.pangrova.metadata.dynamictable.RefreshHistoryStore;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.spi.streaming.CdcLifecycleManager;

import java.util.Objects;

/**
 * Aggregated dependencies for {@link DynamicTableDdlExecutor}.
 *
 * <p>Keeps the executor constructor at 1 param (this record),
 * satisfying the max-4-constructor-params rule.</p>
 */
public record DynamicTableDdlContext(
        DynamicTableStateStore stateStore,
        RefreshHistoryStore historyStore,
        MaterializedViewRegistry materializedViewRegistry,
        RefreshScheduler scheduler,
        DynamicTableDagManager dagManager,
        IncrementalFeasibilityChecker feasibilityChecker,
        CdcLifecycleManager cdcLifecycleManager,
        RefreshExecutor refreshExecutor,
        DynamicTableConfig config,
        StreamScheduler streamScheduler
) {

    public DynamicTableDdlContext {
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(historyStore, "historyStore must not be null");
        Objects.requireNonNull(materializedViewRegistry, "materializedViewRegistry must not be null");
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        Objects.requireNonNull(dagManager, "dagManager must not be null");
        Objects.requireNonNull(feasibilityChecker, "feasibilityChecker must not be null");
        Objects.requireNonNull(cdcLifecycleManager, "cdcLifecycleManager must not be null");
        Objects.requireNonNull(refreshExecutor, "refreshExecutor must not be null");
        Objects.requireNonNull(config, "config must not be null");
        // streamScheduler is nullable — only available when streaming subsystem is active
    }

    /**
     * Backward-compatible constructor without streamScheduler.
     */
    // CHECKSTYLE.OFF: ParameterNumber
    // Kept for backward compatibility with existing callers predating streamScheduler.
    public DynamicTableDdlContext(
            DynamicTableStateStore stateStore,
            RefreshHistoryStore historyStore,
            MaterializedViewRegistry materializedViewRegistry,
            RefreshScheduler scheduler,
            DynamicTableDagManager dagManager,
            IncrementalFeasibilityChecker feasibilityChecker,
            CdcLifecycleManager cdcLifecycleManager,
            RefreshExecutor refreshExecutor,
            DynamicTableConfig config) {
        this(stateStore, historyStore, materializedViewRegistry, scheduler,
                dagManager, feasibilityChecker, cdcLifecycleManager, refreshExecutor,
                config, null);
    }
    // CHECKSTYLE.ON: ParameterNumber
}
