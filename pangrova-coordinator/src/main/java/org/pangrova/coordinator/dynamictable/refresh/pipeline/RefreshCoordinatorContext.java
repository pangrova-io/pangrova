package org.pangrova.coordinator.dynamictable.refresh.pipeline;

import org.pangrova.coordinator.dynamictable.DynamicTableRefreshDispatchFunction;
import org.pangrova.coordinator.dynamictable.refresh.IncrementalAnalysisProvider;
import org.pangrova.coordinator.dynamictable.write.DynamicTableWriter;
import org.pangrova.metadata.dynamictable.DynamicTableStateStore;
import org.pangrova.metadata.dynamictable.RefreshHistoryStore;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.IncrementalScanCapability;

import java.util.Objects;

/**
 * Aggregates dependencies for {@link RefreshCoordinator} to satisfy
 * the max-4-constructor-parameter rule.
 *
 * <p>Groups the persistence stores, writer, connector capabilities,
 * and native dispatch function that the coordinator needs for state commit,
 * history recording, data writing, incremental split generation,
 * incremental aggregation merge, and native refresh dispatch.</p>
 *
 * @param stateStore                  persistent store for Dynamic Table state
 * @param historyStore                persistent store for refresh history entries
 * @param writer                      writer for flushing incremental results to the target table
 * @param splitManager                connector split manager for split generation (nullable)
 * @param incrementalScanCapability   connector incremental scan capability (nullable)
 * @param incrementalAnalysisProvider provider for incremental analysis and materialized data (nullable)
 * @param nativeRefreshDispatch       compiles refresh query to Substrait and dispatches to worker (nullable)
 */
public record RefreshCoordinatorContext(
        DynamicTableStateStore stateStore,
        RefreshHistoryStore historyStore,
        DynamicTableWriter writer,
        ConnectorSplitManager splitManager,
        IncrementalScanCapability incrementalScanCapability,
        IncrementalAnalysisProvider incrementalAnalysisProvider,
        DynamicTableRefreshDispatchFunction nativeRefreshDispatch
) {

    /**
     * Full constructor with all dependencies.
     */
    public RefreshCoordinatorContext {
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(historyStore, "historyStore must not be null");
        Objects.requireNonNull(writer, "writer must not be null");
        // splitManager, incrementalScanCapability, incrementalAnalysisProvider,
        // nativeRefreshDispatch are nullable
    }

    /**
     * Constructor with all optional capabilities except native dispatch.
     */
    public RefreshCoordinatorContext(
            DynamicTableStateStore stateStore,
            RefreshHistoryStore historyStore,
            DynamicTableWriter writer,
            ConnectorSplitManager splitManager,
            IncrementalScanCapability incrementalScanCapability,
            IncrementalAnalysisProvider incrementalAnalysisProvider) {
        this(stateStore, historyStore, writer, splitManager,
                incrementalScanCapability, incrementalAnalysisProvider, null);
    }

    /**
     * Constructor with incremental scan support but without incremental analysis.
     */
    public RefreshCoordinatorContext(
            DynamicTableStateStore stateStore,
            RefreshHistoryStore historyStore,
            DynamicTableWriter writer,
            ConnectorSplitManager splitManager,
            IncrementalScanCapability incrementalScanCapability) {
        this(stateStore, historyStore, writer, splitManager, incrementalScanCapability, null, null);
    }

    /**
     * Constructor without optional dependencies.
     */
    public RefreshCoordinatorContext(
            DynamicTableStateStore stateStore,
            RefreshHistoryStore historyStore,
            DynamicTableWriter writer) {
        this(stateStore, historyStore, writer, null, null, null, null);
    }

    /**
     * Whether this context supports incremental split generation.
     */
    public boolean supportsIncrementalSplits() {
        return splitManager != null && incrementalScanCapability != null;
    }

    /**
     * Whether this context supports native refresh dispatch to workers.
     */
    public boolean supportsNativeRefreshDispatch() {
        return nativeRefreshDispatch != null;
    }
}
