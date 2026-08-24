package org.pangrova.coordinator.dynamictable.schedule;

import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.refresh.DagProvider;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.metadata.dynamictable.DynamicTableStateStore;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Aggregated dependencies for {@link RefreshScheduler}.
 *
 * <p>Keeps the scheduler constructor at max 2 params (this + config).</p>
 *
 * @param refreshExecutor executes a single refresh cycle
 * @param stateStore      persistent Dynamic Table state store
 * @param config          Dynamic Table configuration
 * @param leaderCheck     returns true if this node is the HA leader; refresh is skipped otherwise
 * @param dagProvider     provides downstream dependency graph for cascade refresh (nullable)
 */
public record RefreshSchedulerContext(
        RefreshExecutor refreshExecutor,
        DynamicTableStateStore stateStore,
        DynamicTableConfig config,
        BooleanSupplier leaderCheck,
        DagProvider dagProvider
) {

    /**
     * Primary constructor with all fields.
     */
    public RefreshSchedulerContext {
        Objects.requireNonNull(refreshExecutor, "refreshExecutor must not be null");
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(leaderCheck, "leaderCheck must not be null");
        // dagProvider is nullable — cascade refresh is disabled when null
    }

    /**
     * Convenience constructor with leader check but no cascade (dagProvider = null).
     */
    public RefreshSchedulerContext(
            RefreshExecutor refreshExecutor,
            DynamicTableStateStore stateStore,
            DynamicTableConfig config,
            BooleanSupplier leaderCheck) {
        this(refreshExecutor, stateStore, config, leaderCheck, null);
    }

    /**
     * Convenience constructor that defaults to always-leader and no cascade (single-node mode).
     */
    public RefreshSchedulerContext(
            RefreshExecutor refreshExecutor,
            DynamicTableStateStore stateStore,
            DynamicTableConfig config) {
        this(refreshExecutor, stateStore, config, () -> true, null);
    }
}
