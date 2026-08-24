package org.pangrova.coordinator.dynamictable.refresh.pipeline;

import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.TriggerType;

/**
 * Functional interface for executing a single Dynamic Table refresh.
 *
 * <p>Decouples the scheduler from {@link RefreshCoordinator}'s full
 * dependency tree, enabling lightweight test implementations.</p>
 */
@FunctionalInterface
public interface RefreshExecutor {

    /**
     * Execute a refresh for the given Dynamic Table state.
     *
     * @param state       current Dynamic Table state
     * @param triggerType how this refresh was initiated
     * @return result of the refresh execution
     */
    RefreshResult execute(DynamicTableState state, TriggerType triggerType);
}
