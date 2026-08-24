package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.metadata.dynamictable.DynamicTableState;

/**
 * Abstraction for registering a Dynamic Table for periodic refresh.
 * Breaks the circular dependency between refresh and schedule packages.
 */
public interface RefreshRegistrar {

    /**
     * Register a Dynamic Table for scheduled refresh.
     *
     * @param state the DT state to register
     */
    void register(DynamicTableState state);
}
