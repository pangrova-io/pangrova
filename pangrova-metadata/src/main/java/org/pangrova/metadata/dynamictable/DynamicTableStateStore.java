package org.pangrova.metadata.dynamictable;

import org.pangrova.common.exception.StaleEpochException;

import java.util.List;
import java.util.Optional;

/**
 * Persistent store for Dynamic Table incremental refresh state.
 *
 * <p>Implementations must be thread-safe. State is keyed by
 * {@link DynamicTableState#dtName()}.</p>
 */
public interface DynamicTableStateStore {

    /**
     * Save or update the state for a Dynamic Table. Upsert semantics:
     * if a state with the same dtName exists, it is replaced.
     */
    void saveState(DynamicTableState state);

    /**
     * Load the state for a Dynamic Table by name.
     *
     * @return the state, or empty if not found
     */
    Optional<DynamicTableState> loadState(String dtName);

    /**
     * List all Dynamic Tables in ACTIVE state for the given tenant.
     *
     * @param tenantId tenant identifier
     * @return unmodifiable list of active states for the tenant
     */
    List<DynamicTableState> listActiveByTenant(String tenantId);

    /**
     * List all Dynamic Table states regardless of state or tenant.
     *
     * @return unmodifiable list of all states
     */
    List<DynamicTableState> listAll();

    /**
     * Delete the state for a Dynamic Table by name.
     *
     * @return true if the entry existed and was removed
     */
    boolean delete(String dtName);

    /**
     * Conditionally update the state only if the stored epoch matches
     * {@code expectedEpoch}. This provides optimistic concurrency control
     * for HA epoch fencing during refresh operations.
     *
     * <p>Default implementation delegates to {@link #saveState} for backward
     * compatibility. JDBC implementations should use
     * {@code UPDATE ... WHERE epoch = ?} for true CAS semantics.</p>
     *
     * @param state         the new state to persist
     * @param expectedEpoch the epoch value expected in the current stored row
     * @throws StaleEpochException if the stored epoch does not match
     */
    default void saveStateIfEpoch(DynamicTableState state, long expectedEpoch) {
        saveState(state);
    }
}
