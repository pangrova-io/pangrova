package org.pangrova.metadata.control;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable policy and history for controlled-table maintenance.
 *
 * <p>Implementations must not synthesize "allowed" answers. A missing policy is
 * resolved by {@link TableControlMaintenanceService}, which applies ownership
 * defaults; the store itself only reports what was persisted.</p>
 */
public interface TableControlMaintenanceStore {

    Optional<TableMaintenancePolicy> findPolicy(UUID tableId);

    void savePolicy(TableMaintenancePolicy policy);

    void saveRun(TableMaintenanceRun run);

    List<TableMaintenanceRun> listRuns(UUID tableId, int limit);
}
