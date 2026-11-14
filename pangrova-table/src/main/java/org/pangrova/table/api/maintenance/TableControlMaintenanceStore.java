package org.pangrova.table.api.maintenance;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.table.api.maintenance.TableMaintenancePolicy;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable policy and history for controlled-table maintenance.
 *
 * <p>Implementations must not synthesize "allowed" answers. A missing policy is
 * resolved by the caller's table maintenance service, which applies ownership
 * defaults; the store itself only reports what was persisted.</p>
 */
@Internal
public interface TableControlMaintenanceStore {

    Optional<TableMaintenancePolicy> findPolicy(UUID tableId);

    void savePolicy(TableMaintenancePolicy policy);

    void saveRun(TableMaintenanceRun run);

    List<TableMaintenanceRun> listRuns(UUID tableId, int limit);
}
