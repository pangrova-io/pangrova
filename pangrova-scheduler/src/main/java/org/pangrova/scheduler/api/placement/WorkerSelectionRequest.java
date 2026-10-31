package org.pangrova.scheduler.api.placement;

import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Request object for worker selection decisions.
 * Contains the tenant context, data file paths for locality-aware placement,
 * and current worker state snapshots.
 *
 * @param tenantId        tenant requesting the task (nullable for default tenant)
 * @param splitFilePaths  data file paths for locality-aware selection
 * @param workerSnapshots current state of all candidate workers
 */
public record WorkerSelectionRequest(
        String tenantId,
        List<String> splitFilePaths,
        List<SchedulingWorkerSnapshot> workerSnapshots) {

    public WorkerSelectionRequest {
        splitFilePaths = splitFilePaths == null ? List.of() : List.copyOf(splitFilePaths);
        workerSnapshots = List.copyOf(checkNotNull(workerSnapshots, "workerSnapshots"));
    }
}
