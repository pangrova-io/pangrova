package org.pangrova.coordinator.query.scheduling;

import java.util.List;

import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.api.speculation.SpeculativeWorkerInventory;

/** Adapts the coordinator's live Worker registry to the scheduler inventory port. */
public final class CoordinatorSpeculativeWorkerInventory implements SpeculativeWorkerInventory {

    private final WorkerManager workerManager;

    public CoordinatorSpeculativeWorkerInventory(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    @Override
    public List<? extends PlacementWorker> activeWorkers() {
        return workerManager.getActiveWorkers();
    }
}
