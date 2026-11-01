package org.pangrova.scheduler.speculation;

import java.util.List;

import org.pangrova.scheduler.api.placement.PlacementWorker;

/** Read-only Worker inventory consumed by speculative placement. */
@FunctionalInterface
public interface SpeculativeWorkerInventory {

    List<? extends PlacementWorker> activeWorkers();
}
