package org.pangrova.scheduler.api.speculation;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.scheduler.api.placement.PlacementWorker;

/** Read-only Worker inventory consumed by speculative placement. */
@Experimental
@FunctionalInterface
public interface SpeculativeWorkerInventory {

    List<? extends PlacementWorker> activeWorkers();
}
