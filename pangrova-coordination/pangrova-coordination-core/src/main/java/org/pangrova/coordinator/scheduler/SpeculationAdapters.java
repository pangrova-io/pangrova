package org.pangrova.coordinator.scheduler;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.speculation.SpeculativeAttemptCanceller;
import org.pangrova.scheduler.speculation.SpeculativeAttemptDispatcher;
import org.pangrova.scheduler.speculation.SpeculativeQueryInventory;
import org.pangrova.scheduler.speculation.SpeculativeTaskRegistry;
import org.pangrova.scheduler.speculation.SpeculativeWorkerInventory;

/** Composition helpers that bind scheduler speculation ports to coordinator owners. */
public final class SpeculationAdapters {

    private SpeculationAdapters() {}

    public static SpeculativeTaskRegistry taskRegistry(CoordinatorTaskTracker tracker) {
        return new CoordinatorSpeculativeTaskRegistry(tracker);
    }

    public static SpeculativeWorkerInventory workerInventory(WorkerManager workerManager) {
        return new CoordinatorSpeculativeWorkerInventory(workerManager);
    }

    public static SpeculativeAttemptDispatcher dispatcher(TaskDispatcher dispatcher) {
        return new CoordinatorSpeculativeAttemptDispatcher(dispatcher);
    }

    public static SpeculativeAttemptCanceller canceller(TaskCancelRetrier retrier) {
        return new CoordinatorSpeculativeAttemptCanceller(retrier);
    }

    public static SpeculativeQueryInventory queryInventory(QueryManager queryManager) {
        return new CoordinatorSpeculativeQueryInventory(queryManager);
    }
}
