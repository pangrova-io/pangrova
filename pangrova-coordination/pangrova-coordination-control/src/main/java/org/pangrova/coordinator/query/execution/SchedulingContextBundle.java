package org.pangrova.coordinator.query.execution;

import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.scheduler.speculation.SpeculativeStageHandler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.resilience.resilience4j.ResilienceRegistry;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Groups scheduling and execution infrastructure dependencies for QueryDispatcher.
 * Required fields: taskDispatcher, taskScheduler, workerManager, taskTracker.
 * Optional fields are nullable.
 */
public record SchedulingContextBundle(
        TaskDispatcher taskDispatcher,
        TaskScheduler taskScheduler,
        WorkerManager workerManager,
        CoordinatorTaskTracker taskTracker,
        SplitAssignmentManager splitAssignmentManager,
        SpeculativeStageHandler speculativeStageHandler,
        SpillBudget spillBudget,
        ResilienceRegistry resilienceRegistry) {

    /** Compact constructor — validates required scheduling dependencies. */
    public SchedulingContextBundle {
        checkNotNull(taskDispatcher, "taskDispatcher must not be null");
        checkNotNull(taskScheduler, "taskScheduler must not be null");
        checkNotNull(workerManager, "workerManager must not be null");
        checkNotNull(taskTracker, "taskTracker must not be null");
    }
}
