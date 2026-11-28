package org.pangrova.worker;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import org.pangrova.worker.task.TaskManager;

/**
 * Groups Worker runtime dependencies into a single parameter object.
 * Keeps PangrovaWorker constructor within the 4-param governance limit.
 */
public record WorkerComponents(
        TaskManager taskManager) {

    public WorkerComponents {
        checkNotNull(taskManager, "taskManager must not be null");
    }
}
