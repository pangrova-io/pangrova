package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.stage.Stage;

/**
 * Callback for registering dispatched tasks in the task-to-stage registry.
 * Used by {@link LeafStageDispatcher} to notify {@link DAGDispatchEngine}
 * of new task-to-stage mappings during result collection.
 */
@FunctionalInterface
interface TaskStageRegistrar {

    /** Registers a task belonging to the given stage for the specified query. */
    void register(String queryId, String taskId, Stage stage);
}
