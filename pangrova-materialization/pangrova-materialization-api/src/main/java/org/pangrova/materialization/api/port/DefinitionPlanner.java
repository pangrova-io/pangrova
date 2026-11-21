package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Plans and binds materialization definitions.
 *
 * <p>Implementations live in the coordinator and delegate to the production
 * planner. A definition that cannot be bound safely must be rejected here,
 * before any object or attempt is persisted.
 */
@Experimental
public interface DefinitionPlanner {

    /**
     * Plans a definition revision.
     *
     * @param request planning request
     * @return bound definition
     */
    PlannedDefinition plan(PlanningRequest request);
}
