package org.pangrova.coordinator.feature;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.AuthorizedRelation;

/** Trusted process-injected access to the sole authenticated SQL compilation pipeline. */
@FunctionalInterface
public interface FeatureSourceCompiler {
    /** Compiles read-only SQL with the current principal's authorization, masks and row filters. */
    AuthorizedRelation compile(String sql, CoordinatorSessionContext actor);
}
