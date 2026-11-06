package org.pangrova.coordinator.feature;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.planner.core.analysis.AuthorizedRelation;

/** Trusted process-injected access to the sole authenticated SQL compilation pipeline. */
@FunctionalInterface
public interface FeatureSourceCompiler {
    /** Compiles read-only SQL with the current principal's authorization, masks and row filters. */
    AuthorizedRelation compile(String sql, QuerySession actor);
}
