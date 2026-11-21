package org.pangrova.coordinator.ai.embedding;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.planner.analysis.AuthorizedRelation;

/** Trusted late binding to the sole authenticated SQL compiler, never a client plan supplier. */
@FunctionalInterface
public interface GeneratedSourceCompiler {
    AuthorizedRelation compile(String sql, QuerySession actor);
}
