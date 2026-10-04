package org.pangrova.coordinator.ai.embedding;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.AuthorizedRelation;

/** Trusted late binding to the sole authenticated SQL compiler, never a client plan supplier. */
@FunctionalInterface
public interface GeneratedSourceCompiler {
    AuthorizedRelation compile(String sql, CoordinatorSessionContext actor);
}
