package org.pangrova.materialization.api.rewrite;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Guard in the transparent-rewrite safety chain.
 *
 * <p>Every guard must allow the rewrite; the first skip wins. Guards are
 * ordered at registration so policy, privilege, freshness, and generation
 * checks can be evaluated deterministically.
 */
@Experimental
public interface RewriteGuard {

    /**
     * Returns a stable guard identifier used in explain output and events.
     *
     * @return guard identifier
     */
    String guardId();

    /**
     * Evaluates the guard.
     *
     * @param context observed rewrite facts
     * @return decision
     */
    RewriteDecision evaluate(RewriteContext context);
}
