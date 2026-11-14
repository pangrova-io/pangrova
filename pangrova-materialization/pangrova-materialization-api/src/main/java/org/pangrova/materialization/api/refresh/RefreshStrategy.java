package org.pangrova.materialization.api.refresh;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.domain.StrategyId;
import org.pangrova.materialization.api.domain.RefreshPlan;
import org.pangrova.connector.api.materialization.ChangeProof;

/**
 * Pluggable strategy that decides whether, and how, a refresh can execute.
 *
 * <p>Strategies never fall back to a weaker mode. If a strategy cannot prove
 * that it applies, it returns an incompatible decision and resolution fails
 * with {@code INCREMENTAL_UNSUPPORTED}; the caller must explicitly request a
 * full refresh instead.
 */
@Experimental
public interface RefreshStrategy {

    /**
     * Returns the strategy identifier.
     *
     * @return strategy identifier
     */
    StrategyId id();

    /**
     * Returns the refresh kind this strategy executes.
     *
     * @return refresh kind
     */
    RefreshKind kind();

    /**
     * Validates that the strategy applies to the definition and observed sources.
     *
     * @param context refresh context
     * @return compatibility decision
     */
    StrategyCompatibility validate(RefreshContext context);

    /**
     * Builds an executable plan.
     *
     * @param context validated refresh context
     * @return executable plan
     */
    RefreshPlan plan(RefreshContext context);

    /**
     * Verifies the change proof that accompanies an incremental plan.
     *
     * @param plan planned refresh
     * @param proof connector-provided proof
     */
    void verify(RefreshPlan plan, ChangeProof proof);
}
