package org.pangrova.materialization.usecase;

import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.TriggerKind;

/**
 * Application callback that performs one scheduled refresh.
 *
 * <p>The scheduler owns claiming and settling the durable schedule; the
 * callback owns planning, authorization of the stored definer context, and the
 * data-plane execution protocol. It must be idempotent for a given command id.
 */
@FunctionalInterface
public interface ScheduledRefresh {

    /**
     * Refreshes one materialization.
     *
     * @param object object to refresh
     * @param commandId deterministic idempotency key for this schedule claim
     * @param trigger trigger kind
     */
    void refresh(MaterializationId object, CommandId commandId, TriggerKind trigger);
}
