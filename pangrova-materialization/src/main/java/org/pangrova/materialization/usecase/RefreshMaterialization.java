package org.pangrova.materialization.usecase;

import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.TriggerKind;

/**
 * Command that recomputes an object into an isolated candidate and publishes it.
 *
 * <p>Planning and execution session properties are read from the persisted
 * definition revision: a refresh may never rebind the definition.
 *
 * @param object target object
 * @param requester principal issuing the refresh
 * @param trigger attempt origin
 * @param commandId idempotency key
 */
public record RefreshMaterialization(
        MaterializationId object,
        Principal requester,
        TriggerKind trigger,
        CommandId commandId) {

    public RefreshMaterialization {
        if (object == null || requester == null || trigger == null || commandId == null) {
            throw new IllegalArgumentException("object, requester, trigger, and command id are required");
        }
    }
}
