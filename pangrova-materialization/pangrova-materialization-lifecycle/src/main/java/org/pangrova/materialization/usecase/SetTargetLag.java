package org.pangrova.materialization.usecase;

import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Principal;
import org.pangrova.materialization.api.domain.TargetLag;

/**
 * Command that changes the freshness target of a materialization.
 *
 * <p>A manual object becomes scheduled; an active object receives an earlier
 * deadline when the lag shrinks; a suspended object stays suspended.
 *
 * @param object target object
 * @param requester principal issuing the command
 * @param targetLag new freshness target
 * @param commandId idempotency key
 */
public record SetTargetLag(
        MaterializationId object,
        Principal requester,
        TargetLag targetLag,
        CommandId commandId) {

    public SetTargetLag {
        if (object == null || requester == null || targetLag == null || commandId == null) {
            throw new IllegalArgumentException(
                    "object, requester, target lag, and command id are required");
        }
    }
}
