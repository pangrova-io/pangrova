package org.pangrova.materialization.usecase;

import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Principal;

/**
 * Command that suspends automatic refresh of a materialization.
 *
 * <p>Suspension is idempotent: suspending an already suspended schedule
 * succeeds without changing the record.
 *
 * @param object target object
 * @param requester principal issuing the command
 * @param commandId idempotency key
 */
public record SuspendMaterialization(
        MaterializationId object,
        Principal requester,
        CommandId commandId) {

    public SuspendMaterialization {
        if (object == null || requester == null || commandId == null) {
            throw new IllegalArgumentException("object, requester, and command id are required");
        }
    }
}
