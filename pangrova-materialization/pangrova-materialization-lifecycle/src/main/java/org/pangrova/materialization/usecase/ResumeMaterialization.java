package org.pangrova.materialization.usecase;

import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Principal;

/**
 * Command that resumes automatic refresh of a materialization.
 *
 * <p>Resumption requires a recorded target lag and is idempotent for an
 * already active schedule.
 *
 * @param object target object
 * @param requester principal issuing the command
 * @param commandId idempotency key
 */
public record ResumeMaterialization(
        MaterializationId object,
        Principal requester,
        CommandId commandId) {

    public ResumeMaterialization {
        if (object == null || requester == null || commandId == null) {
            throw new IllegalArgumentException("object, requester, and command id are required");
        }
    }
}
