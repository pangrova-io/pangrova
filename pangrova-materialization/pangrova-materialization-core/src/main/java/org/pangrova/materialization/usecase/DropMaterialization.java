package org.pangrova.materialization.usecase;

import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;

/**
 * Command that removes a logical materialization while retaining its physical
 * generations until pinned readers and GC allow collection.
 *
 * <p>Without cascade the command fails when a live materialization still
 * consumes the target. With cascade every transitive dependent is dropped
 * first, deepest dependent first, so no live definition ever references a
 * dropped object.
 *
 * @param object target object
 * @param requester principal issuing the drop
 * @param commandId idempotency key
 * @param cascade whether to drop transitive dependents
 * @param ifExists whether a missing target is a successful no-op
 */
public record DropMaterialization(
        MaterializationId object,
        Principal requester,
        CommandId commandId,
        boolean cascade,
        boolean ifExists) {

    public DropMaterialization {
        if (object == null || requester == null || commandId == null) {
            throw new IllegalArgumentException("object, requester, and command id are required");
        }
    }

    /** Creates a drop that fails when live dependents exist. */
    public DropMaterialization(MaterializationId object, Principal requester, CommandId commandId) {
        this(object, requester, commandId, false, false);
    }

    /** Creates a drop with explicit dependency handling and existence policy. */
    public DropMaterialization(MaterializationId object, Principal requester, CommandId commandId,
            boolean cascade) {
        this(object, requester, commandId, cascade, false);
    }
}
