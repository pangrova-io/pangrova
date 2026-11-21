package org.pangrova.materialization.usecase;

import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Principal;

/**
 * Command that renames a logical materialization without touching its physical
 * generations.
 *
 * <p>Rename is rejected while live dependents exist: dependent definitions are
 * stored SQL text, so rewriting their references is a definition change, not a
 * metadata rename. Drop or redefine dependents first.
 *
 * @param object current identity
 * @param requester principal issuing the command
 * @param newObject new identity
 * @param commandId idempotency key
 */
public record RenameMaterialization(
        MaterializationId object,
        Principal requester,
        MaterializationId newObject,
        CommandId commandId) {

    public RenameMaterialization {
        if (object == null || requester == null || newObject == null || commandId == null) {
            throw new IllegalArgumentException(
                    "object, requester, new identity, and command id are required");
        }
        if (object.equals(newObject)) {
            throw new IllegalArgumentException("new identity must differ from the current identity");
        }
        if (!object.tenant().equals(newObject.tenant())) {
            throw new IllegalArgumentException("rename cannot move an object across tenants");
        }
        if (newObject.name().parts().size() != 3) {
            throw new IllegalArgumentException(
                    "materialization names must be fully qualified catalog.schema.table");
        }
    }
}
