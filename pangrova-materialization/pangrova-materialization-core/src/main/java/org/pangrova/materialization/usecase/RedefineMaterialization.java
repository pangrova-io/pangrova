package org.pangrova.materialization.usecase;

import java.util.Map;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;

/**
 * Command that replaces the definition of a materialization.
 *
 * <p>Redefinition records a new immutable definition revision and advances the
 * object header; the currently published generation keeps serving reads until
 * the next refresh publishes the new definition.
 *
 * @param object target object
 * @param requester principal issuing the command
 * @param definitionSql new definition SQL
 * @param sessionProperties planning session properties bound to the revision
 * @param commandId idempotency key
 */
public record RedefineMaterialization(
        MaterializationId object,
        Principal requester,
        String definitionSql,
        Map<String, String> sessionProperties,
        CommandId commandId) {

    public RedefineMaterialization {
        if (object == null || requester == null || commandId == null) {
            throw new IllegalArgumentException("object, requester, and command id are required");
        }
        if (definitionSql == null || definitionSql.isBlank()) {
            throw new IllegalArgumentException("definitionSql must not be blank");
        }
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
    }
}
