package org.pangrova.materialization.port;

import java.time.Instant;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.TenantId;

/**
 * Durable receipt of an idempotent command.
 *
 * <p>Command ids are scoped by tenant: two tenants may independently use the
 * same client-supplied id without observing each other's results.
 *
 * @param tenant tenant that issued the command
 * @param commandId client-visible idempotency key
 * @param resultReference opaque reference to the recorded result
 * @param createdAt instant at which the result was recorded
 */
public record CommandRecord(
        TenantId tenant, CommandId commandId, String resultReference, Instant createdAt) {

    public CommandRecord {
        if (tenant == null || commandId == null) {
            throw new IllegalArgumentException("tenant and commandId must not be null");
        }
        if (resultReference == null || resultReference.isBlank()) {
            throw new IllegalArgumentException("resultReference must not be blank");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt must not be null");
        }
    }
}
