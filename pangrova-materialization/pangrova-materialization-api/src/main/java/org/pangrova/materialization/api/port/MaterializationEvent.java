package org.pangrova.materialization.api.port;

import org.pangrova.materialization.api.enums.MaterializationEventType;

import java.time.Instant;
import java.util.Optional;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.Principal;
import org.pangrova.execution.api.identity.QueryId;

/**
 * Structured audit and outbox event.
 *
 * <p>Events are written in the same transaction as the state change they
 * describe and delivered asynchronously. They must never contain row data,
 * credentials, or sensitive SQL literals.
 *
 * @param eventId unique event identifier
 * @param eventType event type
 * @param object affected object
 * @param command originating command, when applicable
 * @param attempt affected attempt, when applicable
 * @param generation affected generation, when applicable
 * @param query affecting query, when applicable
 * @param requesting principal that issued the command
 * @param effective principal the operation ran as
 * @param detail structured, non-sensitive detail
 * @param occurredAt event instant
 */
public record MaterializationEvent(
        String eventId,
        MaterializationEventType eventType,
        MaterializationId object,
        Optional<CommandId> command,
        Optional<AttemptId> attempt,
        Optional<GenerationId> generation,
        Optional<QueryId> query,
        Principal requesting,
        Principal effective,
        String detail,
        Instant occurredAt) {

    public MaterializationEvent {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (eventType == null || object == null || requesting == null || effective == null) {
            throw new IllegalArgumentException("eventType, object, and principals must not be null");
        }
        if (command == null || attempt == null || generation == null || query == null) {
            throw new IllegalArgumentException("optional event references must not be null");
        }
        query.ifPresent(id -> {
            if (id.id().isBlank() || id.id().length() > 128) {
                throw new IllegalArgumentException("materialization event query id must contain 1 to 128 characters");
            }
        });
        detail = detail == null ? "" : detail;
        if (occurredAt == null) {
            throw new IllegalArgumentException("occurredAt must not be null");
        }
    }
}
