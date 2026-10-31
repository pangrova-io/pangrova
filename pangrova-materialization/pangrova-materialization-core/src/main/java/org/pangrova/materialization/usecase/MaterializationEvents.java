package org.pangrova.materialization.usecase;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationEventType;

/**
 * Builds durable materialization events.
 *
 * <p>Every event carries a fresh identity so repository replay can deduplicate
 * by event id without relying on timestamps.
 */
final class MaterializationEvents {

    private MaterializationEvents() {
    }

    static MaterializationEvent of(MaterializationEventType type, MaterializationId object,
            Principal requesting, Principal effective, CommandId command,
            Optional<AttemptId> attempt, Optional<GenerationId> generation,
            String detail, Instant occurredAt) {
        return of(type, object, requesting, effective, Optional.of(command), attempt, generation,
                detail, occurredAt);
    }

    static MaterializationEvent of(MaterializationEventType type, MaterializationId object,
            Principal requesting, Principal effective, Optional<CommandId> command,
            Optional<AttemptId> attempt, Optional<GenerationId> generation,
            String detail, Instant occurredAt) {
        return new MaterializationEvent(UUID.randomUUID().toString(), type, object, command,
                attempt, generation, Optional.empty(), requesting, effective, detail, occurredAt);
    }
}
