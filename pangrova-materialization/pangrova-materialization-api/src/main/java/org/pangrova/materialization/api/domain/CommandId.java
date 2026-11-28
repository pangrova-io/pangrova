package org.pangrova.materialization.api.domain;

import java.util.UUID;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Client-visible idempotency key of a materialization command.
 *
 * <p>When the client does not supply one, the coordinator generates it and
 * returns it in the command receipt. Replaying the same command id must return
 * the recorded result without repeating side effects.
 *
 * @param value command identifier
 */
public record CommandId(String value) {

    private static final int MAX_LENGTH = 128;

    public CommandId {
        value = requireText(value, "commandId");
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("commandId must not exceed " + MAX_LENGTH + " characters");
        }
    }

    /**
     * Creates a fresh random command identifier.
     *
     * @return new identifier
     */
    public static CommandId random() {
        return new CommandId(UUID.randomUUID().toString());
    }
}
