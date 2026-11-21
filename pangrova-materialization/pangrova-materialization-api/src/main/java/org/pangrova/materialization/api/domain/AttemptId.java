package org.pangrova.materialization.api.domain;

import java.util.UUID;

/**
 * Identity of one refresh attempt.
 *
 * @param value attempt identifier
 */
public record AttemptId(UUID value) {

    public AttemptId {
        if (value == null) {
            throw new IllegalArgumentException("attempt id must not be null");
        }
    }

    /**
     * Creates a fresh random attempt identifier.
     *
     * @return new identifier
     */
    public static AttemptId random() {
        return new AttemptId(UUID.randomUUID());
    }
}
