package org.pangrova.materialization.api.domain;

import java.util.UUID;

/**
 * Identity of one physical generation of a materialization.
 *
 * @param value generation identifier
 */
public record GenerationId(UUID value) {

    public GenerationId {
        if (value == null) {
            throw new IllegalArgumentException("generation id must not be null");
        }
    }

    /**
     * Creates a fresh random generation identifier.
     *
     * @return new identifier
     */
    public static GenerationId random() {
        return new GenerationId(UUID.randomUUID());
    }
}
