package org.pangrova.materialization.api.domain;

import java.util.UUID;

/**
 * Identity of one logical incarnation of a materialization object.
 *
 * <p>A new incarnation starts when an object is dropped and recreated, or when
 * its identity is deliberately reset. Generations and attempts never cross
 * incarnation boundaries.
 *
 * @param value incarnation identifier
 */
public record IncarnationId(UUID value) {

    public IncarnationId {
        if (value == null) {
            throw new IllegalArgumentException("incarnation id must not be null");
        }
    }

    /**
     * Creates a fresh random incarnation identifier.
     *
     * @return new identifier
     */
    public static IncarnationId random() {
        return new IncarnationId(UUID.randomUUID());
    }
}
