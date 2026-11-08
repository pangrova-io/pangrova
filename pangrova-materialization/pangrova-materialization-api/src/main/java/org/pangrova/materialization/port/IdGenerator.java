package org.pangrova.materialization.port;

import java.util.UUID;

/**
 * Source of unique identifiers for attempts, generations, incarnations, and queries.
 */
public interface IdGenerator {

    /**
     * Returns a fresh universally unique identifier.
     *
     * @return new identifier
     */
    UUID newId();
}
