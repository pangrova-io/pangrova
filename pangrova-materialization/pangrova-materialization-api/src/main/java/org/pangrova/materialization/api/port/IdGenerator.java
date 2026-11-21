package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

import java.util.UUID;

/**
 * Source of unique identifiers for attempts, generations, incarnations, and queries.
 */
@Experimental
public interface IdGenerator {

    /**
     * Returns a fresh universally unique identifier.
     *
     * @return new identifier
     */
    UUID newId();
}
