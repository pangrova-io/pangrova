package org.pangrova.materialization.port;

import java.time.Instant;

/**
 * Time source for the materialization kernel.
 *
 * <p>All domain and use-case timestamps come from this port so freshness,
 * fencing, and GC decisions are testable without sleeping.
 */
public interface Clock {

    /**
     * Returns the current instant.
     *
     * @return current instant
     */
    Instant now();
}
