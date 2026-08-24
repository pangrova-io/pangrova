package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

/**
 * Transaction isolation levels supported by connectors.
 */
@Experimental
public enum IsolationLevel {

    /** Allows dirty reads, non-repeatable reads, and phantom reads. */
    READ_UNCOMMITTED,

    /** Prevents dirty reads; allows non-repeatable reads and phantom reads. */
    READ_COMMITTED,

    /** Prevents dirty and non-repeatable reads; allows phantom reads. */
    REPEATABLE_READ,

    /** Full isolation — prevents dirty reads, non-repeatable reads, and phantom reads. */
    SERIALIZABLE
}
