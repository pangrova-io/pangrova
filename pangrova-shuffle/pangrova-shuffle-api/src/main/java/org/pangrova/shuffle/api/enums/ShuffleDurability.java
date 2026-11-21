package org.pangrova.shuffle.api.enums;

/**
 * Storage-medium description only. Persistence of bytes does not establish selected-attempt
 * visibility, consumer retry, reachability after process loss or safe scale-down.
 * Those guarantees must be declared independently and validated against the current exchange.
 */
public enum ShuffleDurability {
    /** Process-local memory. */
    EPHEMERAL,
    /** Local filesystem storage; access and lifetime depend on its owner and deployment. */
    LOCAL_SPILL,
    /** External storage; replication, retention and availability still require explicit guarantees. */
    DURABLE
}
