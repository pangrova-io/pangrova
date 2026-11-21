package org.pangrova.coordinator.enums;

/** Phase determining whether the breaker admits a call. */
public enum CircuitBreakerPhase {
    CLOSED,
    OPEN,
    HALF_OPEN
}
