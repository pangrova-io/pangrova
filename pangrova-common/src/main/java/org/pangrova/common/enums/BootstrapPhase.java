package org.pangrova.common.enums;

/**
 * Ordered phases of system bootstrap sequence.
 */
public enum BootstrapPhase {
    INFRASTRUCTURE,
    COORDINATORS,
    WORKERS,
    READINESS
}
