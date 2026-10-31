package org.pangrova.common.lifecycle;

/**
 * Ordered phases of system bootstrap sequence.
 */
public enum BootstrapPhase {
    INFRASTRUCTURE,
    COORDINATORS,
    WORKERS,
    READINESS
}
