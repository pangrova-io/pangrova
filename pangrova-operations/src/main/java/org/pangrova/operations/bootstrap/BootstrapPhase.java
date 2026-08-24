package org.pangrova.operations.bootstrap;

/**
 * Ordered phases of system bootstrap sequence.
 */
public enum BootstrapPhase {
    INFRASTRUCTURE,
    COORDINATORS,
    WORKERS,
    READINESS
}
