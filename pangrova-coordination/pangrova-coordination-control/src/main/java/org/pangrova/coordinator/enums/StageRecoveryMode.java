package org.pangrova.coordinator.enums;

/** How a lost stage output will be recovered. */
public enum StageRecoveryMode {
    RECOMPUTE,
    MEMORY_AND_REMOTE
}
