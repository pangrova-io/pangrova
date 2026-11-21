package org.pangrova.worker.enums;

/** Reason control access failed its Worker epoch check. */
public enum ControlEpochFenceReason {
    CONTROL_PLANE_NOT_READY,
    MISSING_EPOCH,
    INVALID_EPOCH,
    EPOCH_AHEAD,
    EPOCH_MISMATCH
}
