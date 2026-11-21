package org.pangrova.coordinator.enums;

/**
     * Reasons for rejecting a query admission.
     */
public enum AdmissionRejectionReason {
    MEMORY_PRESSURE,
    CPU_PRESSURE,
    QUEUE_FULL
}
