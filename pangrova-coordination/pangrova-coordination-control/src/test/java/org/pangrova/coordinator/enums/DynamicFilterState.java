package org.pangrova.coordinator.enums;

/**
     * Lifecycle states for a dynamic filter.
     */
public enum DynamicFilterState {
    PENDING,
    COLLECTING,
    READY,
    APPLIED,
    COMPLETED,
    CANCELLED
}
