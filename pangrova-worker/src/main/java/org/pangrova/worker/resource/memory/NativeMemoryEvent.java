package org.pangrova.worker.resource.memory;

/**
 * Event reported from Native execution domain to Java control plane.
 *
 * <p>Native periodically reports memory statistics per tenant. Additionally,
 * spill/shrink/oom events are reported as they occur. The Java-side
 * {@link CrossDomainMemoryCoordinator} consumes these events to maintain
 * a combined view of tenant memory usage across both domains.</p>
 *
 * @param tenantId        tenant identifier
 * @param type            event type (STAT, SPILL, SHRINK, OOM)
 * @param allocatedBytes  current native allocation for this tenant (bytes)
 * @param timestampMillis epoch millis when the event was generated on native side
 */
public record NativeMemoryEvent(
        String tenantId,
        Type type,
        long allocatedBytes,
        long timestampMillis
) {

    public enum Type {
        /** Periodic heartbeat stat report. */
        STAT,
        /** Native triggered a spill for this tenant. */
        SPILL,
        /** Native shrunk allocation for this tenant. */
        SHRINK,
        /** Native encountered OOM for this tenant. */
        OOM
    }

    public NativeMemoryEvent {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be null or blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (allocatedBytes < 0) {
            throw new IllegalArgumentException("allocatedBytes must be non-negative, got " + allocatedBytes);
        }
        if (timestampMillis <= 0) {
            throw new IllegalArgumentException("timestampMillis must be positive, got " + timestampMillis);
        }
    }
}
