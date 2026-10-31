package org.pangrova.worker.resource.memory;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/**
 * A point-in-time snapshot of one memory domain's usage.
 *
 * @param domain      the memory domain (never null)
 * @param usedBytes   bytes currently allocated/reserved in this domain (non-negative)
 * @param budgetBytes the configured budget for this domain (positive)
 */
@Experimental(reason = "G-RES-07: unified memory budget enforcement SPI")
public record MemoryDomainSnapshot(MemoryDomain domain, long usedBytes, long budgetBytes) {

    public MemoryDomainSnapshot {
        Objects.requireNonNull(domain, "domain must not be null");
        if (usedBytes < 0) {
            throw new IllegalArgumentException("usedBytes must be non-negative, got " + usedBytes);
        }
        if (budgetBytes <= 0) {
            throw new IllegalArgumentException("budgetBytes must be positive, got " + budgetBytes);
        }
    }

    /** Returns the utilisation ratio (0.0 to potentially > 1.0 if over-committed). */
    public double utilisation() {
        return (double) usedBytes / budgetBytes;
    }

    /** Returns true if usage exceeds the configured budget. */
    public boolean isOverCommitted() {
        return usedBytes > budgetBytes;
    }
}
