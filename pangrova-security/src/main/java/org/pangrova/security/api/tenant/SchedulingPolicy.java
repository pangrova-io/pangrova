package org.pangrova.security.api.tenant;

/**
 * Scheduling strategy for queries within a resource pool.
 */
public enum SchedulingPolicy {
    FIFO,
    FAIR,
    PRIORITY
}
