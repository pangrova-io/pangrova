package org.pangrova.security.tenant.model;

/**
 * Scheduling strategy for queries within a resource pool.
 */
public enum SchedulingPolicy {
    FIFO,
    FAIR,
    PRIORITY
}
