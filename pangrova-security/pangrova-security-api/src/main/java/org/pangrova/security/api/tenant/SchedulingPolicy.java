package org.pangrova.security.api.tenant;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Scheduling strategy for queries within a resource pool.
 */
@Experimental
public enum SchedulingPolicy {
    FIFO,
    FAIR,
    PRIORITY
}
