package org.pangrova.security.api.tenant;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Lifecycle state of a tenant.
 */
@Experimental
public enum TenantStatus {
    ACTIVE,
    SUSPENDED,
    DELETED
}
