package org.pangrova.spi.security.tenant;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Role a user holds within a tenant.
 */
@Experimental
public enum TenantRole {
    OWNER,
    ADMIN,
    MEMBER
}
