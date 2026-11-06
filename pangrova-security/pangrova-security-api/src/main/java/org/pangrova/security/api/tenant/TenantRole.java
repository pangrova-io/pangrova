package org.pangrova.security.api.tenant;

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
