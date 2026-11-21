package org.pangrova.security.api.tenant;

import org.pangrova.security.api.enums.TenantMembershipOperation;
import org.pangrova.common.api.annotation.Experimental;

/** Exact tenant ID and principal, with no namespace splitting or implicit tenant transfer. */
@Experimental
public record TenantMembershipMutation(String tenantId, String principal, TenantMembershipOperation operation) {
    private static final int MAX_NAME_LENGTH = 255;

    public TenantMembershipMutation {
        requireName(tenantId);
        requireName(principal);
        java.util.Objects.requireNonNull(operation, "operation");
    }

    private static void requireName(String name) {
        if (name == null || name.isBlank() || name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Tenant ID and principal must contain 1 to 255 characters");
        }
    }

}
