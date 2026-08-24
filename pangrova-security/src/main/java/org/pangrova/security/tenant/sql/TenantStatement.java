package org.pangrova.security.tenant.sql;

import org.pangrova.security.tenant.model.TenantRole;

import java.util.Map;
import java.util.Objects;

/**
 * AST node types for tenant management SQL statements.
 */
public sealed interface TenantStatement {

    record CreateTenant(String tenantId, String name, Map<String, String> properties)
            implements TenantStatement {
        public CreateTenant {
            properties = Map.copyOf(
                    Objects.requireNonNull(properties, "properties must not be null"));
        }
    }

    record AlterTenant(String tenantId, Map<String, String> properties)
            implements TenantStatement {
        public AlterTenant {
            properties = Map.copyOf(
                    Objects.requireNonNull(properties, "properties must not be null"));
        }
    }

    record DropTenant(String tenantId)
            implements TenantStatement {}

    record GrantTenant(String tenantId, String userPrincipal, TenantRole role)
            implements TenantStatement {}

    record RevokeTenant(String tenantId, String userPrincipal)
            implements TenantStatement {}
}
