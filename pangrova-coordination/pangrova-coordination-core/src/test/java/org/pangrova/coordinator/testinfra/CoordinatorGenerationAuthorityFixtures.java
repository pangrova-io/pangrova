package org.pangrova.coordinator.testinfra;

import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;
import org.pangrova.spi.security.tenant.TenantStatus;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Explicit PostgreSQL authority for this leaf's durable artifact fixtures. */
public final class CoordinatorGenerationAuthorityFixtures {
    private CoordinatorGenerationAuthorityFixtures() {}

    /** Provisions the real subject, active tenant and exact permissions used by a fixture. */
    public static void provision(DataSource dataSource, String tenantId, String principal,
                                 DeferredAuthorizationScope scope) {
        var roles = new JdbcRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("artifact_builder"));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, "artifact_builder"));
        var tenants = new JdbcTenantStore(dataSource);
        tenants.initSchema();
        tenants.createTenant(new Tenant(tenantId, tenantId, TenantStatus.ACTIVE,
                null, List.of(), List.of(), null, Map.of(), Instant.EPOCH, principal));
        tenants.addUserToTenant(tenantId, principal, TenantRole.MEMBER);
        var grants = new JdbcGrantStore(dataSource);
        for (var permission : scope.permissions()) {
            var table = permission.table();
            grants.addGrant(PrivilegeGrant.of("artifact_builder", permission.privilege(), "TABLE",
                    table.catalog(), table.schema(), table.table(), null));
        }
    }
}
