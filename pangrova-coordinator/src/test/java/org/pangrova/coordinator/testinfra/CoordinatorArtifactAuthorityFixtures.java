package org.pangrova.coordinator.testinfra;

import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Explicit PostgreSQL authority for this leaf's durable artifact fixtures. */
public final class CoordinatorArtifactAuthorityFixtures {
    private CoordinatorArtifactAuthorityFixtures() {}

    /** Provisions the real subject, active tenant and exact permissions used by a fixture. */
    public static void provision(DataSource dataSource, String tenantId, String principal,
                                 DeferredAuthorizationScope scope) {
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("artifact_builder"));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, "artifact_builder"));
        var tenants = new JdbcTenantStore(dataSource);
        tenants.createTenant(new Tenant(tenantId, tenantId, TenantStatus.ACTIVE,
                null, List.of(), List.of(), null, Map.of(), Instant.EPOCH, principal));
        tenants.addUserToTenant(tenantId, principal, TenantRole.MEMBER);
        var grants = new PersistentGrantStore(dataSource);
        for (var permission : scope.permissions()) {
            var table = permission.table();
            grants.addGrant(PrivilegeGrant.of("artifact_builder", permission.privilege(), "TABLE",
                    table.catalog(), table.schema(), table.table(), null));
        }
    }
}
