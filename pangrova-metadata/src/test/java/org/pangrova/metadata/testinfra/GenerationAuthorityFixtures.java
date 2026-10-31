package org.pangrova.metadata.testinfra;

import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;
import java.sql.Connection;
import java.util.Set;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.store.jdbc.PersistentGrantStore;
import org.pangrova.security.store.jdbc.PersistentRoleStore;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantRole;
import org.pangrova.security.api.tenant.TenantStatus;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Explicit PostgreSQL authority for this leaf's durable artifact fixtures. */
public final class GenerationAuthorityFixtures {
    private GenerationAuthorityFixtures() {}

    /** No model provider is loaded in Metadata owner tests; unexpected model scope is an error. */
    public static CurrentModelAuthority modelAuthority() {
        return new CurrentModelAuthority() {
            @Override
            public List<DeferredAuthorizationScope.ModelSnapshot> observe(
                    Connection connection, String principal, Set<String> models) {
                throw new AssertionError("Metadata fixtures must not observe model authority");
            }

            @Override
            public void requireCurrent(Connection connection, String principal,
                    List<DeferredAuthorizationScope.ModelSnapshot> models) {
                if (!models.isEmpty()) {
                    throw new AssertionError("Metadata fixture received model authority owned by AI: " + models);
                }
            }
        };
    }

    /** Uses real Metadata tenant/table authority with an explicit model-free fixture boundary. */
    public static GenerationStores artifacts() {
        return new GenerationStores(modelAuthority());
    }

    /** Provisions the real subject, active tenant and exact permissions used by a fixture. */
    public static void provision(DataSource dataSource, String tenantId, String principal,
                                 DeferredAuthorizationScope scope) {
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("artifact_builder"));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, "artifact_builder"));
        var tenants = new JdbcTenantStore(dataSource);
        tenants.initSchema();
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
