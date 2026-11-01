package org.pangrova.indexing.testinfra;

import org.pangrova.indexing.control.DeferredTableControlAuthority;
import org.pangrova.indexing.store.jdbc.ArtifactCurrentAuthority;
import org.pangrova.security.authz.DeferredAuthorizationGuard;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.security.tenant.store.JdbcTenantStore;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Test doubles for indexing's authority ports. Indexing tests never read metadata-owner tables;
 * the real {@code TableControlDeferredPolicy} composition is verified by the cross-owner tests
 * that stay in pangrova-metadata.
 */
public final class ArtifactAuthorityTestDoubles {
    private ArtifactAuthorityTestDoubles() {}

    /** Permissive deferred authorization guard: accepts every scope without touching
     *  security or metadata rows. Tests asserting real grant/denial semantics stay in
     *  pangrova-metadata. */
    public static DeferredAuthorizationGuard permissiveGuard() {
        return (connection, principal, scope) -> { };
    }

    /** Permissive deferred table-control authority (indexing port with the same shape). */
    public static DeferredTableControlAuthority permissiveTableControl() {
        return (connection, principal, scope) -> { };
    }

    /** Wraps {@link #permissiveTableControl()} for stores taking an {@link ArtifactCurrentAuthority}. */
    public static ArtifactCurrentAuthority permissive() {
        return new ArtifactCurrentAuthority(permissiveTableControl());
    }

    /**
     * Provisions the real subject, active tenant and direct user-role assignment rows that the
     * hardcoded authority checks inside {@link ArtifactCurrentAuthority} and
     * {@code CurrentDeferredExecutionContext} read. The table-control and grant gates stay
     * permissive, so no privilege grants are needed here; real grant/denial semantics
     * are verified by the cross-owner tests that stay in pangrova-metadata.
     */
    public static void provisionTenant(DataSource dataSource, String tenantId, String principal) {
        var roles = new PersistentRoleStore(dataSource);
        roles.createRole(Role.of("artifact_builder"));
        roles.grantRoleToUser(UserRoleAssignment.of(principal, "artifact_builder"));
        var tenants = new JdbcTenantStore(dataSource);
        tenants.createTenant(new Tenant(tenantId, tenantId, TenantStatus.ACTIVE,
                null, List.of(), List.of(), null, Map.of(), Instant.EPOCH, principal));
        tenants.addUserToTenant(tenantId, principal, TenantRole.MEMBER);
    }
}
