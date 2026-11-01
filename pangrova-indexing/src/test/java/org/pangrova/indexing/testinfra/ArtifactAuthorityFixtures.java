package org.pangrova.indexing.testinfra;

import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Explicit PostgreSQL authority for this leaf's durable artifact fixtures. */
public final class ArtifactAuthorityFixtures {
    private ArtifactAuthorityFixtures() {}

    /** Provisions the real subject, active tenant and exact permissions used by a fixture. */
    public static void provision(DataSource dataSource, String tenantId, String principal,
                                 DeferredAuthorizationScope scope) {
        var roles = new PersistentRoleStore(dataSource);
        if (securitySchemaPresent(dataSource)) {
            // Migrated sources already carry the bundled security schema; re-running
            // SecuritySchema.initialize on them trips the trigger-managed authorization
            // revision guard. The authority gate, however, is a runtime construct that the
            // per-owner baseline chain intentionally does not ship, so install it idempotently:
            // direct-SQL authority mutations must stay linearly ordered with publications.
            installAuthorityGate(dataSource);
        } else {
            roles.initSchema();
        }
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

    private static boolean securitySchemaPresent(DataSource dataSource) {
        try (var connection = dataSource.getConnection();
                var tables = connection.getMetaData()
                        .getTables(null, null, "pangrova_authorization_revision", null)) {
            return tables.next();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to probe security schema presence", e);
        }
    }

    /** Installs only the runtime authority gate (all CREATE OR REPLACE, never touches the
     *  trigger-managed authorization revision table) on an already-migrated schema. */
    private static void installAuthorityGate(DataSource dataSource) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            AuthorizationTransactionGate.initialize(connection);
            connection.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to install authority gate", e);
        }
    }
}
