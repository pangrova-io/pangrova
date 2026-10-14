package org.pangrova.testing.e2e;

import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;

import javax.sql.DataSource;
import java.util.UUID;

/** Grants only the named catalog's lifecycle privileges for a SQL E2E scenario. */
public final class CatalogDdlGrantFixture {

    private CatalogDdlGrantFixture() {}

    public static void withCatalogLifecycleGrants(DataSource dataSource, String principal,
                                                   String catalog, Runnable scenario) {
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        String role = "e2e_catalog_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        try {
            roles.grantRoleToUser(UserRoleAssignment.of(principal, role));
            var grants = new PersistentGrantStore(dataSource);
            grants.addGrant(PrivilegeGrant.of(
                    role, Privilege.CREATE_CATALOG, "CATALOG", catalog, null, null, null));
            grants.addGrant(PrivilegeGrant.of(
                    role, Privilege.DROP_CATALOG, "CATALOG", catalog, null, null, null));
            scenario.run();
        } finally {
            roles.dropRole(role);
        }
    }
}
