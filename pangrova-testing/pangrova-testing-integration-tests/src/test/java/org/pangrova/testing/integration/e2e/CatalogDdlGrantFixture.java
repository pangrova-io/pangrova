package org.pangrova.testing.integration.e2e;

import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;

import javax.sql.DataSource;
import java.util.UUID;

/** Grants only the named catalog's lifecycle privileges for a SQL E2E scenario. */
public final class CatalogDdlGrantFixture {

    private CatalogDdlGrantFixture() {}

    public static void withCatalogLifecycleGrants(DataSource dataSource, String principal,
                                                   String catalog, Runnable scenario) {
        withGrants(dataSource, principal, catalog, scenario, Privilege.CREATE_CATALOG, Privilege.DROP_CATALOG);
    }

    public static void withCatalogRevisionGrants(DataSource dataSource, String principal,
                                                 String catalog, Runnable scenario) {
        withGrants(dataSource, principal, catalog, scenario, Privilege.CREATE_CATALOG,
                Privilege.DROP_CATALOG, Privilege.ALTER_CATALOG, Privilege.SELECT);
    }

    private static void withGrants(DataSource dataSource, String principal, String catalog,
                                   Runnable scenario, Privilege... privileges) {
        var roles = new JdbcRoleStore(dataSource);
        String role = "e2e_catalog_" + UUID.randomUUID();
        roles.createRole(Role.of(role));
        try {
            roles.grantRoleToUser(UserRoleAssignment.of(principal, role));
            var grants = new JdbcGrantStore(dataSource);
            for (Privilege privilege : privileges) {
                grants.addGrant(PrivilegeGrant.of(role, privilege, "CATALOG", catalog, null, null, null));
            }
            scenario.run();
        } finally {
            roles.dropRole(role);
        }
    }
}
