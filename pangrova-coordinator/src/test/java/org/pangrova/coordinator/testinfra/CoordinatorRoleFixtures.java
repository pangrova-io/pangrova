package org.pangrova.coordinator.testinfra;

import org.pangrova.security.authz.GroupRoleMapping;
import org.pangrova.security.authz.InMemoryRoleStore;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.RoleStore;

import java.util.Map;

/** Creates only the explicit group-to-role bindings declared by each test fixture. */
public final class CoordinatorRoleFixtures {
    private CoordinatorRoleFixtures() {}

    /** Builds a role catalog without inferring any roles from an identity. */
    public static RoleStore mappedRoles(Map<String, String> bindings) {
        InMemoryRoleStore roles = new InMemoryRoleStore();
        bindings.forEach((group, role) -> {
            roles.createRole(Role.of(role));
            roles.mapGroupToRole(GroupRoleMapping.of(group, role));
        });
        return roles;
    }
}
