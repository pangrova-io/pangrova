package org.pangrova.security.testinfra;

import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.testing.testkit.security.policy.InMemoryRoleStore;
import org.pangrova.security.api.policy.Role;
import org.pangrova.persistence.security.api.policy.RoleStore;

import java.util.Map;

/** Creates only the explicit group-to-role bindings declared by each test fixture. */
public final class SecurityRoleFixtures {
    private SecurityRoleFixtures() {}

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
