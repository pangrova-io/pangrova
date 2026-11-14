package org.pangrova.security.api.grant;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.enums.Privilege;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** One atomic privilege statement. Role names are case-sensitive; order and duplicates have no semantic effect. */
@Experimental
public record PrivilegeMutation(UUID requestId, Operation operation, Set<Privilege> privileges,
        PrivilegeScope scope, Set<String> roles, boolean withGrantOption, RevokeBehavior revokeBehavior) {
    public PrivilegeMutation {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(revokeBehavior, "revokeBehavior");
        privileges = Set.copyOf(privileges);
        roles = Set.copyOf(roles);
        if (privileges.isEmpty() || roles.isEmpty() || (long) privileges.size() * roles.size() > 256) {
            throw new IllegalArgumentException("Expected 1 to 256 privilege/role pairs");
        }
        roles.forEach(PrivilegeScope::requireName);
        for (var privilege : privileges) validateScope(privilege, scope.objectType());
        if (privileges.contains(Privilege.OWNER)) {
            throw new IllegalArgumentException("OWNER requires the ownership transfer operation");
        }
        if (operation != Operation.GRANT && withGrantOption) {
            throw new IllegalArgumentException("WITH GRANT OPTION applies only to GRANT");
        }
        if (operation == Operation.GRANT && revokeBehavior != RevokeBehavior.RESTRICT) {
            throw new IllegalArgumentException("CASCADE applies only to REVOKE");
        }
    }

    private static void validateScope(Privilege privilege, PrivilegeScope.ObjectType type) {
        if (privilege == Privilege.ALL) return;
        boolean valid = switch (type) {
            case TABLE -> Set.of(Privilege.SELECT, Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE,
                    Privilege.TRUNCATE, Privilege.DROP, Privilege.ALTER, Privilege.RENAME_TABLE,
                    Privilege.SHOW_COLUMNS).contains(privilege);
            case VOLUME -> Set.of(Privilege.READ_VOLUME, Privilege.WRITE_VOLUME,
                    Privilege.MANAGE_VOLUME).contains(privilege);
            case STAGE -> Set.of(Privilege.USAGE_STAGE, Privilege.READ_STAGE, Privilege.WRITE_STAGE)
                    .contains(privilege);
            case STORAGE_PROFILE -> Set.of(Privilege.MANAGE_STORAGE_PROFILE, Privilege.VIEW_STORAGE_PROFILE,
                    Privilege.READ_STORAGE_PROFILE).contains(privilege);
            case CATALOG, SCHEMA -> switch (privilege) {
                case OWNER, MANAGE_STORAGE_PROFILE, VIEW_STORAGE_PROFILE, READ_STORAGE_PROFILE,
                        EXECUTE_QUERY, KILL_QUERY, VIEW_QUERY, SET_SESSION -> false;
                case CREATE_CATALOG, DROP_CATALOG, ALTER_CATALOG -> type == PrivilegeScope.ObjectType.CATALOG;
                default -> true;
            };
        };
        if (!valid) throw new IllegalArgumentException(privilege + " is not grantable on " + type);
    }

    @Experimental
    public enum Operation { GRANT, REVOKE, REVOKE_GRANT_OPTION }

    @Experimental
    public enum RevokeBehavior { RESTRICT, CASCADE }
}
