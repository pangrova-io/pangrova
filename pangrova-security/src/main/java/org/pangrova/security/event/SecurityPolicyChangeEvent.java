package org.pangrova.security.event;

/**
 * Sealed hierarchy of security policy change events for cache invalidation and propagation.
 */
public sealed interface SecurityPolicyChangeEvent {

    String catalog();

    String schema();

    String table();

    record RlsPolicyChanged(
            String catalog,
            String schema,
            String table,
            String policyName
    ) implements SecurityPolicyChangeEvent {}

    record ColumnMaskChanged(
            String catalog,
            String schema,
            String table,
            String columnName
    ) implements SecurityPolicyChangeEvent {}

    record RoleAssignmentChanged(
            String principal,
            String roleName
    ) implements SecurityPolicyChangeEvent {

        @Override
        public String catalog() {
            return "";
        }

        @Override
        public String schema() {
            return "";
        }

        @Override
        public String table() {
            return "";
        }
    }

    record GrantChanged(
            String grantedBy,
            String roleName,
            String privilege
    ) implements SecurityPolicyChangeEvent {

        @Override
        public String catalog() {
            return "";
        }

        @Override
        public String schema() {
            return "";
        }

        @Override
        public String table() {
            return "";
        }
    }
}
