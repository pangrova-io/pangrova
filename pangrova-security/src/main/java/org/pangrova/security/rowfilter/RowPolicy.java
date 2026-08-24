package org.pangrova.security.rowfilter;

import org.pangrova.spi.security.UserIdentity;

import java.util.List;

/**
 * Row-level security policy that filters rows based on user identity and role membership.
 */
public record RowPolicy(
        long policyId,
        String policyName,
        String catalogName,
        String schemaName,
        String tableName,
        String filterExpression,
        List<String> applyToRoles,
        boolean enabled,
        int priority
) {

    /**
     * Defensive copies of mutable lists to ensure immutability.
     * Rejects filter expressions containing DDL keywords.
     */
    public RowPolicy {
        applyToRoles = applyToRoles != null ? List.copyOf(applyToRoles) : List.of();
        org.pangrova.security.authz.SqlIdentifierValidator.rejectDdlInExpression(filterExpression);
    }

    /**
     * Returns true if this policy applies to the given user.
     * An empty applyToRoles list means the policy applies to everyone.
     */
    public boolean appliesTo(UserIdentity user) {
        if (applyToRoles.isEmpty()) {
            return true;
        }
        return user.groups().stream().anyMatch(applyToRoles::contains);
    }
}
