package org.pangrova.spi.security.rowfilter;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Set;

/**
 * Row-level security policy that filters rows based on user identity and role membership.
 */
@Experimental
public record RowPolicy(
        long policyId,
        String policyName,
        String catalogName,
        String schemaName,
        String tableName,
        String filterExpression,
        List<String> applyToRoles,
        boolean enabled,
        int priority,
        java.util.UUID tableId
) {

    /**
     * Defensive copies of mutable lists to ensure immutability.
     * Rejects filter expressions containing DDL keywords.
     */
    public RowPolicy {
        applyToRoles = applyToRoles != null ? List.copyOf(applyToRoles) : List.of();
        org.pangrova.spi.security.policy.SqlIdentifierValidator.rejectDdlInExpression(filterExpression);
    }

    /**
     * Returns true if this policy applies to the given user.
     * An empty applyToRoles list means the policy applies to everyone.
     */
    public boolean appliesTo(Set<String> effectiveRoles) {
        if (applyToRoles.isEmpty()) {
            return true;
        }
        return effectiveRoles.stream().anyMatch(applyToRoles::contains);
    }
}
