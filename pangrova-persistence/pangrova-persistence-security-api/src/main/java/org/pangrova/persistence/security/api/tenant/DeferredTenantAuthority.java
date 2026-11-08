package org.pangrova.persistence.security.api.tenant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Predicate;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

/**
 * Validates deferred-work tenant membership and namespace on the caller's transaction.
 * Implementations must use the caller-owned connection and never commit, roll back or close it.
 */
public interface DeferredTenantAuthority {

    /** Requires the principal to be an active member of the tenant with an in-scope namespace. */
    void requireCurrent(Connection connection, String principal, String tenantId,
            DeferredAuthorizationScope scope) throws SQLException;

    /**
     * Same check, exempting engine-owned resources from the tenant's user-facing
     * catalog/schema restriction.
     */
    void requireCurrent(Connection connection, String principal, String tenantId,
            DeferredAuthorizationScope scope, Predicate<QualifiedTableName> engineOwnedResource)
            throws SQLException;
}
