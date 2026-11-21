package org.pangrova.persistence.security.postgresql.tenant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Predicate;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.persistence.security.api.tenant.DeferredTenantAuthority;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

/** PostgreSQL deferred-tenant authority over the canonical tenant and membership tables. */
public final class PostgresDeferredTenantAuthority implements DeferredTenantAuthority {

    @Override
    public void requireCurrent(Connection connection, String principal, String tenantId,
            DeferredAuthorizationScope scope) throws SQLException {
        JdbcTenantStore.requireDeferredTenant(connection, principal, tenantId, scope);
    }

    @Override
    public void requireCurrent(Connection connection, String principal, String tenantId,
            DeferredAuthorizationScope scope, Predicate<QualifiedTableName> engineOwnedResource)
            throws SQLException {
        JdbcTenantStore.requireDeferredTenant(connection, principal, tenantId, scope, engineOwnedResource);
    }
}
