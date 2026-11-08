package org.pangrova.persistence.workflow.api.security;

import java.sql.Connection;
import java.sql.SQLException;

import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

/**
 * Revalidates a deferred table write against current table, catalog and policy state on the
 * caller-owned transaction connection.
 */
@FunctionalInterface
public interface TableControlWriteAuthority {

    void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope)
            throws SQLException;
}
