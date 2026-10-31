package org.pangrova.security.spi.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Current model invocation authority for Security-owned deferred authorization receipts. */
@Internal
public interface CurrentModelAuthority {
    /**
     * Observes exact model incarnations, revisions and deployments in logical-name order,
     * after acquiring the authorization gate and checking the principal's current direct roles.
     * An empty name set still requires the gate and current role authority.
     * The caller owns the connection and retains its transaction through the protected operation;
     * the provider never acquires, commits, rolls back or closes a connection.
     */
    List<ModelSnapshot> observe(Connection connection, String principal, Set<String> modelNames) throws SQLException;

    /**
     * Requires current invocation authority and exact equality with every supplied receipt snapshot.
     * Nonempty snapshots acquire the authorization gate on the caller's explicit transaction;
     * an empty list requires no model authority and returns without touching the connection.
     * The provider never commits, rolls back or closes the caller's transaction.
     */
    void requireCurrent(Connection connection, String principal, List<ModelSnapshot> expected) throws SQLException;
}
