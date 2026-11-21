package org.pangrova.persistence.jdbc.api.security.policy;

import java.sql.Connection;
import java.sql.SQLException;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

/**
 * Current table/policy authorization participating in the caller's JDBC transaction.
 * The caller owns connection, lock ordering, commit and rollback. Implementations
 * must use this exact connection, must not close it or change transaction state,
 * and must reject changed identity, revoked grants or incompatible policy. A
 * historical authorization observation is not sufficient. Exceptions propagate
 * to the transaction owner; there is no permissive default implementation.
 */
@Experimental
@FunctionalInterface
public interface CurrentTableAuthorization {
    /** Validates current authority in the already acquired transaction scope. */
    void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope) throws SQLException;
}
