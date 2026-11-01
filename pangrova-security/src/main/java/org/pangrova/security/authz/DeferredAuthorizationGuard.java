package org.pangrova.security.authz;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Revalidates a deferred authorization scope against the exact current durable authority.
 *
 * <p>Consumers that persist long-lived work on behalf of a deferred grant (for example
 * scalar index builds) must re-check the grant on the writing connection immediately
 * before using it. The implementation is supplied by the owner of the durable policy
 * records so consumers never read those records directly.</p>
 */
@FunctionalInterface
public interface DeferredAuthorizationGuard {

    /**
     * Fails closed when the principal, tenant, table identity, catalog revision or
     * policy projection no longer matches the scope captured at scheduling time.
     */
    void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope)
            throws SQLException;
}
