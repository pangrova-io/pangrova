package org.pangrova.indexing.control;

import org.pangrova.security.authz.DeferredAuthorizationScope;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Durable table-control authority gate for deferred artifact work.
 *
 * <p>Verifies on the publication transaction that the table-control identity,
 * catalog deployment, governance bindings, and row policies a deferred artifact
 * was admitted against are still current. The table-control owner
 * ({@code pangrova-metadata}) implements this check and the composition root
 * injects it, so {@code pangrova-indexing} never depends on metadata.</p>
 */
@FunctionalInterface
public interface DeferredTableControlAuthority {

    /**
     * Re-checks current table-control authority on the caller's transaction.
     *
     * @param connection open connection with the publication locks held
     * @param principal  invocation principal
     * @param scope      deferred authorization scope recorded at admission
     */
    void requireCurrent(Connection connection, String principal,
            DeferredAuthorizationScope scope) throws SQLException;
}
