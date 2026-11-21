package org.pangrova.persistence.jdbc.api.security.policy;

import org.pangrova.common.api.annotation.Internal;

import java.sql.Connection;
import java.sql.SQLException;

/** Backend coordination for current-authority checks and the mutation they protect. */
@Internal
@FunctionalInterface
public interface AuthorizationPublicationLock {
    /**
     * Acquires the backend's shared authorization publication gate on the caller's transaction.
     * The caller must have disabled auto-commit and retain the transaction through its protected
     * mutation. The lock is released only by that transaction's completion. Implementations must
     * reject an invalid transaction, propagate acquisition failure, and never commit, roll back,
     * close or replace the connection. A no-op implementation cannot provide this guarantee.
     */
    void lockPublication(Connection connection) throws SQLException;
}
