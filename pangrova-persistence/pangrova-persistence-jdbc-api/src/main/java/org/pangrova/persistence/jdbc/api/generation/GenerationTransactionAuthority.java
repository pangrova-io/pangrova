package org.pangrova.persistence.jdbc.api.generation;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;

/**
 * Current domain authority on an artifact caller's transaction. Implementations
 * acquire the authorization gate before tenant, table-policy and model checks.
 * They must use the supplied connection without committing, rolling back or
 * replacing it. Execution-owner and job locks are acquired afterwards by the caller.
 */
@Internal
public interface GenerationTransactionAuthority {
    /** Reads and validates the exact persisted tenant/job dispatch specification. */
    default GenerationDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
            throws SQLException {
        return authorize(connection, tenantId, jobId).dispatchSpec();
    }

    /** Includes the roles that authorized this exact persisted scope in the caller's transaction. */
    AuthorizedDispatch authorize(Connection connection, String tenantId, String jobId) throws SQLException;

    record AuthorizedDispatch(GenerationTransactionInputs.StoredDispatch stored, Set<String> currentDirectRoles) {
        public AuthorizedDispatch {
            Objects.requireNonNull(stored, "stored");
            currentDirectRoles = Set.copyOf(currentDirectRoles);
        }

        public GenerationDispatchSpec dispatchSpec() {
            return stored.dispatchSpec();
        }
    }

    /** Validates an explicit compiler-observed specification under current domain authority. */
    void requireCurrent(Connection connection, GenerationDispatchSpec spec) throws SQLException;

    /** Authorizes a persisted submission subject before its durable job and stage graph exist. */
    void requireCurrent(Connection connection, String tenantId, String principal,
                        DeferredAuthorizationScope authorizationScope) throws SQLException;
}
