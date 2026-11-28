package org.pangrova.persistence.jdbc.api.generation;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.dependency.GenerationDependency;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.page.GenerationPage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable, scoped task inputs read using exactly the supplied connection.
 * Implementations support both auto-commit reads and caller-owned transactions;
 * they never acquire another connection, change auto-commit, commit, roll back or
 * close the supplied connection. Domain callers retain authorization and lock ownership.
 */
@Internal
public interface GenerationTransactionInputs {
    /**
     * Reads the persisted compiler-observed contract, including checksummed graph
     * and authorization scope. Missing state returns empty; corrupt state is rejected.
     * This read does not grant current authority or synthesize missing observations.
     */
    default Optional<GenerationDispatchSpec> findDispatchSpec(Connection connection, String tenantId, String jobId)
            throws SQLException {
        return findStoredDispatch(connection, tenantId, jobId).map(StoredDispatch::dispatchSpec);
    }

    /** Reads the specification and its verified canonical graph digest from the same row. */
    Optional<StoredDispatch> findStoredDispatch(Connection connection, String tenantId, String jobId)
            throws SQLException;

    /** The exact persisted contract and the digest already checked against its graph bytes. */
    record StoredDispatch(GenerationDispatchSpec dispatchSpec, String graphSha256) {
        public StoredDispatch {
            Objects.requireNonNull(dispatchSpec, "dispatchSpec");
            if (!DigestUtils.isSha256Hex(graphSha256)) {
                throw new IllegalArgumentException("Stored dispatch requires the verified SHA-256 of its graph");
            }
        }
    }

    /** Loads exact version-pinned job dependencies in persisted ordinal order. */
    List<GenerationDependency> loadJobDependencies(Connection connection, String tenantId, String jobId)
            throws SQLException;

    /**
     * Loads the exact page cohort in persisted ordinal order, including canonical
     * checksummed splits and page dependencies on the same connection. Missing or
     * corrupt legacy split bytes are rejected instead of fabricating replay inputs.
     */
    List<GenerationPage> loadPages(Connection connection, String tenantId, String jobId) throws SQLException;
}
