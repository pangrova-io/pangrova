package org.pangrova.persistence.api.generation.transaction.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.generation.GenerationDependency;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.job.GenerationPage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
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
    Optional<GenerationDispatchSpec> findDispatchSpec(Connection connection, String tenantId, String jobId)
            throws SQLException;

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
