package org.pangrova.databuild.spi.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.databuild.api.DataBuildDependency;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildPage;

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
public interface DataBuildTransactionInputs {
    /**
     * Reads the persisted compiler-observed contract, including checksummed graph
     * and authorization scope. Missing state returns empty; corrupt state is rejected.
     * This read does not grant current authority or synthesize missing observations.
     */
    Optional<DataBuildDispatchSpec> findDispatchSpec(Connection connection, String tenantId, String jobId)
            throws SQLException;

    /** Loads exact version-pinned job dependencies in persisted ordinal order. */
    List<DataBuildDependency> loadJobDependencies(Connection connection, String tenantId, String jobId)
            throws SQLException;

    /**
     * Loads the exact page cohort in persisted ordinal order, including canonical
     * checksummed splits and page dependencies on the same connection. Missing or
     * corrupt legacy split bytes are rejected instead of fabricating replay inputs.
     */
    List<DataBuildPage> loadPages(Connection connection, String tenantId, String jobId) throws SQLException;
}
