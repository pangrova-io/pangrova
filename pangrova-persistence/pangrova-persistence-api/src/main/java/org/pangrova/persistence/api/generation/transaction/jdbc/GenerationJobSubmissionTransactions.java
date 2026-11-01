package org.pangrova.persistence.api.generation.transaction.jdbc;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.job.GenerationJob;
import org.pangrova.spi.generation.job.GenerationPage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Canonical job submission participating in a caller-owned transaction. */
@Internal
@FunctionalInterface
public interface GenerationJobSubmissionTransactions {
    /**
     * Validates admission and current authority, then atomically inserts or verifies
     * the exact immutable job, dispatch graph, route, dependencies and page cohort.
     * The supplied connection must already have auto-commit disabled. Implementations
     * use only this connection and never commit, roll back, close it, change its
     * auto-commit state, or acquire a replacement transaction. The caller retains
     * responsibility for committing or rolling back all participating domain writes.
     */
    void createJob(Connection connection, GenerationJob job,
            GenerationDispatchSpec dispatchSpec, List<GenerationPage> pages) throws SQLException;
}
