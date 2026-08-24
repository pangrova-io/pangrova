package org.pangrova.governance.quality.rule;

/**
 * Functional interface for executing quality rule SQL against table data.
 *
 * <p>The coordinator provides the implementation at wiring time, routing
 * governance SQL through the internal query pipeline. This avoids a direct
 * dependency from governance to coordinator internals.</p>
 */
@FunctionalInterface
public interface GovernanceQueryRunner {

    /**
     * Execute a quality rule SQL query and return aggregated results.
     *
     * <p>The implementation is responsible for running the SQL against the
     * target table and collecting pass/fail metrics with failing row samples.</p>
     *
     * @param sql the SQL statement to execute (a SELECT counting violations)
     * @return aggregated result with row counts and failing samples
     * @throws GovernanceQueryException if the query fails
     */
    QualityQueryResult execute(String sql);
}
