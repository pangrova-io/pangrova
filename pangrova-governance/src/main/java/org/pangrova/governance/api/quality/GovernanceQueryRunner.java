package org.pangrova.governance.api.quality;

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
     * @param identity persisted subject; reauthorize its current privileges, tenant, row filters and masks
     * @param request exact target, predicate and result shape; never an executable SQL statement
     * @return aggregated result with row counts and failing samples
     * @throws GovernanceQueryException if the query fails
     */
    QualityQueryResult execute(QualityReadRequest request, QualityExecutionIdentity identity);

    /** The producer declares the expected result shape; aliases alone cannot distinguish sample columns. */
    enum QueryType { COUNTS, SAMPLES }
}
