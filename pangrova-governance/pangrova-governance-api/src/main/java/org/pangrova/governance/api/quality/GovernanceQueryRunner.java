package org.pangrova.governance.api.quality;

import org.pangrova.governance.api.exception.GovernanceQueryException;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Functional interface for executing quality rule SQL against table data.
 *
 * <p>An installed data-plane runner must bind the target's stable ID to its current
 * qualified name before reading, then authorize the persisted subject for all
 * referenced relations. The production Coordinator currently has no such runner.</p>
 */
@FunctionalInterface
@Experimental
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

}
