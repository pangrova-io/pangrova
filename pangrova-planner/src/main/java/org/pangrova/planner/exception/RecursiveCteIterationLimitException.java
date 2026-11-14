package org.pangrova.planner.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.planner.api.exception.PlannerException;

/**
 * Thrown when a recursive CTE exceeds the configured iteration limit.
 * Indicates potential infinite recursion in the query.
 */
public final class RecursiveCteIterationLimitException extends PlannerException {

    private final String cteName;
    private final int iterationLimit;

    /**
     * Creates an exception for the CTE that exceeded its iteration limit.
     *
     * @param cteName name of the recursive CTE
     * @param iterationLimit configured iteration limit that was exceeded
     */
    public RecursiveCteIterationLimitException(String cteName, int iterationLimit) {
        super(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "Recursive CTE '%s' exceeded iteration limit of %d".formatted(cteName, iterationLimit));
        this.cteName = cteName;
        this.iterationLimit = iterationLimit;
    }

    public String cteName() {
        return cteName;
    }

    public int iterationLimit() {
        return iterationLimit;
    }
}
