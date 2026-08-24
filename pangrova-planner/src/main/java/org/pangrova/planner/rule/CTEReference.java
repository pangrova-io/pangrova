package org.pangrova.planner.rule;

/**
 * Describes a CTE (Common Table Expression) with its cost and reference count.
 *
 * @param cteName        name of the CTE
 * @param cteCost        estimated cost of computing the CTE once
 * @param referenceCount number of times the CTE is referenced in the query
 */
public record CTEReference(String cteName, double cteCost, int referenceCount) {

    public CTEReference {
        if (cteName == null || cteName.isBlank()) {
            throw new IllegalArgumentException("cteName must not be blank");
        }
        if (cteCost < 0) {
            throw new IllegalArgumentException("cteCost must be non-negative");
        }
        if (referenceCount < 0) {
            throw new IllegalArgumentException("referenceCount must be non-negative");
        }
    }
}
