package org.pangrova.planner.core.rerank;

/**
 * Thrown when a RERANK BY clause fails validation during query planning.
 *
 * <p>Common causes:
 * <ul>
 *   <li>RERANK BY combined with GROUP BY/HAVING (incompatible)</li>
 *   <li>RERANK BY without any retrieval source (no vector ORDER BY or MATCH())</li>
 *   <li>Invalid strategy parameters (non-positive k, non-numeric weights)</li>
 * </ul>
 */
public final class RerankerValidationException extends RuntimeException {

    private static final int ERROR_CODE = 3002;

    public RerankerValidationException(String message) {
        super(message);
    }

    /**
     * Returns the Pangrova error code for reranker validation failures.
     *
     * @return error code 3002 (RERANK_REQUIRES_RETRIEVAL)
     */
    public int errorCode() {
        return ERROR_CODE;
    }
}
