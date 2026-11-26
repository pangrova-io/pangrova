package org.pangrova.query.exception;

/** Recovery must retry because a candidate admission still owns this query identity. */
public final class QueryRegistrationPendingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public QueryRegistrationPendingException(String queryId) {
        super("Query admission is still pending: " + queryId);
    }
}
