package org.pangrova.client.jdbc.exception;

import java.sql.SQLException;

/** A cancellation confirmed by the Coordinator that owns the query. */
public final class QueryCancelledException extends SQLException {
    private static final long serialVersionUID = 1L;

    public QueryCancelledException(String queryId) {
        super("Query was cancelled, queryId=" + queryId);
    }
}
