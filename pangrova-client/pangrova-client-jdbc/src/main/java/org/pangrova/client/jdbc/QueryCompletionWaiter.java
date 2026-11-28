package org.pangrova.client.jdbc;

import org.pangrova.client.http.QueryExecution;
import org.pangrova.client.jdbc.exception.QueryCancelledException;
import org.pangrova.common.client.exception.PangrovaConnectionException;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.query.SubmitQueryResponse;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Optional;

/**
 * Waits for a readable result stream or resultless completion, then for terminal outcome.
 *
 * Maps the owner-published query status to JDBC results and errors.
 */
final class QueryCompletionWaiter {

    private static final long MILLIS_PER_SECOND = 1000L;

    private static final String STATE_FINISHED = "FINISHED";
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_FINISHING = "FINISHING";
    private static final String STATE_FAILED = "FAILED";
    private static final String STATE_CANCELLED = "CANCELLED";

    private QueryCompletionWaiter() {
    }

    /**
     * Resolves the actual server result: a readable stream or completed resultless query.
     * A streaming OUTPUT task may need its Flight stream consumed before it can finish.
     *
     * @param queryTimeoutSeconds JDBC query timeout; {@code 0} or less means no timeout
     * @return the result disposition, endpoint when streaming, and whether the query was terminal
     * @throws SQLTimeoutException if the timeout elapses first; the execution owner cancels the query
     * @throws SQLException        if the query fails or is cancelled server-side
     */
    static ReadyResult awaitResult(QueryExecution query, long deadlineMs,
                                               int queryTimeoutSeconds) throws SQLException {
        SubmitQueryResponse receipt = query.submission();
        String queryId = receipt.queryId();
        if (STATE_FAILED.equals(receipt.state())) {
            // The receipt contains no ErrorDetail. Preserve the owner's recorded failure.
            QueryStatusResponse status = awaitStatus(query, QueryWaitTarget.TERMINAL,
                    deadlineMs, queryTimeoutSeconds);
            if (STATE_FAILED.equals(status.state())) {
                throw failedQueryException(status, queryId);
            }
            throw new SQLException("Query failed, queryId=" + queryId);
        }
        ReadyResult immediate = checkImmediateResult(receipt, queryId);
        if (immediate != null) {
            return immediate;
        }
        while (true) {
            QueryStatusResponse status = awaitStatus(query, QueryWaitTarget.RESULT_OR_TERMINAL,
                    deadlineMs, queryTimeoutSeconds);
            ReadyResult result = handleReadableState(status, queryId);
            if (result != null) {
                return result;
            }
        }
    }

    static long deadlineFor(int queryTimeoutSeconds) {
        return queryTimeoutSeconds > 0
                ? System.currentTimeMillis() + queryTimeoutSeconds * MILLIS_PER_SECOND
                : Long.MAX_VALUE;
    }

    /** Checks the canonical terminal outcome after the Flight stream is drained. */
    static QueryStatusResponse awaitTerminalSuccess(QueryExecution query, long deadlineMs, int queryTimeoutSeconds)
            throws SQLException {
        String queryId = query.submission().queryId();
        while (true) {
            QueryStatusResponse status = awaitStatus(query, QueryWaitTarget.TERMINAL,
                    deadlineMs, queryTimeoutSeconds);
            if (STATE_FAILED.equals(status.state())) {
                throw failedQueryException(status, queryId);
            }
            if (STATE_CANCELLED.equals(status.state())) {
                throw new QueryCancelledException(queryId);
            }
            if (STATE_FINISHED.equals(status.state())) {
                return status;
            }
        }
    }

    /** A readable endpoint can be present in the submit response itself. */
    private static ReadyResult checkImmediateResult(SubmitQueryResponse response,
                                                      String queryId) throws SQLException {
        if (STATE_CANCELLED.equals(response.state())) {
            throw new QueryCancelledException(queryId);
        }
        if (response.resultDisposition() == ResultDisposition.STREAM && isReadableState(response.state())) {
            return new ReadyResult(ResultDisposition.STREAM, response.resultEndpoint(),
                    STATE_FINISHED.equals(response.state()), Optional.empty());
        }
        if (response.resultDisposition() == ResultDisposition.NONE && STATE_FINISHED.equals(response.state())) {
            return new ReadyResult(ResultDisposition.NONE, null, true, Optional.empty());
        }
        return null;
    }

    static void checkTimeout(String queryId, long deadlineMs, int queryTimeoutSeconds) throws SQLTimeoutException {
        if (System.currentTimeMillis() >= deadlineMs) {
            // Only PangrovaStatement.runQuery owns automatic cancellation and its acknowledgement.
            // A second cancellation here could overwrite a real ACK with a later NOT_FOUND.
            throw new SQLTimeoutException(
                    "Query timed out after " + queryTimeoutSeconds + "s, queryId=" + queryId);
        }
    }

    /**
     * Maps a status response onto either a readable endpoint or a failure.
     *
     * @return the result when readable, or {@code null} while still scheduling
     */
    private static ReadyResult handleReadableState(QueryStatusResponse status,
                                                     String queryId) throws SQLException {
        String state = status.state();
        if (STATE_FAILED.equals(state)) {
            throw failedQueryException(status, queryId);
        }
        if (STATE_CANCELLED.equals(state)) {
            throw new QueryCancelledException(queryId);
        }
        return switch (status.resultDisposition()) {
            case STREAM -> isReadableState(state)
                    ? new ReadyResult(ResultDisposition.STREAM, status.resultEndpoint(),
                        STATE_FINISHED.equals(state), Optional.empty())
                    : null;
            case NONE -> STATE_FINISHED.equals(state)
                    ? new ReadyResult(ResultDisposition.NONE, null, true, Optional.of(status))
                    : null;
            case UNAVAILABLE -> throw new SQLException(
                    "Query result is no longer available, queryId=" + queryId, "HY000");
            case PENDING -> null;
        };
    }

    private static boolean isReadableState(String state) {
        return STATE_RUNNING.equals(state) || STATE_FINISHING.equals(state)
                || STATE_FINISHED.equals(state);
    }

    /** Carries a terminal resultless status when observed directly, so its update count need not be fetched twice. */
    record ReadyResult(ResultDisposition disposition, ResultEndpoint endpoint, boolean terminal,
                       Optional<QueryStatusResponse> terminalStatus) {}

    private static SQLException failedQueryException(QueryStatusResponse status, String queryId) {
        String errorMsg = status.error() != null
                ? "[" + status.error().errorCode() + "] " + status.error().message()
                : "Query failed without error details";
        return new SQLException("Query failed: " + errorMsg + ", queryId=" + queryId);
    }

    private static QueryStatusResponse awaitStatus(QueryExecution query, QueryWaitTarget target,
            long deadlineMs, int timeoutSeconds) throws SQLException {
        String queryId = query.submission().queryId();
        checkTimeout(queryId, deadlineMs, timeoutSeconds);
        try {
            return query.awaitStatus(target, deadlineMs);
        } catch (PangrovaConnectionException failure) {
            if ("QUERY_STATUS_DEADLINE_EXCEEDED".equals(failure.errorCode())) {
                throw new SQLTimeoutException("Query timed out after " + timeoutSeconds
                        + "s, queryId=" + queryId, failure);
            }
            if ("CONNECTION_INTERRUPTED".equals(failure.errorCode())) {
                throw new SQLException("Interrupted while waiting for query completion", failure);
            }
            throw failure;
        }
    }
}
