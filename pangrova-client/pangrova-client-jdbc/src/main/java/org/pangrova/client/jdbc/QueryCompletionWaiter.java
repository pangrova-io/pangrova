package org.pangrova.client.jdbc;

import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.protocol.rest.query.QueryStatusResponse;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

/**
 * Blocks until a submitted query reaches a terminal state and yields its result
 * endpoint.
 *
 * <p>Extracted from {@link PangrovaStatement} so the JDBC surface stays readable:
 * everything here is the polling protocol (backoff, timeout, terminal-state
 * mapping), which is independent of {@code Statement} semantics.
 */
final class QueryCompletionWaiter {

    private static final long POLL_INTERVAL_MS = 200;
    private static final long MAX_POLL_INTERVAL_MS = 5000;
    private static final long MILLIS_PER_SECOND = 1000L;

    private static final String STATE_FINISHED = "FINISHED";
    private static final String STATE_FAILED = "FAILED";
    private static final String STATE_CANCELLED = "CANCELLED";

    private QueryCompletionWaiter() {
    }

    /**
     * Waits for {@code queryId} to finish.
     *
     * @param queryTimeoutSeconds JDBC query timeout; {@code 0} or less means no timeout
     * @return the endpoint the result can be fetched from
     * @throws SQLTimeoutException if the timeout elapses first (the query is cancelled)
     * @throws SQLException        if the query fails or is cancelled server-side
     */
    static ResultEndpoint awaitCompletion(PangrovaRestClient client, String queryId,
                                          SubmitQueryResponse submitResponse,
                                          int queryTimeoutSeconds) throws SQLException {
        ResultEndpoint immediate = checkImmediateResult(submitResponse);
        if (immediate != null) {
            return immediate;
        }
        long deadlineMs = queryTimeoutSeconds > 0
                ? System.currentTimeMillis() + queryTimeoutSeconds * MILLIS_PER_SECOND
                : Long.MAX_VALUE;
        return pollUntilTerminal(client, queryId, deadlineMs, queryTimeoutSeconds);
    }

    /** A query that finished during submission needs no polling at all. */
    private static ResultEndpoint checkImmediateResult(SubmitQueryResponse response) {
        if (response.resultEndpoint() != null && STATE_FINISHED.equals(response.state())) {
            return response.resultEndpoint();
        }
        return null;
    }

    private static ResultEndpoint pollUntilTerminal(PangrovaRestClient client, String queryId,
                                                    long deadlineMs,
                                                    int queryTimeoutSeconds) throws SQLException {
        long pollMs = POLL_INTERVAL_MS;
        while (true) {
            checkTimeout(client, queryId, deadlineMs, queryTimeoutSeconds);
            QueryStatusResponse status = client.getQueryStatus(queryId);
            ResultEndpoint result = handleTerminalState(status, queryId);
            if (result != null) {
                return result;
            }
            pollMs = sleepWithBackoff(pollMs);
        }
    }

    private static void checkTimeout(PangrovaRestClient client, String queryId, long deadlineMs,
                                     int queryTimeoutSeconds) throws SQLException {
        if (System.currentTimeMillis() > deadlineMs) {
            try {
                client.cancelQuery(queryId);
            } catch (Exception ignored) {
                // Best-effort cancel: the timeout is reported regardless.
            }
            throw new SQLTimeoutException(
                    "Query timed out after " + queryTimeoutSeconds + "s, queryId=" + queryId);
        }
    }

    /**
     * Maps a status response onto a terminal outcome.
     *
     * @return the result endpoint when finished, or {@code null} while still running
     */
    private static ResultEndpoint handleTerminalState(QueryStatusResponse status,
                                                      String queryId) throws SQLException {
        String state = status.state();
        if (STATE_FINISHED.equals(state)) {
            return requireEndpoint(status.resultEndpoint(), queryId);
        }
        if (STATE_FAILED.equals(state)) {
            throw failedQueryException(status, queryId);
        }
        if (STATE_CANCELLED.equals(state)) {
            throw new SQLException("Query was cancelled, queryId=" + queryId);
        }
        return null;
    }

    private static ResultEndpoint requireEndpoint(ResultEndpoint endpoint,
                                                  String queryId) throws SQLException {
        if (endpoint == null) {
            throw new SQLException(
                    "Query finished but no result endpoint available, queryId=" + queryId);
        }
        return endpoint;
    }

    private static SQLException failedQueryException(QueryStatusResponse status, String queryId) {
        String errorMsg = status.error() != null
                ? "[" + status.error().errorCode() + "] " + status.error().message()
                : "Query failed without error details";
        return new SQLException("Query failed: " + errorMsg + ", queryId=" + queryId);
    }

    /** Sleeps, then doubles the interval up to {@link #MAX_POLL_INTERVAL_MS}. */
    private static long sleepWithBackoff(long pollMs) throws SQLException {
        ThreadUtils.sleepQuietly(pollMs);
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("Interrupted while waiting for query completion");
        }
        return Math.min(pollMs * 2, MAX_POLL_INTERVAL_MS);
    }
}
