package org.pangrova.client.jdbc;

import org.pangrova.transport.client.http.PangrovaRestClient;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.client.protocol.QueryStatusResponse;
import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.client.protocol.SubmitQueryResponse;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;

/**
 * Waits for a readable result endpoint, then for the query's terminal outcome.
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
    private static final String STATE_RUNNING = "RUNNING";
    private static final String STATE_FINISHING = "FINISHING";
    private static final String STATE_FAILED = "FAILED";
    private static final String STATE_CANCELLED = "CANCELLED";

    private QueryCompletionWaiter() {
    }

    /**
     * Waits until the result endpoint can be read. A streaming OUTPUT task may
     * need the client to consume its Flight stream before it can finish.
     *
     * @param queryTimeoutSeconds JDBC query timeout; {@code 0} or less means no timeout
     * @return the endpoint and whether the query was already terminal
     * @throws SQLTimeoutException if the timeout elapses first; the execution owner cancels the query
     * @throws SQLException        if the query fails or is cancelled server-side
     */
    static ReadyEndpoint awaitReadableEndpoint(PangrovaRestClient client, String queryId,
                                               SubmitQueryResponse submitResponse,
                                               long deadlineMs,
                                               int queryTimeoutSeconds) throws SQLException {
        ReadyEndpoint immediate = checkImmediateResult(submitResponse, queryId);
        if (immediate != null) {
            return immediate;
        }
        long pollMs = POLL_INTERVAL_MS;
        while (true) {
            checkTimeout(queryId, deadlineMs, queryTimeoutSeconds);
            QueryStatusResponse status = client.getQueryStatus(queryId);
            ReadyEndpoint result = handleReadableState(status, queryId);
            if (result != null) {
                return result;
            }
            pollMs = sleepWithBackoff(pollMs);
        }
    }

    static long deadlineFor(int queryTimeoutSeconds) {
        return queryTimeoutSeconds > 0
                ? System.currentTimeMillis() + queryTimeoutSeconds * MILLIS_PER_SECOND
                : Long.MAX_VALUE;
    }

    /** Checks the canonical terminal outcome after the Flight stream is drained. */
    static QueryStatusResponse awaitTerminalSuccess(PangrovaRestClient client, String queryId,
                                                    long deadlineMs, int queryTimeoutSeconds)
            throws SQLException {
        long pollMs = POLL_INTERVAL_MS;
        while (true) {
            checkTimeout(queryId, deadlineMs, queryTimeoutSeconds);
            QueryStatusResponse status = client.getQueryStatus(queryId);
            if (STATE_FAILED.equals(status.state())) {
                throw failedQueryException(status, queryId);
            }
            if (STATE_CANCELLED.equals(status.state())) {
                throw new SQLException("Query was cancelled, queryId=" + queryId);
            }
            if (STATE_FINISHED.equals(status.state())) {
                return status;
            }
            pollMs = sleepWithBackoff(pollMs);
        }
    }

    /** A readable endpoint can be present in the submit response itself. */
    private static ReadyEndpoint checkImmediateResult(SubmitQueryResponse response,
                                                      String queryId) throws SQLException {
        if (STATE_FAILED.equals(response.state()) || STATE_CANCELLED.equals(response.state())) {
            throw new SQLException("Query " + response.state().toLowerCase()
                    + ", queryId=" + queryId);
        }
        if (response.resultEndpoint() != null && isReadableState(response.state())) {
            return new ReadyEndpoint(response.resultEndpoint(),
                    STATE_FINISHED.equals(response.state()));
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
     * @return the endpoint when readable, or {@code null} while still scheduling
     */
    private static ReadyEndpoint handleReadableState(QueryStatusResponse status,
                                                     String queryId) throws SQLException {
        String state = status.state();
        if (STATE_FAILED.equals(state)) {
            throw failedQueryException(status, queryId);
        }
        if (STATE_CANCELLED.equals(state)) {
            throw new SQLException("Query was cancelled, queryId=" + queryId);
        }
        if (STATE_FINISHED.equals(state)) {
            return new ReadyEndpoint(requireEndpoint(status.resultEndpoint(), queryId), true);
        }
        if (status.resultEndpoint() != null && isReadableState(state)) {
            return new ReadyEndpoint(status.resultEndpoint(), false);
        }
        return null;
    }

    private static boolean isReadableState(String state) {
        return STATE_RUNNING.equals(state) || STATE_FINISHING.equals(state)
                || STATE_FINISHED.equals(state);
    }

    record ReadyEndpoint(ResultEndpoint endpoint, boolean terminal) {}

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
