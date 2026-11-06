package org.pangrova.client.cli;

import org.pangrova.client.error.PangrovaClientException;
import org.pangrova.transport.client.http.PangrovaRestClient;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.client.protocol.QueryStatusResponse;
import org.pangrova.client.protocol.SubmitQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Read-Eval-Print Loop for interactive SQL execution.
 * Supports multi-line input (terminated by {@code ;}), {@code \q} or {@code exit} to quit.
 *
 * <p>Query polling respects a configurable timeout. When the timeout expires or the
 * polling thread is interrupted, a best-effort cancel request is sent to the coordinator
 * before reporting the error to the user.
 */
final class SqlShell {

    private static final Logger LOG = LoggerFactory.getLogger(SqlShell.class);
    private static final long POLL_INTERVAL_MS = 500;
    /** Default query timeout in minutes for the convenience constructor. */
    private static final long DEFAULT_QUERY_TIMEOUT_MINUTES = 30;
    private static final String PROMPT = "pangrova> ";
    private static final String CONTINUATION_PROMPT = "     -> ";

    private final QueryClient client;
    private final BufferedReader reader;
    private final PrintStream out;
    private final Duration queryTimeout;
    private final String catalog;
    private final String schema;

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout) {
        this(client, reader, out, queryTimeout, null);
    }

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout,
             String catalog) {
        this(client, reader, out, queryTimeout, catalog, null);
    }

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout,
             String catalog, String schema) {
        this.client = client;
        this.reader = reader;
        this.out = out;
        this.queryTimeout = queryTimeout;
        this.catalog = catalog;
        this.schema = schema;
    }

    /**
     * Convenience constructor that wraps an {@link PangrovaRestClient} and uses
     * the default 30-minute query timeout.
     */
    SqlShell(PangrovaRestClient client, BufferedReader reader, PrintStream out) {
        this(QueryClient.wrap(client), reader, out,
                Duration.ofMinutes(DEFAULT_QUERY_TIMEOUT_MINUTES));
    }

    /**
     * Starts the REPL loop. Blocks until the user types {@code \q} or {@code exit},
     * or the input stream is exhausted.
     */
    void run() {
        out.println("Pangrova SQL Shell. Type \\q or exit to quit.");
        out.println();

        StringBuilder sqlBuffer = new StringBuilder();
        boolean multiLine = false;

        while (true) {
            String line = readLine(multiLine);
            if (line == null) {
                break;
            }

            String trimmed = line.trim();

            if (!multiLine && isQuitCommand(trimmed)) {
                out.println("Bye!");
                break;
            }

            if (trimmed.isEmpty() && !multiLine) {
                continue;
            }

            multiLine = accumulateAndExecute(sqlBuffer, line, trimmed);
        }
    }

    private String readLine(boolean multiLine) {
        out.print(multiLine ? CONTINUATION_PROMPT : PROMPT);
        out.flush();
        try {
            return reader.readLine();
        } catch (IOException e) {
            LOG.warn("Failed to read input", e);
            return null;
        }
    }

    private boolean accumulateAndExecute(StringBuilder sqlBuffer, String line, String trimmed) {
        sqlBuffer.append(line);
        if (trimmed.endsWith(";")) {
            String sql = stripTrailingSemicolon(sqlBuffer.toString().trim());
            sqlBuffer.setLength(0);
            executeAndDisplay(sql);
            return false;
        }
        sqlBuffer.append('\n');
        return true;
    }

    private void executeAndDisplay(String sql) {
        try {
            execute(sql);
        } catch (QueryTimeoutException e) {
            out.println("Error [QUERY_TIMEOUT]: " + e.getMessage());
        } catch (PangrovaClientException e) {
            out.println("Error [" + e.errorCode() + "]: " + e.getMessage());
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
            LOG.debug("Unexpected error during query execution", e);
        }
    }

    /** Executes one server-parsed statement; failures propagate to batch callers. */
    String execute(String sql) {
        if (sql == null || sql.isBlank()) throw new IllegalArgumentException("SQL must not be blank");
        Map<String, String> sessionProps = new java.util.HashMap<>();
        if (catalog != null) sessionProps.put("catalog", catalog);
        if (schema != null) sessionProps.put("schema", schema);
        SubmitQueryResponse submitResp = client.submitQuery(sql, Map.copyOf(sessionProps));
        out.println("Query submitted: " + submitResp.queryId());
        consumeAndComplete(submitResp);
        if (out.checkError()) throw new IllegalStateException("Failed to write query results");
        return submitResp.queryId();
    }

    private void consumeAndComplete(SubmitQueryResponse submitResp) {
        String queryId = submitResp.queryId();
        long deadlineMs = Math.addExact(System.currentTimeMillis(), queryTimeout.toMillis());
        boolean success = false;
        try {
            QueryStatusResponse ready = poll(queryId, deadlineMs, true);
            byte[] resultBytes = null;
            if (ready.resultEndpoint() != null && !isFailedOrCancelled(ready.state())) {
                resultBytes = client.fetchResult(ready.resultEndpoint(), deadlineMs);
                checkDeadline(queryId, deadlineMs);
            }
            QueryStatusResponse terminal = isTerminalState(ready.state())
                    ? ready : poll(queryId, deadlineMs, false);
            handleQueryResult(submitResp, terminal, resultBytes);
            success = "FINISHED".equalsIgnoreCase(terminal.state());
        } catch (PangrovaClientException failure) {
            if ("TRANSPORT_QUERY_DEADLINE_EXCEEDED".equals(failure.errorCode())) {
                var timeout = new QueryTimeoutException(queryId, queryTimeout);
                timeout.initCause(failure);
                throw timeout;
            }
            throw failure;
        } finally {
            if (!success) bestEffortCancel(queryId);
        }
    }

    private void handleQueryResult(SubmitQueryResponse submitResp, QueryStatusResponse status, byte[] resultBytes) {
        if ("FAILED".equalsIgnoreCase(status.state())) {
            throw new PangrovaClientException(status.error() == null ? "QUERY_FAILED" : status.error().errorCode(),
                    status.error() == null ? "Query failed without error detail" : status.error().message());
        }
        if ("CANCELLED".equalsIgnoreCase(status.state())) {
            throw new PangrovaClientException("QUERY_CANCELLED", "Query cancelled");
        }
        List<String> columnNames = submitResp.columnNames();
        if (resultBytes == null && (columnNames == null || columnNames.isEmpty())) {
            out.println("OK (no result set)");
            return;
        }
        out.print(formatResults(resultBytes, columnNames));
    }

    private QueryStatusResponse poll(String queryId, long deadlineMs, boolean allowReadable) {
        while (true) {
            checkDeadline(queryId, deadlineMs);
            QueryStatusResponse status = client.getQueryStatus(queryId);
            String state = status.state();
            if (isTerminalState(state) || allowReadable && status.resultEndpoint() != null
                    && ("RUNNING".equalsIgnoreCase(state) || "FINISHING".equalsIgnoreCase(state))) {
                return status;
            }

            out.print(".");
            out.flush();

            ThreadUtils.sleepQuietly(POLL_INTERVAL_MS);
            if (Thread.currentThread().isInterrupted()) {
                throw new PangrovaClientException("INTERRUPTED",
                        "Query " + queryId + " interrupted; cancel request sent to coordinator");
            }
        }
    }

    private void checkDeadline(String queryId, long deadlineMs) {
        if (System.currentTimeMillis() >= deadlineMs) throw new QueryTimeoutException(queryId, queryTimeout);
        if (Thread.currentThread().isInterrupted()) {
            throw new PangrovaClientException("INTERRUPTED", "Query " + queryId + " interrupted");
        }
    }

    private static boolean isFailedOrCancelled(String state) {
        return "FAILED".equalsIgnoreCase(state) || "CANCELLED".equalsIgnoreCase(state);
    }

    private void bestEffortCancel(String queryId) {
        try {
            client.cancelQuery(queryId);
        } catch (Exception e) {
            LOG.warn("Failed to cancel query {} after timeout/interrupt: {}", queryId, e.getMessage());
        }
    }

    static String formatResults(byte[] resultBytes, List<String> columnNames) {
        if (resultBytes == null) {
            throw new PangrovaClientException("RESULT_ENDPOINT_MISSING", "Query finished without its result endpoint");
        }
        ResultBatchDecoder.DecodedResult decoded = ResultBatchDecoder.decode(resultBytes);

        List<String> headers = decoded.columnNames().isEmpty()
                ? columnNames == null ? List.of() : columnNames : decoded.columnNames();
        return TableFormatter.format(headers, decoded.rows());
    }

    static boolean isQuitCommand(String input) {
        return "\\q".equals(input) || "exit".equalsIgnoreCase(input);
    }

    static boolean isTerminalState(String state) {
        return "FINISHED".equalsIgnoreCase(state)
                || "FAILED".equalsIgnoreCase(state)
                || "CANCELLED".equalsIgnoreCase(state);
    }

    static String stripTrailingSemicolon(String sql) {
        if (sql.endsWith(";")) {
            return sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
