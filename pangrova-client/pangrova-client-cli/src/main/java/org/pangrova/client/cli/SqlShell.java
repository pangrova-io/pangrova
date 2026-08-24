package org.pangrova.client.cli;

import org.pangrova.client.rest.PangrovaClientException;
import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.protocol.rest.query.QueryStatusResponse;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
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

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout) {
        this(client, reader, out, queryTimeout, null);
    }

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout,
             String catalog) {
        this.client = client;
        this.reader = reader;
        this.out = out;
        this.queryTimeout = queryTimeout;
        this.catalog = catalog;
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
            Map<String, String> sessionProps = catalog != null && !catalog.isEmpty()
                    ? Map.of("catalog", catalog) : Map.of();
            SubmitQueryResponse submitResp = client.submitQuery(sql, sessionProps);
            out.println("Query submitted: " + submitResp.queryId());

            QueryStatusResponse status = pollUntilDone(submitResp.queryId());
            handleQueryResult(submitResp, status);
        } catch (QueryTimeoutException e) {
            out.println("Error [QUERY_TIMEOUT]: " + e.getMessage());
        } catch (PangrovaClientException e) {
            out.println("Error [" + e.errorCode() + "]: " + e.getMessage());
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
            LOG.debug("Unexpected error during query execution", e);
        }
    }

    private void handleQueryResult(SubmitQueryResponse submitResp, QueryStatusResponse status) {
        if ("FAILED".equalsIgnoreCase(status.state())) {
            printError(status);
            return;
        }
        List<String> columnNames = submitResp.columnNames();
        if (columnNames == null || columnNames.isEmpty()) {
            out.println("OK (no result set)");
            return;
        }
        displayResults(status, columnNames);
    }

    private QueryStatusResponse pollUntilDone(String queryId) {
        long deadlineNanos = System.nanoTime() + queryTimeout.toNanos();
        while (true) {
            QueryStatusResponse status = client.getQueryStatus(queryId);
            String state = status.state();

            if (isTerminalState(state)) {
                return status;
            }

            if (System.nanoTime() >= deadlineNanos) {
                bestEffortCancel(queryId);
                throw new QueryTimeoutException(queryId, queryTimeout);
            }

            out.print(".");
            out.flush();

            ThreadUtils.sleepQuietly(POLL_INTERVAL_MS);
            if (Thread.currentThread().isInterrupted()) {
                bestEffortCancel(queryId);
                throw new PangrovaClientException("INTERRUPTED",
                        "Query " + queryId + " interrupted; cancel request sent to coordinator");
            }
        }
    }

    private void bestEffortCancel(String queryId) {
        try {
            client.cancelQuery(queryId);
        } catch (Exception e) {
            LOG.warn("Failed to cancel query {} after timeout/interrupt: {}", queryId, e.getMessage());
        }
    }

    private void displayResults(QueryStatusResponse status, List<String> columnNames) {
        if (status.resultEndpoint() == null) {
            out.println("OK (no result endpoint)");
            return;
        }

        byte[] resultBytes = client.fetchResult(status.resultEndpoint());
        ResultBatchDecoder.DecodedResult decoded;
        try {
            decoded = ResultBatchDecoder.decode(resultBytes);
        } catch (ResultDecodeException e) {
            out.println("Error decoding result: " + e.getMessage());
            return;
        }

        List<String> headers = decoded.columnNames().isEmpty() ? columnNames : decoded.columnNames();
        String table = TableFormatter.format(headers, decoded.rows());
        out.print(table);
    }

    private void printError(QueryStatusResponse status) {
        if (status.error() != null) {
            out.println("Error [" + status.error().errorCode() + "]: " + status.error().message());
        } else {
            out.println("Query failed (no error detail available)");
        }
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
