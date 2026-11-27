package org.pangrova.client.cli;

import org.pangrova.client.cli.exception.QueryTimeoutException;
import org.pangrova.client.http.QueryClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.ResultCursor;
import org.pangrova.transport.client.exception.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Read-Eval-Print Loop for interactive SQL execution.
 * Supports multi-line input (terminated by {@code ;}), {@code \q} or {@code exit} to quit.
 *
 * <p>Query status and Flight reads respect a configurable timeout. When the timeout expires or the
 * waiting thread is interrupted, a best-effort cancel request is sent to the coordinator
 * before reporting the error to the user.
 */
final class SqlShell {

    private static final Logger LOG = LoggerFactory.getLogger(SqlShell.class);
    private static final long REVOCATION_STATUS_BUDGET_MS = 500;
    private static final int DISPLAY_PAGE_ROWS = 128;
    private static final String PROMPT = "pangrova> ";
    private static final String CONTINUATION_PROMPT = "     -> ";
    private static final int COMPACT_BANNER_WIDTH = 80;
    private static final String FULL_BANNER = """
            ██████╗  █████╗ ███╗   ██╗ ██████╗ ██████╗  ██████╗ ██╗   ██╗ █████╗
            ██╔══██╗██╔══██╗████╗  ██║██╔════╝ ██╔══██╗██╔═══██╗██║   ██║██╔══██╗
            ██║  ██║███████║██╔██╗ ██║██║  ███╗██████╔╝██║   ██║██║   ██║███████║
            ██║  ██║██╔══██║██║╚██╗██║██║   ██║██╔══██╗██║   ██║╚██╗ ██╔╝██╔══██║
            ██████╔╝██║  ██║██║ ╚████║╚██████╔╝██║  ██║╚██████╔╝ ╚████╔╝ ███████╗
            ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝   ╚═══╝  ╚══════╝
            """;

    private final QueryClient client;
    private final BufferedReader reader;
    private final PrintStream out;
    private final Duration queryTimeout;
    private final String catalog;
    private final String schema;
    private final TerminalStyle style;

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout) {
        this(client, reader, out, queryTimeout, null);
    }

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout,
             String catalog) {
        this(client, reader, out, queryTimeout, catalog, null);
    }

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout,
             String catalog, String schema) {
        this(client, reader, out, queryTimeout, catalog, schema, TerminalStyle.plain());
    }

    SqlShell(QueryClient client, BufferedReader reader, PrintStream out, Duration queryTimeout,
             String catalog, String schema, TerminalStyle style) {
        this.client = client;
        this.reader = reader;
        this.out = out;
        this.queryTimeout = queryTimeout;
        this.catalog = catalog;
        this.schema = schema;
        this.style = style;
    }

    /**
     * Starts the REPL loop. Blocks until the user types {@code \q} or {@code exit},
     * or the input stream is exhausted.
     */
    void run() {
        printWelcome(out);

        StringBuilder sqlBuffer = new StringBuilder();
        boolean multiLine = false;

        while (true) {
            String line = readLine(multiLine);
            if (line == null) {
                break;
            }

            String trimmed = line.trim();

            if (!multiLine && isQuitCommand(trimmed)) {
                out.println(style.dim("Bye!"));
                break;
            }

            if (trimmed.isEmpty() && !multiLine) {
                continue;
            }

            multiLine = accumulateAndExecute(sqlBuffer, line, trimmed);
        }
    }

    private void printWelcome(PrintStream output) {
        if (style.width() < COMPACT_BANNER_WIDTH) {
            output.println(style.header("PANGROVA CLI") + style.dim("  ·  Distributed SQL for open lakehouses"));
        } else {
            output.print(style.accent(FULL_BANNER));
            output.println(style.bold("Distributed SQL for open lakehouses"));
        }
        output.println(style.dim("Open-source lakehouse analytics and AI retrieval."));
        output.println(style.dim("Calcite planning · Velox execution · Arrow Flight · Iceberg · Paimon · Lance"));
        if (catalog != null || schema != null) {
            output.println(style.dim("Session · catalog=" + valueOrDash(catalog) + " · schema=" + valueOrDash(schema)));
        }
        output.println();
        output.println(style.bold("Pangrova SQL Shell") + ". Type \\q or exit to quit.");
        output.println(style.dim("End statements with ';'. Results are paged in groups of up to 128 rows."));
        output.println();
    }

    private static String valueOrDash(String value) {
        return value == null ? "—" : value;
    }

    private String readLine(boolean multiLine) {
        out.print(multiLine ? style.dim(CONTINUATION_PROMPT) : style.prompt(PROMPT));
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
            out.println(style.error("Error [QUERY_TIMEOUT]: " + e.getMessage()));
        } catch (PangrovaClientException e) {
            out.println(style.error("Error [" + e.errorCode() + "]: " + e.getMessage()));
        } catch (Exception e) {
            out.println(style.error("Error: " + e.getMessage()));
            LOG.debug("Unexpected error during query execution", e);
        }
    }

    /** Executes one server-parsed statement; failures propagate to batch callers. */
    String execute(String sql) {
        if (sql == null || sql.isBlank()) throw new IllegalArgumentException("SQL must not be blank");
        Map<String, String> sessionProps = new java.util.HashMap<>();
        if (catalog != null) sessionProps.put("catalog", catalog);
        if (schema != null) sessionProps.put("schema", schema);
        QueryExecution query = client.beginQuery(sql, Map.copyOf(sessionProps));
        String queryId = query.submission().queryId();
        out.println(style.dim("Query submitted: " + queryId));
        consumeAndComplete(query);
        if (out.checkError()) throw new IllegalStateException("Failed to write query results");
        return queryId;
    }

    private void consumeAndComplete(QueryExecution query) {
        SubmitQueryResponse submitResp = query.submission();
        String queryId = submitResp.queryId();
        long deadlineMs = Math.addExact(System.currentTimeMillis(), queryTimeout.toMillis());
        boolean success = false;
        try {
            QueryStatusResponse ready = readableSubmission(submitResp);
            if (ready == null) ready = waitForStatus(query, deadlineMs, true);
            ResultDisplay result = null;
            if (ready.resultDisposition() == ResultDisposition.STREAM && !isFailedOrCancelled(ready.state())) {
                result = consumeResult(query, ready, deadlineMs);
                checkDeadline(queryId, deadlineMs);
            }
            QueryStatusResponse terminal = isTerminalState(ready.state())
                    ? ready : waitForStatus(query, deadlineMs, false);
            handleQueryResult(terminal, result);
            success = "FINISHED".equalsIgnoreCase(terminal.state());
        } catch (PangrovaClientException failure) {
            if ("TRANSPORT_QUERY_DEADLINE_EXCEEDED".equals(failure.errorCode())
                    || "QUERY_STATUS_DEADLINE_EXCEEDED".equals(failure.errorCode())) {
                var timeout = new QueryTimeoutException(queryId, queryTimeout);
                timeout.initCause(failure);
                throw timeout;
            }
            if ("CONNECTION_INTERRUPTED".equals(failure.errorCode())) {
                throw new PangrovaClientException("INTERRUPTED", "Query " + queryId + " interrupted", failure);
            }
            throw failure;
        } finally {
            if (!success) bestEffortCancel(query);
        }
    }

    private static QueryStatusResponse readableSubmission(SubmitQueryResponse receipt) {
        boolean streamReady = receipt.resultDisposition() == ResultDisposition.STREAM
                && (readable(receipt.state()) || "FINISHED".equalsIgnoreCase(receipt.state()));
        boolean noResultReady = receipt.resultDisposition() == ResultDisposition.NONE
                && "FINISHED".equalsIgnoreCase(receipt.state());
        if (streamReady || noResultReady) {
            return QueryStatusResponse.of(receipt.queryId(), receipt.state(), null,
                    receipt.resultDisposition(), receipt.resultEndpoint());
        }
        return null;
    }

    private void handleQueryResult(QueryStatusResponse status, ResultDisplay result) {
        if (isFailedOrCancelled(status.state())) {
            throw queryFailure(status, null);
        }
        if (result != null) {
            result.finish();
        } else if (status.resultDisposition() == ResultDisposition.NONE) {
            out.println(style.success("OK (no result set)"));
        } else {
            throw new PangrovaClientException("RESULT_ENDPOINT_MISSING", "Query finished without its result endpoint");
        }
    }

    private ResultDisplay consumeResult(QueryExecution query, QueryStatusResponse ready, long deadlineMs) {
        try (ResultCursor cursor = client.openResult(ready.resultEndpoint(), deadlineMs)) {
            var display = new ResultDisplay(cursor.columns().stream().map(ColumnDescriptor::name).toList(), out, style);
            while (cursor.next()) {
                display.add(cursor);
            }
            return display;
        } catch (TransportException failure) {
            throw resultFailure(query, ready, deadlineMs,
                    new PangrovaClientException(failure.transportErrorCode(), failure.getMessage(), failure));
        } catch (PangrovaClientException failure) {
            throw resultFailure(query, ready, deadlineMs, failure);
        }
    }

    private PangrovaClientException resultFailure(QueryExecution query, QueryStatusResponse ready, long deadlineMs,
            PangrovaClientException failure) {
        if (!"TRANSPORT_UNAUTHORIZED".equals(failure.errorCode()) || isTerminalState(ready.state())) {
            return failure;
        }
        // A failed task can release Flight authority before the Coordinator publishes its terminal state.
        QueryStatusResponse status = readStatusAfterRevocation(query, deadlineMs, failure);
        if (status != null && isFailedOrCancelled(status.state())) {
            return queryFailure(status, failure);
        }
        return failure;
    }

    private QueryStatusResponse readStatusAfterRevocation(
            QueryExecution query, long deadlineMs, PangrovaClientException transportFailure) {
        if (Thread.currentThread().isInterrupted() || System.currentTimeMillis() >= deadlineMs) {
            return null;
        }
        try {
            long probeDeadline = Math.min(deadlineMs, System.currentTimeMillis() + REVOCATION_STATUS_BUDGET_MS);
            QueryStatusResponse status = query.awaitStatus(QueryWaitTarget.TERMINAL, probeDeadline);
            if (!isTerminalState(status.state()) && System.currentTimeMillis() < probeDeadline) {
                return query.awaitStatus(QueryWaitTarget.TERMINAL, probeDeadline);
            }
            return status;
        } catch (PangrovaClientException failure) {
            transportFailure.addSuppressed(failure);
            return null;
        }
    }

    private static PangrovaClientException queryFailure(QueryStatusResponse status, Throwable cause) {
        if ("CANCELLED".equalsIgnoreCase(status.state())) {
            return new PangrovaClientException("QUERY_CANCELLED", "Query cancelled", cause);
        }
        var error = status.error();
        return new PangrovaClientException(error == null ? "QUERY_FAILED" : error.errorCode(),
                error == null ? "Query failed without error detail" : error.message(), cause);
    }

    private QueryStatusResponse waitForStatus(QueryExecution query, long deadlineMs, boolean allowReadable) {
        String queryId = query.submission().queryId();
        var target = allowReadable ? QueryWaitTarget.RESULT_OR_TERMINAL : QueryWaitTarget.TERMINAL;
        while (true) {
            checkDeadline(queryId, deadlineMs);
            QueryStatusResponse status = query.awaitStatus(target, deadlineMs);
            String state = status.state();
            if (isTerminalState(state) || allowReadable
                    && status.resultDisposition() == ResultDisposition.STREAM && readable(state)) {
                return status;
            }
        }
    }

    private static boolean readable(String state) {
        return "RUNNING".equalsIgnoreCase(state) || "FINISHING".equalsIgnoreCase(state);
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

    private void bestEffortCancel(QueryExecution query) {
        boolean interrupted = Thread.interrupted();
        try {
            query.cancel();
        } catch (Exception failure) {
            LOG.warn("Failed to cancel query {}", query.submission().queryId(), failure);
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    static final class ResultDisplay {
        private final List<String> headers;
        private final PrintStream output;
        private final TerminalStyle style;
        private final List<List<String>> page = new ArrayList<>(DISPLAY_PAGE_ROWS);
        private long rowCount;

        ResultDisplay(List<String> headers, PrintStream output) {
            this(headers, output, TerminalStyle.plain());
        }

        ResultDisplay(List<String> headers, PrintStream output, TerminalStyle style) {
            this.headers = headers;
            this.output = output;
            this.style = style;
        }

        void add(ResultCursor cursor) {
            if (page.size() == DISPLAY_PAGE_ROWS) {
                printPage();
                page.clear();
            }
            var row = new ArrayList<String>(headers.size());
            for (int column = 0; column < headers.size(); column++) {
                row.add(TableFormatter.formatCell(cursor.value(column)));
            }
            page.add(row);
            rowCount++;
        }

        void finish() {
            printPage();
            if (!headers.isEmpty()) {
                output.println(style.dim(rowCount + (rowCount == 1 ? " row" : " rows")));
            }
        }

        private void printPage() {
            output.print(TableFormatter.formatPage(headers, page, style));
            if (output.checkError()) throw new IllegalStateException("Failed to write query results");
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
