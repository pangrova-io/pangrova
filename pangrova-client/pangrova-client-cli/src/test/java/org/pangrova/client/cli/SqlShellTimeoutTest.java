package org.pangrova.client.cli;

import org.pangrova.client.cli.exception.QueryTimeoutException;
import org.pangrova.client.http.QueryClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.ResultCursor;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.common.enums.QueryWaitTarget;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that SqlShell query polling respects timeout and interrupt semantics.
 * Covers F-COMMON-11: CLI query polling has no timeout and no interrupt-to-cancel mechanism.
 */
class SqlShellTimeoutTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void revokedFlightAuthorityReportsOnlyConfirmedQueryFailure(boolean queryFails) {
        String queryId = "q-flight-revoked";
        var endpoint = new ResultEndpoint("localhost", 1234, "ticket", "grpc+tls");
        var running = QueryStatusResponse.of(
                queryId, "RUNNING", null, ResultDisposition.STREAM, endpoint);
        var failed = QueryStatusResponse.of(queryId, "FAILED",
                new ErrorDetail("AI_PROVIDER_UNAVAILABLE", "RUNTIME_ERROR", "Reranking provider returned HTTP 502"),
                ResultDisposition.STREAM, endpoint);
        var statusReads = new AtomicInteger();
        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> properties) {
                return new SubmitQueryResponse(queryId, "RUNNING", ResultDisposition.PENDING, null, List.of("result"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String id) {
                assertThat(id).isEqualTo(queryId);
                return statusReads.incrementAndGet() == 3 && queryFails ? failed : running;
            }

            @Override
            public ResultCursor openResult(ResultEndpoint result, long deadlineEpochMs) {
                assertThat(result).isEqualTo(endpoint);
                throw new PangrovaClientException("TRANSPORT_UNAUTHORIZED", "Flight authority was revoked");
            }

            @Override
            public CancelOutcome cancelQuery(String id) {
                return CancelOutcome.CANCEL_ACCEPTED;
            }
        };
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(client, new BufferedReader(new StringReader("SELECT 1;\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(5));

        shell.run();

        assertThat(output.toString(StandardCharsets.UTF_8)).contains(queryFails
                ? "Error [AI_PROVIDER_UNAVAILABLE]: Reranking provider returned HTTP 502"
                : "Error [TRANSPORT_UNAUTHORIZED]: Flight authority was revoked");
        assertThat(statusReads.get()).isEqualTo(3);
    }

    @Test
    void startupShowsBrandAndProjectIntroduction() {
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(new TestQueryClient() {}, new BufferedReader(new StringReader("\\q\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(1));

        shell.run();

        String rendered = output.toString(StandardCharsets.UTF_8);
        var banner = rendered.lines().limit(6).toList();
        // A P has an open lower-right side; the former D closed at the bottom.
        assertThat(banner).hasSize(6);
        assertThat(banner.get(2)).startsWith("██████╔╝");
        assertThat(banner.get(4)).startsWith("██║      ").endsWith("██║  ██║");
        assertThat(banner.get(5)).startsWith("╚═╝      ").endsWith("╚═╝  ╚═╝");
        assertThat(banner).allSatisfy(line -> assertThat(line.length()).isLessThan(80));
        assertThat(rendered).contains(
                "██████╗",
                "Distributed SQL for open lakehouses",
                "Open-source lakehouse analytics and AI retrieval.",
                "Pangrova SQL Shell. Type \\q or exit to quit.",
                "Bye!");
    }

    @Test
    void narrowTerminalUsesCompactBrand() {
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(new TestQueryClient() {}, new BufferedReader(new StringReader("\\q\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(1),
                null, null, new TerminalStyle(false, 60));

        shell.run();

        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("PANGROVA CLI", "Distributed SQL for open lakehouses")
                .doesNotContain("██████╗");
    }

    @Test
    void colorTerminalEmitsAnsiPresentation() {
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(new TestQueryClient() {}, new BufferedReader(new StringReader("\\q\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(1),
                null, null, new TerminalStyle(true, 100));

        shell.run();

        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("\u001B[38;5;141m", "\u001B[2m", "\u001B[0m");
    }

    @Test
    void queryTimesOutAndSendsCancelRequest() {
        // Arrange: a mock client that never returns a terminal state
        AtomicBoolean cancelCalled = new AtomicBoolean(false);
        AtomicReference<String> cancelledQueryId = new AtomicReference<>();

        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-timeout-test", "RUNNING",
                        ResultDisposition.PENDING, null, List.of("col1"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return new QueryStatusResponse(queryId, "RUNNING", null, null, ResultDisposition.PENDING, null);
            }

            @Override
            public CancelOutcome cancelQuery(String queryId) {
                cancelCalled.set(true);
                cancelledQueryId.set(queryId);
                return CancelOutcome.CANCEL_ACCEPTED;
            }
        };

        // Short timeout of 1 second for test
        Duration shortTimeout = Duration.ofSeconds(1);
        String input = "SELECT 1;\n\\q\n";
        BufferedReader reader = new BufferedReader(new StringReader(input));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos, true, StandardCharsets.UTF_8);

        SqlShell shell = new SqlShell(client, reader, out, shortTimeout);

        // Act
        shell.run();
        String output = baos.toString(StandardCharsets.UTF_8);

        // Assert
        assertThat(cancelCalled.get()).isTrue();
        assertThat(cancelledQueryId.get()).isEqualTo("q-timeout-test");
        assertThat(output).contains("QUERY_TIMEOUT");
        assertThat(output).contains("timed out");
    }

    @Test
    void interruptSendsCancelRequestAndReportsError() throws Exception {
        // Arrange: a mock client that blocks forever, simulating a stuck query
        CountDownLatch pollStarted = new CountDownLatch(1);
        AtomicBoolean cancelCalled = new AtomicBoolean(false);

        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-interrupt-test", "RUNNING",
                        ResultDisposition.PENDING, null, List.of("col1"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                pollStarted.countDown();
                return new QueryStatusResponse(queryId, "RUNNING", null, null, ResultDisposition.PENDING, null);
            }

            @Override
            public CancelOutcome cancelQuery(String queryId) {
                cancelCalled.set(true);
                return CancelOutcome.CANCEL_ACCEPTED;
            }
        };

        // Use a long timeout so interrupt fires before timeout
        Duration longTimeout = Duration.ofMinutes(30);
        String input = "SELECT 1;\n\\q\n";
        BufferedReader reader = new BufferedReader(new StringReader(input));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos, true, StandardCharsets.UTF_8);

        SqlShell shell = new SqlShell(client, reader, out, longTimeout);

        // Act: run in a separate thread and interrupt it
        Thread shellThread = new Thread(shell::run, "shell-interrupt-test");
        shellThread.start();

        // Wait for at least one poll to happen, then interrupt
        assertThat(pollStarted.await(5, TimeUnit.SECONDS)).isTrue();
        shellThread.interrupt();
        shellThread.join(5000);

        String output = baos.toString(StandardCharsets.UTF_8);

        // Assert
        assertThat(cancelCalled.get()).isTrue();
        assertThat(output).contains("INTERRUPTED");
        assertThat(output).contains("q-interrupt-test interrupted");
    }

    @Test
    void normalQueryCompletesWithoutTimeout() {
        // Arrange: a client that returns FINISHED immediately
        AtomicBoolean cancelCalled = new AtomicBoolean(false);

        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-normal", "FINISHED", ResultDisposition.NONE, null, List.of());
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return new QueryStatusResponse(queryId, "FINISHED", null, null, ResultDisposition.NONE, null);
            }

            @Override
            public CancelOutcome cancelQuery(String queryId) {
                cancelCalled.set(true);
                return CancelOutcome.CANCEL_ACCEPTED;
            }
        };

        Duration shortTimeout = Duration.ofSeconds(2);
        String input = "SELECT 1;\n\\q\n";
        BufferedReader reader = new BufferedReader(new StringReader(input));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos, true, StandardCharsets.UTF_8);

        SqlShell shell = new SqlShell(client, reader, out, shortTimeout);

        // Act
        shell.run();
        String output = baos.toString(StandardCharsets.UTF_8);

        // Assert: no timeout error, query completed normally
        assertThat(cancelCalled.get()).isFalse();
        assertThat(output).doesNotContain("QUERY_TIMEOUT");
        assertThat(output).doesNotContain("INTERRUPTED");
        assertThat(output).contains("OK (no result set)");
    }

    @Test
    void cancelFailureDoesNotMaskTimeoutError() {
        // Arrange: cancel request itself throws, but timeout error should still be reported
        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-cancel-fail", "RUNNING",
                        ResultDisposition.PENDING, null, List.of("col1"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return new QueryStatusResponse(queryId, "RUNNING", null, null, ResultDisposition.PENDING, null);
            }

            @Override
            public CancelOutcome cancelQuery(String queryId) {
                throw new PangrovaClientException("CONNECTION_REFUSED", "Connection refused");
            }
        };

        Duration shortTimeout = Duration.ofSeconds(1);
        String input = "SELECT 1;\n\\q\n";
        BufferedReader reader = new BufferedReader(new StringReader(input));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos, true, StandardCharsets.UTF_8);

        SqlShell shell = new SqlShell(client, reader, out, shortTimeout);

        // Act
        shell.run();
        String output = baos.toString(StandardCharsets.UTF_8);

        // Assert: timeout is still reported even though cancel failed
        assertThat(output).contains("QUERY_TIMEOUT");
        assertThat(output).contains("timed out");
    }

    @Test
    void queryTransitionsToFinishedBeforeTimeout() {
        // Arrange: query starts as RUNNING then transitions to FINISHED after a few polls
        var pollCount = new java.util.concurrent.atomic.AtomicInteger(0);

        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-transitions", "RUNNING", ResultDisposition.PENDING, null, List.of());
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                if (pollCount.incrementAndGet() >= 3) {
                    return new QueryStatusResponse(queryId, "FINISHED", null, null, ResultDisposition.NONE, null);
                }
                return new QueryStatusResponse(queryId, "RUNNING", null, null, ResultDisposition.PENDING, null);
            }

            @Override
            public CancelOutcome cancelQuery(String queryId) {
                throw new AssertionError("Cancel should not be called");
            }
        };

        Duration timeout = Duration.ofSeconds(10);
        String input = "SELECT 1;\n\\q\n";
        BufferedReader reader = new BufferedReader(new StringReader(input));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos, true, StandardCharsets.UTF_8);

        SqlShell shell = new SqlShell(client, reader, out, timeout);

        // Act
        shell.run();
        String output = baos.toString(StandardCharsets.UTF_8);

        assertThat(output).contains("Query submitted: q-transitions", "OK (no result set)");
        assertThat(output).doesNotContain("QUERY_TIMEOUT");
    }

    @Test
    void emptySelectStillConsumesAStream() {
        var endpoint = new ResultEndpoint("worker", 1234, "empty-select", "grpc");
        var opened = new AtomicBoolean();
        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> properties) {
                return new SubmitQueryResponse("q-empty", "FINISHED",
                        ResultDisposition.STREAM, endpoint, List.of("value"));
            }

            @Override
            public ResultCursor openResult(ResultEndpoint result, long deadlineEpochMs) {
                assertThat(result).isEqualTo(endpoint);
                opened.set(true);
                return emptyCursor();
            }
        };
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(client, new BufferedReader(new StringReader("SELECT 1 WHERE false;\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(2));

        shell.run();

        assertThat(opened).isTrue();
        assertThat(output.toString(StandardCharsets.UTF_8)).contains("0 rows")
                .doesNotContain("OK (no result set)");
    }

    @Test
    void delayedEndpointPublicationIsConsumedWithoutTreatingItAsNoResult() {
        var endpoint = new ResultEndpoint("worker", 1234, "delayed", "grpc");
        var reads = new AtomicInteger();
        var opened = new AtomicBoolean();
        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> properties) {
                return new SubmitQueryResponse("q-delayed", "RUNNING",
                        ResultDisposition.PENDING, null, List.of("value"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                int read = reads.incrementAndGet();
                if (read == 1) {
                    return QueryStatusResponse.of(
                            queryId, "RUNNING", null, ResultDisposition.PENDING, null);
                }
                if (read == 2) {
                    return QueryStatusResponse.of(
                            queryId, "RUNNING", null, ResultDisposition.STREAM, endpoint);
                }
                return QueryStatusResponse.of(
                        queryId, "FINISHED", null, ResultDisposition.STREAM, endpoint);
            }

            @Override
            public ResultCursor openResult(ResultEndpoint result, long deadlineEpochMs) {
                assertThat(result).isEqualTo(endpoint);
                opened.set(true);
                return emptyCursor();
            }
        };
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(client, new BufferedReader(new StringReader("SELECT 1;\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(2));

        shell.run();

        assertThat(opened).isTrue();
        assertThat(output.toString(StandardCharsets.UTF_8)).contains("0 rows")
                .doesNotContain("OK (no result set)");
    }

    @Test
    void cancellationBeforeEndpointPublicationIsTerminalNoResult() {
        var cancellations = new AtomicInteger();
        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> properties) {
                return new SubmitQueryResponse("q-cancelled", "RUNNING",
                        ResultDisposition.PENDING, null, List.of("value"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return QueryStatusResponse.of(
                        queryId, "CANCELLED", null, ResultDisposition.NONE, null);
            }

            @Override
            public CancelOutcome cancelQuery(String queryId) {
                cancellations.incrementAndGet();
                return CancelOutcome.CANCEL_ACCEPTED;
            }

            @Override
            public ResultCursor openResult(ResultEndpoint endpoint, long deadlineEpochMs) {
                throw new AssertionError("A cancelled resultless query must not open Flight");
            }
        };
        var output = new ByteArrayOutputStream();
        var shell = new SqlShell(client, new BufferedReader(new StringReader("SELECT 1;\n")),
                new PrintStream(output, true, StandardCharsets.UTF_8), Duration.ofSeconds(2));

        shell.run();

        assertThat(output.toString(StandardCharsets.UTF_8)).contains("QUERY_CANCELLED")
                .doesNotContain("RESULT_ENDPOINT_MISSING");
        assertThat(cancellations).hasValue(1);
    }

    private static ResultCursor emptyCursor() {
        return new ResultCursor() {
            @Override
            public List<ColumnDescriptor> columns() {
                return List.of(new ColumnDescriptor("value", Types.INTEGER, "INTEGER", 10, 0, Optional.empty()));
            }

            @Override
            public boolean next() {
                return false;
            }

            @Override
            public Object value(int column) {
                throw new AssertionError("No rows");
            }

            @Override
            public void close() { }
        };
    }

    @Test
    void timeoutMessageIncludesFormattedDuration() {
        QueryTimeoutException ex = new QueryTimeoutException("q-123", Duration.ofMinutes(30));
        assertThat(ex.getMessage()).contains("30m");
        assertThat(ex.getMessage()).contains("q-123");
        assertThat(ex.queryId()).isEqualTo("q-123");
        assertThat(ex.timeout()).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void timeoutMessageFormatsSecondsOnly() {
        QueryTimeoutException ex = new QueryTimeoutException("q-456", Duration.ofSeconds(45));
        assertThat(ex.getMessage()).contains("45s");
    }

    @Test
    void timeoutMessageFormatsMinutesAndSeconds() {
        QueryTimeoutException ex = new QueryTimeoutException("q-789", Duration.ofSeconds(95));
        assertThat(ex.getMessage()).contains("1m 35s");
    }

    /**
     * Base test implementation of QueryClient. Methods throw by default;
     * tests override the methods they need.
     */
    private abstract static class TestQueryClient implements QueryClient {
        @Override
        public QueryExecution beginQuery(String sql, Map<String, String> sessionProperties) {
            SubmitQueryResponse receipt = submitQuery(sql, sessionProperties);
            return new QueryExecution() {
                @Override public SubmitQueryResponse submission() { return receipt; }
                @Override public QueryStatusResponse awaitStatus(QueryWaitTarget target, long deadline) {
                    return getQueryStatus(receipt.queryId());
                }
                @Override public CancelOutcome cancel() { return cancelQuery(receipt.queryId()); }
            };
        }

        public SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties) {
            throw new UnsupportedOperationException("Not stubbed");
        }

        public QueryStatusResponse getQueryStatus(String queryId) {
            throw new UnsupportedOperationException("Not stubbed");
        }

        public CancelOutcome cancelQuery(String queryId) {
            throw new UnsupportedOperationException("Not stubbed");
        }

        @Override
        public ResultCursor openResult(ResultEndpoint endpoint, long deadlineEpochMs) {
            throw new UnsupportedOperationException("This polling-only test does not execute a result stream");
        }
    }
}
