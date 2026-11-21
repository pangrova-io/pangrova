package org.pangrova.client.cli;

import org.pangrova.client.cli.exception.QueryTimeoutException;
import org.pangrova.common.client.query.QueryClient;
import org.pangrova.common.client.enums.CancelOutcome;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.client.query.SubmitQueryResponse;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that SqlShell query polling respects timeout and interrupt semantics.
 * Covers F-COMMON-11: CLI query polling has no timeout and no interrupt-to-cancel mechanism.
 */
class SqlShellTimeoutTest {

    @Test
    void queryTimesOutAndSendsCancelRequest() {
        // Arrange: a mock client that never returns a terminal state
        AtomicBoolean cancelCalled = new AtomicBoolean(false);
        AtomicReference<String> cancelledQueryId = new AtomicReference<>();

        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-timeout-test", "RUNNING", null, List.of("col1"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return new QueryStatusResponse(queryId, "RUNNING", null, null, null);
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
                return new SubmitQueryResponse("q-interrupt-test", "RUNNING", null, List.of("col1"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                pollStarted.countDown();
                return new QueryStatusResponse(queryId, "RUNNING", null, null, null);
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
        assertThat(output).contains("cancel request sent");
    }

    @Test
    void normalQueryCompletesWithoutTimeout() {
        // Arrange: a client that returns FINISHED immediately
        AtomicBoolean cancelCalled = new AtomicBoolean(false);

        QueryClient client = new TestQueryClient() {
            @Override
            public SubmitQueryResponse submitQuery(String sql, Map<String, String> props) {
                return new SubmitQueryResponse("q-normal", "FINISHED", null, List.of());
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return new QueryStatusResponse(queryId, "FINISHED", null, null, null);
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
                return new SubmitQueryResponse("q-cancel-fail", "RUNNING", null, List.of("col1"));
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                return new QueryStatusResponse(queryId, "RUNNING", null, null, null);
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
                return new SubmitQueryResponse("q-transitions", "RUNNING", null, List.of());
            }

            @Override
            public QueryStatusResponse getQueryStatus(String queryId) {
                if (pollCount.incrementAndGet() >= 3) {
                    return new QueryStatusResponse(queryId, "FINISHED", null, null, null);
                }
                return new QueryStatusResponse(queryId, "RUNNING", null, null, null);
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

        // Assert: progress dots were printed, then finished normally
        assertThat(output).contains("..");  // at least 2 dots
        assertThat(output).contains("OK (no result set)");
        assertThat(output).doesNotContain("QUERY_TIMEOUT");
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
        public SubmitQueryResponse submitQuery(String sql, Map<String, String> sessionProperties) {
            throw new UnsupportedOperationException("Not stubbed");
        }

        @Override
        public QueryStatusResponse getQueryStatus(String queryId) {
            throw new UnsupportedOperationException("Not stubbed");
        }

        @Override
        public CancelOutcome cancelQuery(String queryId) {
            throw new UnsupportedOperationException("Not stubbed");
        }

        @Override
        public byte[] fetchResult(ResultEndpoint endpoint, long deadlineEpochMs) {
            throw new UnsupportedOperationException("This polling-only test does not execute a result stream");
        }
    }
}
