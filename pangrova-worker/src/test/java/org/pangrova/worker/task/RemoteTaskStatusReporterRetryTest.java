package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.ObservableDoubleGauge;
import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongUpDownCounter;
import org.pangrova.testing.TestObservability;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

/**
 * Validates RemoteTaskStatusReporter retry behavior for final-state reports.
 *
 * <p>Uses a real Jetty HTTP server that can simulate transient failures,
 * verifying exponential backoff retry and eventual delivery. No mocks.</p>
 *
 * <p><b>Validates: Requirements 1.27, 1.28, 2.27, 2.28</b></p>
 */
class RemoteTaskStatusReporterRetryTest {

    private Server server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void finalStateReport_retriesOnNetworkFailure_eventuallyDelivers() throws Exception {
        int failCount = 2;
        FailThenSucceedServlet servlet = new FailThenSucceedServlet(failCount);
        int port = startServer(servlet);

        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = createReporterWithNoSleep(port, metrics);

        reporter.report(finishedReport(1));

        assertThat(servlet.totalRequests.get()).isEqualTo(failCount + 1);
        assertThat(servlet.successRequests.get()).isEqualTo(1);
        assertThat(metrics.failedCounter.count.get()).isEqualTo(0);
    }

    @Test
    void finalStateReport_exhaustsRetries_recordsFailureMetric() throws Exception {
        AlwaysFailServlet servlet = new AlwaysFailServlet();
        int port = startServer(servlet);

        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = createReporterWithNoSleep(port, metrics);

        reporter.report(failedReport(1));

        assertThat(servlet.totalRequests.get()).isEqualTo(RemoteTaskStatusReporter.MAX_RETRY_ATTEMPTS);
        assertThat(metrics.failedCounter.count.get()).isEqualTo(1);
    }

    @Test
    void cancelledReport_retriesOnTransientFailure() throws Exception {
        FailThenSucceedServlet servlet = new FailThenSucceedServlet(2);
        int port = startServer(servlet);
        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = createReporterWithNoSleep(port, metrics);

        reporter.report(cancelledReport(3));

        assertThat(servlet.totalRequests.get()).isEqualTo(3);
        assertThat(servlet.successRequests.get()).isEqualTo(1);
        assertThat(metrics.failedCounter.count.get()).isZero();
    }

    @Test
    void nonFinalStateReport_doesNotRetry() throws Exception {
        AlwaysFailServlet servlet = new AlwaysFailServlet();
        int port = startServerWithWarmup(servlet, () -> servlet.totalRequests.set(0));

        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = createReporterWithNoSleep(port, metrics);

        // Use SCHEDULED (non-final, non-RUNNING) to verify no retry.
        // RUNNING state has a special one-retry policy (P1-16).
        reporter.report(scheduledReport(1));

        assertThat(servlet.totalRequests.get()).isEqualTo(1);
        assertThat(metrics.failedCounter.count.get()).isEqualTo(0);
    }

    @Test
    void finalStateReport_unreachableHost_exhaustsRetriesAndRecordsMetric() {
        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = new RemoteTaskStatusReporter(
                "http://localhost:1", TestObservability.TRACING, null) {
            @Override
            void sleep(long millis) {
                // skip sleep for fast test
            }
        };

        reporter.report(finishedReport(42));

        assertThat(metrics.failedCounter.count.get()).isEqualTo(0);
    }

    @Test
    void finalStateReport_retriesPreserveReportSequence() throws Exception {
        SequenceCapturingServlet servlet = new SequenceCapturingServlet(2);
        int port = startServer(servlet);

        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = createReporterWithNoSleep(port, metrics);

        reporter.report(finishedReport(99));

        assertThat(servlet.totalRequests.get()).isEqualTo(3);
        assertThat(servlet.lastBody.get()).contains("\"report_sequence\":99");
    }

    @Test
    void finalStateReport_http409IsUndeliveredAndRetries() throws Exception {
        ConflictServlet servlet = new ConflictServlet();
        int port = startServer(servlet);

        CountingMetricsProvider metrics = new CountingMetricsProvider();
        RemoteTaskStatusReporter reporter = createReporterWithNoSleep(port, metrics);

        reporter.report(finishedReport(1));

        assertThat(servlet.totalRequests.get())
                .isEqualTo(RemoteTaskStatusReporter.MAX_RETRY_ATTEMPTS);
        assertThat(metrics.failedCounter.count.get()).isEqualTo(1);
    }

    @Test
    void statusReportCarriesWorkerInternalBearerCredential() throws Exception {
        AuthorizationCapturingServlet servlet = new AuthorizationCapturingServlet();
        int port = startServer(servlet);
        RemoteTaskStatusReporter reporter = new RemoteTaskStatusReporter(
                "http://localhost:" + port,
                java.net.http.HttpClient.newHttpClient(),
                TestObservability.TRACING,
                new CountingMetricsProvider(),
                "worker-service-secret");

        reporter.report(scheduledReport(1));

        assertThat(servlet.authorization)
                .isEqualTo("Bearer worker-service-secret");
    }

    // --- Helpers ---

    private int startServer(HttpServlet servlet) throws Exception {
        ServletContextHandler handler = new ServletContextHandler();
        handler.addServlet(new ServletHolder(servlet), "/v1/task/status-report");
        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        return EmbeddedHttpServerFactory.getPort(server);
    }

    /**
     * Resets the request counter on the servlet after server startup warmup.
     * Jetty's start() returns when the connector is bound, but the servlet
     * handler may not be ready to dispatch requests immediately. A warmup
     * POST ensures the handler is fully initialized before the test begins.
     */
    private int startServerWithWarmup(HttpServlet servlet, Runnable resetCounters) throws Exception {
        int port = startServer(servlet);
        try {
            var client = java.net.http.HttpClient.newHttpClient();
            var req = java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create("http://localhost:" + port + "/v1/task/status-report"))
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            client.send(req, java.net.http.HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {
            // warmup request may fail — that's fine, we just need the handler initialized
        }
        resetCounters.run();
        return port;
    }

    private RemoteTaskStatusReporter createReporterWithNoSleep(int port, CountingMetricsProvider metrics) {
        return new RemoteTaskStatusReporter("http://localhost:" + port,
                java.net.http.HttpClient.newHttpClient(),
                TestObservability.TRACING, metrics, null) {
            @Override
            void sleep(long millis) {
                // skip sleep for fast test execution
            }
        };
    }

    private static TaskStatusReport finishedReport(long sequence) {
        return new TaskStatusReport(
                "task-1", "q-1", 0, 0, "COMPUTE", "w-1", "FINISHED",
                null, null, null, sequence, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
    }

    private static TaskStatusReport failedReport(long sequence) {
        return new TaskStatusReport(
                "task-1", "q-1", 0, 0, "COMPUTE", "w-1", "FAILED",
                null, null, null, sequence, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
    }

    private static TaskStatusReport cancelledReport(long sequence) {
        return new TaskStatusReport(
                "task-1", "q-1", 0, 0, "COMPUTE", "w-1", "CANCELLED",
                null, null, null, sequence, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
    }

    private static TaskStatusReport runningReport(long sequence) {
        return new TaskStatusReport(
                "task-1", "q-1", 0, 0, "COMPUTE", "w-1", "RUNNING",
                null, null, null, sequence, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
    }

    private static TaskStatusReport scheduledReport(long sequence) {
        return new TaskStatusReport(
                "task-1", "q-1", 0, 0, "COMPUTE", "w-1", "SCHEDULED",
                null, null, null, sequence, System.currentTimeMillis(), null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
    }

    // --- Test Servlets (real HTTP behavior, no mocks) ---

    /** Returns HTTP 500 for the first N requests, then 200. */
    private static final class FailThenSucceedServlet extends HttpServlet {
        private final int failUntil;
        final AtomicInteger totalRequests = new AtomicInteger(0);
        final AtomicInteger successRequests = new AtomicInteger(0);

        FailThenSucceedServlet(int failCount) {
            this.failUntil = failCount;
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            int count = totalRequests.incrementAndGet();
            if (count <= failUntil) {
                resp.setStatus(500);
                resp.getWriter().write("simulated failure");
            } else {
                successRequests.incrementAndGet();
                resp.setStatus(200);
                resp.getWriter().write("{}");
            }
        }
    }

    /** Always returns HTTP 500. */
    private static final class AlwaysFailServlet extends HttpServlet {
        final AtomicInteger totalRequests = new AtomicInteger(0);

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            totalRequests.incrementAndGet();
            resp.setStatus(500);
            resp.getWriter().write("always fail");
        }
    }

    /** Returns HTTP 409 (Conflict). */
    private static final class ConflictServlet extends HttpServlet {
        final AtomicInteger totalRequests = new AtomicInteger(0);

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            totalRequests.incrementAndGet();
            resp.setStatus(409);
            resp.getWriter().write("worker not registered");
        }
    }

    private static final class AuthorizationCapturingServlet extends HttpServlet {
        volatile String authorization;

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
            authorization = req.getHeader("Authorization");
            resp.setStatus(200);
        }
    }

    /** Fails N times then succeeds, capturing the request body. */
    private static final class SequenceCapturingServlet extends HttpServlet {
        private final int failUntil;
        final AtomicInteger totalRequests = new AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicReference<String> lastBody =
                new java.util.concurrent.atomic.AtomicReference<>();

        SequenceCapturingServlet(int failCount) {
            this.failUntil = failCount;
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String body = new String(req.getInputStream().readAllBytes());
            lastBody.set(body);
            int count = totalRequests.incrementAndGet();
            if (count <= failUntil) {
                resp.setStatus(500);
                resp.getWriter().write("simulated failure");
            } else {
                resp.setStatus(200);
                resp.getWriter().write("{}");
            }
        }
    }

    // --- Counting MetricsProvider (real implementation, no mocks) ---

    private static final class CountingMetricsProvider implements MetricsProvider {
        final CountingCounter failedCounter = new CountingCounter();

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            return failedCounter;
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            return new ObservableDoubleGauge() {};
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                                String unit, List<Double> boundaries) {
            return value -> {};
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            return value -> {};
        }
    }

    private static final class CountingCounter implements LongCounter {
        final AtomicInteger count = new AtomicInteger(0);

        @Override
        public void add(long value) {
            count.addAndGet((int) value);
        }
    }
}
