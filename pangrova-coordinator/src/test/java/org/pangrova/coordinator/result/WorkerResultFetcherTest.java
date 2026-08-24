package org.pangrova.coordinator.result;

import org.pangrova.client.rest.PangrovaConnectionException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link WorkerResultFetcher} — coordinator-side result fetch
 * from worker's native result server.
 */
class WorkerResultFetcherTest {

    private static final ResultEndpoint TRANSPORT_ENDPOINT =
            new ResultEndpoint("localhost", 9091, "unused", "grpc");

    private ScheduledExecutorService executor;
    private CoordinatorResultBuffer buffer;
    private CoordinatorResultWriter writer;
    private CoordinatorResultService resultService;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        buffer = new CoordinatorResultBuffer(executor, 60_000L);
        writer = new CoordinatorResultWriter(new RootAllocator());
        resultService = new CoordinatorResultService(buffer, writer, TRANSPORT_ENDPOINT);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void fetchAndDispatchBuffersResultAndReturnsLocalEndpoint() {
        byte[] expectedBytes = {10, 20, 30, 40};
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> expectedBytes, () -> {});

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorResultService.LocalDispatchResult result = fetcher.fetchAndDispatch(
                "worker-host-1", 9092, "task-1", "query-1",
                resultService, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(result.workerHost()).isEqualTo("coordinator-local");
        assertThat(result.taskId()).isEqualTo("query-1");
        assertThat(result.resultEndpoint().host()).isEqualTo("localhost");
        assertThat(result.resultEndpoint().port()).isEqualTo(9091);
        assertThat(result.resultEndpoint().ticket()).isEqualTo("query-1");
        assertThat(buffer.get("query-1")).isEqualTo(expectedBytes);
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void fetchAndDispatchTransitionsStateMachineToFinished() {
        byte[] bytes = {1, 2, 3};
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> bytes, () -> {});

        QueryStateMachine sm = new QueryStateMachine();
        fetcher.fetchAndDispatch("host", 9092, "t1", "q1", resultService, sm.toFinisher());

        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
        assertThat(sm.isTerminal()).isTrue();
    }

    @Test
    void fetchAndDispatchUsesTaskIdAsTicketForWorkerEndpoint() {
        AtomicReference<ResultEndpoint> captured = new AtomicReference<>();
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    captured.set(endpoint);
                    return new byte[]{1};
                }, () -> {});

        QueryStateMachine sm = new QueryStateMachine();
        fetcher.fetchAndDispatch("10.0.1.5", 9093, "velox-task-42", "q2",
                resultService, sm.toFinisher());

        ResultEndpoint ep = captured.get();
        assertThat(ep).isNotNull();
        assertThat(ep.host()).isEqualTo("10.0.1.5");
        assertThat(ep.port()).isEqualTo(9093);
        assertThat(ep.ticket()).isEqualTo("velox-task-42");
        assertThat(ep.protocol()).isEqualTo("grpc");
    }

    @Test
    void fetchAndDispatchHandlesEmptyResult() {
        byte[] emptyBytes = {};
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> emptyBytes, () -> {});

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorResultService.LocalDispatchResult result = fetcher.fetchAndDispatch(
                "host", 9092, "t1", "q-empty", resultService, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(buffer.get("q-empty")).isEqualTo(emptyBytes);
        assertThat(sm.getState()).isEqualTo(QueryStateMachine.State.FINISHED);
    }

    @Test
    void fetchAndDispatchUsesWorkerHostAndPortForEndpoint() {
        AtomicReference<ResultEndpoint> captured = new AtomicReference<>();
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    captured.set(endpoint);
                    return new byte[]{42};
                }, () -> {});

        QueryStateMachine sm = new QueryStateMachine();
        fetcher.fetchAndDispatch("192.168.1.100", 12345, "task-abc", "q3",
                resultService, sm.toFinisher());

        ResultEndpoint ep = captured.get();
        assertThat(ep.host()).isEqualTo("192.168.1.100");
        assertThat(ep.port()).isEqualTo(12345);
    }

    @Test
    void closeInvokesDelegate() {
        boolean[] closed = {false};
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> new byte[0], () -> closed[0] = true);

        fetcher.close();

        assertThat(closed[0]).isTrue();
    }

    @Test
    void fetchAndDispatchRetriesOnConnectionFailureThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger(0);
        byte[] expectedBytes = {1, 2, 3};
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    if (attempts.getAndIncrement() < 2) {
                        throw new PangrovaConnectionException(
                                "TRANSPORT_CONNECTION_FAILED", "Connection refused");
                    }
                    return expectedBytes;
                },
                () -> {},
                3, 100, 5000, millis -> {});

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorResultService.LocalDispatchResult result = fetcher.fetchAndDispatch(
                "host", 9092, "t1", "q-retry", resultService, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(buffer.get("q-retry")).isEqualTo(expectedBytes);
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void fetchAndDispatchThrowsAfterMaxRetriesExceeded() {
        AtomicInteger attempts = new AtomicInteger(0);
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    attempts.incrementAndGet();
                    throw new PangrovaConnectionException(
                            "TRANSPORT_CONNECTION_FAILED", "Connection refused");
                },
                () -> {},
                3, 100, 5000, millis -> {});

        QueryStateMachine sm = new QueryStateMachine();
        assertThatThrownBy(() -> fetcher.fetchAndDispatch(
                "host", 9092, "t1", "q-fail", resultService, sm.toFinisher()))
                .isInstanceOf(PangrovaException.class)
                .satisfies(ex -> {
                    PangrovaException ae = (PangrovaException) ex;
                    assertThat(ae.getErrorCode()).isEqualTo(PangrovaErrorCode.TRANSPORT_CONNECTION_FAILED);
                    assertThat(ae.getMessage()).contains("q-fail");
                    assertThat(ae.getMessage()).contains("3 retries");
                    assertThat(ae.getCause()).isInstanceOf(PangrovaConnectionException.class);
                });
        assertThat(attempts.get()).isEqualTo(4); // 1 initial + 3 retries
    }

    @Test
    void fetchAndDispatchAppliesExponentialBackoffDelays() {
        List<Long> delays = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger(0);
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    if (attempts.getAndIncrement() < 3) {
                        throw new PangrovaConnectionException(
                                "TRANSPORT_CONNECTION_FAILED", "Connection refused");
                    }
                    return new byte[]{1};
                },
                () -> {},
                3, 100, 5000, delays::add);

        QueryStateMachine sm = new QueryStateMachine();
        fetcher.fetchAndDispatch("host", 9092, "t1", "q-backoff", resultService, sm.toFinisher());

        assertThat(delays).hasSize(3);
        assertThat(delays.get(0)).isEqualTo(100);
        assertThat(delays.get(1)).isEqualTo(200);
        assertThat(delays.get(2)).isEqualTo(400);
    }

    @Test
    void fetchAndDispatchDoesNotRetryOnNonConnectionException() {
        AtomicInteger attempts = new AtomicInteger(0);
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException("Unexpected error");
                },
                () -> {},
                3, 100, 5000, millis -> {});

        QueryStateMachine sm = new QueryStateMachine();
        assertThatThrownBy(() -> fetcher.fetchAndDispatch(
                "host", 9092, "t1", "q-noretry", resultService, sm.toFinisher()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unexpected error");
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void fetchAndDispatchSucceedsOnFirstAttemptWithNoRetry() {
        List<Long> delays = new ArrayList<>();
        byte[] expectedBytes = {42};
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> expectedBytes,
                () -> {},
                3, 100, 5000, delays::add);

        QueryStateMachine sm = new QueryStateMachine();
        CoordinatorResultService.LocalDispatchResult result = fetcher.fetchAndDispatch(
                "host", 9092, "t1", "q-first", resultService, sm.toFinisher());

        assertThat(result).isNotNull();
        assertThat(delays).isEmpty();
    }

    @Test
    void fetchAndDispatchRespectsMaxDelayCapOnBackoff() {
        List<Long> delays = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger(0);
        WorkerResultFetcher fetcher = new WorkerResultFetcher(
                endpoint -> {
                    attempts.incrementAndGet();
                    throw new PangrovaConnectionException(
                            "TRANSPORT_CONNECTION_FAILED", "Connection refused");
                },
                () -> {},
                5, 1000, 2000, delays::add);

        QueryStateMachine sm = new QueryStateMachine();
        assertThatThrownBy(() -> fetcher.fetchAndDispatch(
                "host", 9092, "t1", "q-cap", resultService, sm.toFinisher()))
                .isInstanceOf(PangrovaException.class);

        // delays: 1000, 2000, 2000, 2000, 2000 (capped at maxDelay)
        assertThat(delays).hasSize(5);
        assertThat(delays.get(0)).isEqualTo(1000);
        assertThat(delays.get(1)).isEqualTo(2000);
        for (int i = 2; i < delays.size(); i++) {
            assertThat(delays.get(i)).isEqualTo(2000);
        }
    }
}
