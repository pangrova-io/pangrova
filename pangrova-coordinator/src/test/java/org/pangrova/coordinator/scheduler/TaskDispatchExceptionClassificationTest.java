package org.pangrova.coordinator.scheduler;

import org.pangrova.coordinator.scheduler.failure.TaskDispatchException;

import org.pangrova.coordinator.scheduler.failure.ErrorCategory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests exception classification logic in RemoteTaskDispatcher.classifyAndWrap.
 * Verifies that each exception type maps to the correct ErrorCategory.
 */
class TaskDispatchExceptionClassificationTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();
    private static final String WORKER_HOST = "worker-1";

    private final RemoteTaskDispatcher dispatcher =
            new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);

    @Test
    void connectException_classifiedAsNetwork() {
        ConnectException cause = new ConnectException("Connection refused");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.NETWORK);
        assertThat(result.getWorkerHost()).isEqualTo(WORKER_HOST);
        assertThat(result.getCause()).isSameAs(cause);
    }

    @Test
    void ioException_classifiedAsNetwork() {
        IOException cause = new IOException("Broken pipe");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.NETWORK);
        assertThat(result.getWorkerHost()).isEqualTo(WORKER_HOST);
    }

    @Test
    void httpTimeoutException_classifiedAsRpcTimeout() {
        HttpTimeoutException cause = new HttpTimeoutException("request timed out");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.RPC_TIMEOUT);
        assertThat(result.getWorkerHost()).isEqualTo(WORKER_HOST);
    }

    @Test
    void httpConnectTimeoutException_classifiedAsRpcTimeout() {
        HttpConnectTimeoutException cause = new HttpConnectTimeoutException("connect timed out");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.RPC_TIMEOUT);
    }

    @Test
    void timeoutException_classifiedAsRpcTimeout() {
        TimeoutException cause = new TimeoutException("future timed out");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.RPC_TIMEOUT);
    }

    @Test
    void dispatchRejected_classifiedAsWorkerBusy() {
        PangrovaException cause = new PangrovaException(
                PangrovaErrorCode.COORDINATOR_DISPATCH_REJECTED, "Worker rejected task: overloaded");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.WORKER_BUSY);
    }

    @Test
    void genericRuntimeException_classifiedAsWorkerFatal() {
        RuntimeException cause = new RuntimeException("Internal server error");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.WORKER_FATAL);
    }

    @Test
    void dispatchFailed_classifiedAsWorkerFatal() {
        PangrovaException cause = new PangrovaException(
                PangrovaErrorCode.COORDINATOR_DISPATCH_FAILED, "Worker returned HTTP 500");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.WORKER_FATAL);
    }

    @Test
    void completionException_unwrapsToClassifyCause() {
        IOException innerCause = new IOException("Network unreachable");
        CompletionException wrapper = new CompletionException(innerCause);
        TaskDispatchException result = dispatcher.classifyAndWrap(wrapper, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.NETWORK);
        assertThat(result.getCause()).isSameAs(innerCause);
    }

    @Test
    void completionExceptionWithTimeout_classifiedAsRpcTimeout() {
        TimeoutException innerCause = new TimeoutException("timed out");
        CompletionException wrapper = new CompletionException(innerCause);
        TaskDispatchException result = dispatcher.classifyAndWrap(wrapper, WORKER_HOST);

        assertThat(result.getCategory()).isEqualTo(ErrorCategory.RPC_TIMEOUT);
    }

    @Test
    void alreadyClassifiedException_returnedAsIs() {
        TaskDispatchException original = new TaskDispatchException(
                ErrorCategory.NETWORK, WORKER_HOST, "already classified");
        TaskDispatchException result = dispatcher.classifyAndWrap(original, WORKER_HOST);

        assertThat(result).isSameAs(original);
    }

    @Test
    void exceptionMessage_containsCategoryAndWorkerHost() {
        IOException cause = new IOException("connection reset");
        TaskDispatchException result = dispatcher.classifyAndWrap(cause, WORKER_HOST);

        assertThat(result.getMessage()).contains(WORKER_HOST);
        assertThat(result.getMessage()).contains("NETWORK");
    }
}
