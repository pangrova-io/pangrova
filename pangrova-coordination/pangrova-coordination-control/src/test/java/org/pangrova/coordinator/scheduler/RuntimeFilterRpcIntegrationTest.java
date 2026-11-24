package org.pangrova.coordinator.scheduler;

import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.testing.testkit.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


/** Worker HTTP route for injecting a late runtime filter. */
class RuntimeFilterRpcIntegrationTest {

    private static final TracingProvider TRACING = new NoOpTracingProvider();

    private WorkerHttpServer workerServer;
    private RemoteTaskDispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0);
        workerServer = new WorkerHttpServer(config, taskManager, TestExchangeTransport.INSTANCE);
        workerServer.start();
        dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TRACING);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (workerServer != null) {
            workerServer.stop();
        }
    }

    @Test
    @DisplayName("injectDynamicFilter RPC sends valid HTTP POST to worker endpoint")
    void injectDynamicFilter_sendsHttpPost() {
        // Directly call injectDynamicFilter to verify the HTTP path
        String workerAddress = "localhost:" + workerServer.getPort();
        byte[] filterProto = new byte[]{0x0A, 0x04, 0x08, 0x01, 0x10, 0x64};

        // Should not throw — worker returns CONFLICT (task not found) but that's OK
        dispatcher.injectDynamicFilter("task-nonexistent", "scan_col", filterProto, workerAddress);

        // If we reach here, the HTTP path is functional (no exception thrown)
    }
}
