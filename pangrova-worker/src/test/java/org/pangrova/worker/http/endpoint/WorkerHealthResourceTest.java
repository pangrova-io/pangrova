package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.core.Response;
import org.pangrova.operations.bootstrap.HealthCheckEndpoint;
import org.pangrova.operations.bootstrap.HealthStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerHealthResourceTest {

    @Test
    void healthyWorkerReturns200() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER,
                new StubConditions(true, true, true, 9091, true));
        WorkerHealthResource resource = new WorkerHealthResource(endpoint);
        Response response = resource.health();
        assertThat(response.getStatus()).isEqualTo(200);
        HealthStatus status = (HealthStatus) response.getEntity();
        assertThat(status.ready()).isTrue();
        assertThat(status.resultServerPort()).isEqualTo(9091);
        assertThat(status.resultServerRunning()).isTrue();
    }

    @Test
    void unhealthyWorkerReturns503() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER,
                new StubConditions(true, false, true, -1, false));
        WorkerHealthResource resource = new WorkerHealthResource(endpoint);
        Response response = resource.health();
        assertThat(response.getStatus()).isEqualTo(503);
    }

    private record StubConditions(boolean http, boolean transport, boolean nativeReady,
                                  int rsPort, boolean rsRunning)
            implements HealthCheckEndpoint.HealthConditions {
        @Override public boolean isHttpReady() { return http; }
        @Override public boolean isTransportReady() { return transport; }
        @Override public boolean isNativeReady() { return nativeReady; }
        @Override public int resultServerPort() { return rsPort; }
        @Override public boolean resultServerRunning() { return rsRunning; }
        @Override public int activeWorkerCount() { return 0; }
        @Override public int activeCatalogCount() { return 0; }
    }
}
