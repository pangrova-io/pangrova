package org.pangrova.operations.bootstrap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for HealthCheckEndpoint readiness reporting.
 */
class HealthCheckEndpointTest {

    @Test
    void coordinatorReadyWhenAllConditionsMet() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR,
                new TestConditions(true, false, false, 1, 1));

        assertThat(endpoint.isReady()).isTrue();
        assertThat(endpoint.health().ready()).isTrue();
        assertThat(endpoint.health().nodeType()).isEqualTo(HealthCheckEndpoint.NodeType.COORDINATOR);
    }

    @Test
    void coordinatorNotReadyWithoutWorkers() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR,
                new TestConditions(true, false, false, 0, 1));

        assertThat(endpoint.isReady()).isFalse();
    }

    @Test
    void coordinatorNotReadyWithoutCatalogs() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR,
                new TestConditions(true, false, false, 1, 0));

        assertThat(endpoint.isReady()).isFalse();
    }

    @Test
    void workerReadyWhenAllConditionsMet() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER,
                new TestConditions(true, true, true, 0, 0));

        assertThat(endpoint.isReady()).isTrue();
        assertThat(endpoint.health().nodeType()).isEqualTo(HealthCheckEndpoint.NodeType.WORKER);
    }

    @Test
    void workerNotReadyWithoutNative() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER,
                new TestConditions(true, true, false, 0, 0));

        assertThat(endpoint.isReady()).isFalse();
    }

    @Test
    void healthStatusContainsAllChecks() {
        HealthCheckEndpoint endpoint = new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.WORKER,
                new TestConditions(true, true, true, 0, 0));

        HealthStatus status = endpoint.health();
        assertThat(status.checks()).containsKeys("http", "transport", "native", "resultServerRunning");
        assertThat(status.checks().get("http")).isTrue();
        assertThat(status.checks().get("transport")).isTrue();
        assertThat(status.checks().get("native")).isTrue();
        assertThat(status.checks().get("resultServerRunning")).isFalse();
        assertThat(status.resultServerPort()).isEqualTo(-1);
        assertThat(status.resultServerRunning()).isFalse();
    }

    private record TestConditions(
            boolean httpReady, boolean transportReady, boolean nativeReady,
            int workerCount, int catalogCount
    ) implements HealthCheckEndpoint.HealthConditions {
        @Override public boolean isHttpReady() { return httpReady; }
        @Override public boolean isTransportReady() { return transportReady; }
        @Override public boolean isNativeReady() { return nativeReady; }
        @Override public int activeWorkerCount() { return workerCount; }
        @Override public int activeCatalogCount() { return catalogCount; }
    }
}
