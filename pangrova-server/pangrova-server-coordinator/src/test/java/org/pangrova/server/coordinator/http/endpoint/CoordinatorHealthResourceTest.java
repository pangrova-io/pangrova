package org.pangrova.server.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.pangrova.common.lifecycle.HealthCheckEndpoint;

class CoordinatorHealthResourceTest {

    @Test
    void liveHttpDoesNotRequireWorkerOrCatalogReadiness() {
        var resource = new CoordinatorHealthResource(new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR, conditions(true)));

        assertThat(resource.live().getStatus()).isEqualTo(200);
        assertThat(resource.health().getStatus()).isEqualTo(503);
    }

    @Test
    void unavailableHttpFailsLiveness() {
        var resource = new CoordinatorHealthResource(new HealthCheckEndpoint(
                HealthCheckEndpoint.NodeType.COORDINATOR, conditions(false)));

        assertThat(resource.live().getStatus()).isEqualTo(503);
    }

    private static HealthCheckEndpoint.HealthConditions conditions(boolean httpReady) {
        return new HealthCheckEndpoint.HealthConditions() {
            @Override public boolean isHttpReady() { return httpReady; }
            @Override public boolean isTransportReady() { return false; }
            @Override public boolean isNativeReady() { return false; }
            @Override public int activeWorkerCount() { return 0; }
            @Override public int activeCatalogCount() { return 0; }
        };
    }
}
