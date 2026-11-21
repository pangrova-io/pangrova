package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservabilityScopeShutdownTest {
    @Test
    void coordinatorShutdownClosesTheProviderScopeEvenWhenTelemetryIsDisabled() {
        var bootstrap = new ObservabilityBootstrap(CoordinatorBootstrapConfig.withDefaults("scope-owner", 0));
        bootstrap.start();
        var borrowed = bootstrap.metricsProvider();
        var gauge = borrowed.createGauge("owned", "", "1", () -> 1.0);
        try {
            bootstrap.shutdown();
            bootstrap.shutdown();
            gauge.close();
            assertThatThrownBy(() -> borrowed.createGauge("late", "", "1", () -> 2.0))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        } finally {
            bootstrap.shutdown();
        }
    }
}
