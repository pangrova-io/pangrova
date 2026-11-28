package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.phase.WorkerTelemetryInitializer;
import org.pangrova.worker.bootstrap.lifecycle.WorkerResourceShutdown;
import org.pangrova.worker.bootstrap.lifecycle.WorkerShutdownSequence;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerMetricsScopeShutdownTest {
    @Test
    void sharedResourceShutdownReleasesTheBorrowedMetricsScope() {
        var observability = new WorkerTelemetryInitializer();
        observability.initialize(WorkerBootstrapConfig.withDefaults("scope-owner"));
        var borrowed = observability.metricsProvider();
        var gauge = borrowed.createGauge("owned", "", "1", () -> 1.0);
        var resources = new WorkerResourceShutdown(null, null, null,
                observability.metricsScope(), observability.otelInitializer(), false, null, null);
        try {
            WorkerShutdownSequence.releaseResources(resources, () -> {});
            WorkerShutdownSequence.releaseResources(resources, () -> {});
            gauge.close();
            assertThatThrownBy(() -> borrowed.createGauge("late", "", "1", () -> 2.0))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        } finally {
            WorkerShutdownSequence.releaseResources(resources, () -> {});
        }
    }
}
