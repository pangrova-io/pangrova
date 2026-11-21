package org.pangrova.worker.bootstrap.lifecycle;

import java.util.concurrent.ScheduledExecutorService;

import org.pangrova.observability.metrics.MetricsScope;
import org.pangrova.observability.sdk.config.TelemetryInitializer;
import org.pangrova.observability.sdk.metrics.runtime.NativeMetricsPublisher;
import org.pangrova.worker.buffer.ShuffleBufferManager;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.ShuffleServiceMemoryLease;

/** Shared resource owners released only after tasks, HTTP and heartbeat have drained. */
public record WorkerResourceShutdown(
        ScheduledExecutorService metricsExecutor,
        ResourceManager resourceManager,
        NativeMetricsPublisher nativeMetricsPublisher,
        MetricsScope metricsScope,
        TelemetryInitializer otelInitializer,
        boolean nativeAvailable,
        ShuffleServiceMemoryLease shuffleServiceMemoryLease,
        ShuffleBufferManager shuffleBufferManager) {}
