package org.pangrova.worker.bootstrap;

import org.pangrova.observability.sdk.config.TelemetryInitializer;
import org.pangrova.observability.sdk.metrics.nativebridge.NativeMetricsPublisher;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.buffer.OutputBufferBackpressure;
import org.pangrova.worker.buffer.ResultExpirationScheduler;
import org.pangrova.worker.buffer.ShuffleBufferManager;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Groups all resources needed for the Worker shutdown sequence.
 * Sub-records keep each within the ≤5 param governance limit.
 * Convenience accessors delegate to sub-records for flat access.
 */
public record ShutdownContext(
        SchedulerRefs schedulerRefs,
        ResourceRefs resourceRefs,
        BufferRefs bufferRefs) {

    /**
     * Scheduler-related shutdown resources.
     */
    public record SchedulerRefs(
            ResultExpirationScheduler expirationScheduler,
            ScheduledExecutorService resultExpirationExecutor,
            ScheduledExecutorService metricsExecutor) {
    }

    /**
     * Resource and observability shutdown resources.
     */
    public record ResourceRefs(
            ResourceManager resourceManager,
            NativeMetricsPublisher nativeMetricsPublisher,
            TelemetryInitializer otelInitializer,
            boolean nativeAvailable,
            org.pangrova.worker.bootstrap.phase.ShuffleServiceMemoryLease shuffleServiceMemoryLease) {
    }

    /**
     * Buffer shutdown resources.
     */
    public record BufferRefs(
            OutputBufferBackpressure backpressure,
            ShuffleBufferManager shuffleBufferManager) {
    }

    /** Convenience: delegates to {@code bufferRefs().backpressure()}. */
    public OutputBufferBackpressure backpressure() {
        return bufferRefs != null ? bufferRefs.backpressure() : null;
    }

    /** Convenience: delegates to {@code schedulerRefs().resultExpirationExecutor()}. */
    public ScheduledExecutorService resultExpirationExecutor() {
        return schedulerRefs != null ? schedulerRefs.resultExpirationExecutor() : null;
    }

    /** Convenience: delegates to {@code bufferRefs().shuffleBufferManager()}. */
    public ShuffleBufferManager shuffleBufferManager() {
        return bufferRefs != null ? bufferRefs.shuffleBufferManager() : null;
    }

    /** Convenience: delegates to {@code schedulerRefs().metricsExecutor()}. */
    public ScheduledExecutorService metricsExecutor() {
        return schedulerRefs != null ? schedulerRefs.metricsExecutor() : null;
    }

    /** Convenience: delegates to {@code resourceRefs().nativeMetricsPublisher()}. */
    public NativeMetricsPublisher nativeMetricsPublisher() {
        return resourceRefs != null ? resourceRefs.nativeMetricsPublisher() : null;
    }

    /** Convenience: delegates to {@code resourceRefs().nativeAvailable()}. */
    public boolean nativeAvailable() {
        return resourceRefs != null && resourceRefs.nativeAvailable();
    }

    /** Convenience: delegates to {@code resourceRefs().resourceManager()}. */
    public ResourceManager resourceManager() {
        return resourceRefs != null ? resourceRefs.resourceManager() : null;
    }

    /** Convenience: delegates to {@code resourceRefs().otelInitializer()}. */
    public TelemetryInitializer otelInitializer() {
        return resourceRefs != null ? resourceRefs.otelInitializer() : null;
    }
}
