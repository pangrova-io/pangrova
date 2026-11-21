package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.commit.CdcCommitCoordinator;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.util.Objects;

/**
 * Encapsulates all dependencies for {@link CdcPipelineLoop}.
 *
 * @param sourceReader          reads raw batches from the message source
 * @param converter             format-specific CDC record deserializer
 * @param writeSink             writes CDC envelopes to the target table
 * @param quotaEnforcer         backpressure control for write throughput
 * @param watermarkTracker      per-table watermark state machine
 * @param maintenanceScheduler  compaction scheduling on write completion
 * @param runtimeHooks          metrics provider, failure listener, retry backoff, and optional 2PC
 *                               coordinator; a null coordinator inside the hooks
 *                               falls back to at-least-once semantics
 */
public record CdcPipelineLoopContext(
    CdcSourceReader sourceReader,
    CdcRecordConverter converter,
    CdcWriteSink writeSink,
    CdcQuotaEnforcer quotaEnforcer,
    WatermarkTracker watermarkTracker,
    MaintenanceScheduler maintenanceScheduler,
    CdcPipelineRuntimeHooks runtimeHooks
) {

    public CdcPipelineLoopContext {
        Objects.requireNonNull(sourceReader, "sourceReader must not be null");
        Objects.requireNonNull(converter, "converter must not be null");
        Objects.requireNonNull(writeSink, "writeSink must not be null");
        Objects.requireNonNull(quotaEnforcer, "quotaEnforcer must not be null");
        Objects.requireNonNull(watermarkTracker, "watermarkTracker must not be null");
        Objects.requireNonNull(maintenanceScheduler, "maintenanceScheduler must not be null");
        Objects.requireNonNull(runtimeHooks, "runtimeHooks must not be null");
    }

    /** Returns the callback for pipeline failure events. */
    public CdcPipelineListener listener() {
        return runtimeHooks.listener();
    }

    /** Returns the exponential backoff strategy for transient failures. */
    public BackoffStrategy retryBackoff() {
        return runtimeHooks.retryBackoff();
    }

    /** Returns the optional 2PC coordinator; null means at-least-once fallback. */
    public CdcCommitCoordinator commitCoordinator() {
        return runtimeHooks.commitCoordinator();
    }
}
