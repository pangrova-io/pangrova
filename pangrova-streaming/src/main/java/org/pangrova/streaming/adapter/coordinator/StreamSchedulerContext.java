package org.pangrova.streaming.adapter.coordinator;

import org.pangrova.streaming.api.checkpoint.StreamOffsetStore;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.streaming.api.cdc.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.core.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.core.commit.CdcCommitCoordinator;
import org.pangrova.streaming.core.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.core.watermark.WatermarkTracker;

import java.util.Objects;
import java.util.function.Function;

/**
 * Aggregated dependencies for {@link StreamScheduler}.
 *
 * <p>Uses functional interfaces for source reader and write sink factories
 * so the scheduler can create per-stream instances without depending on
 * concrete Kafka or connector types.</p>
 *
 * @param stateStore          persistent stream state store
 * @param offsetStore         persistent stream offset store
 * @param commitCoordinator   durable source checkpoint and publication state
 * @param converterRegistry   CDC format converter registry
 * @param watermarkTracker    per-table watermark tracker
 * @param maintenanceScheduler compaction scheduling
 * @param config              system-level stream configuration
 * @param sourceReaderFactory creates a CdcSourceReader from stream state
 * @param writeSinkFactory    creates a CdcWriteSink from stream state
 */
public record StreamSchedulerContext(
        StreamStateStore stateStore,
        StreamOffsetStore offsetStore,
        CdcCommitCoordinator commitCoordinator,
        CdcRecordConverterRegistry converterRegistry,
        WatermarkTracker watermarkTracker,
        MaintenanceScheduler maintenanceScheduler,
        StreamConfig config,
        Function<org.pangrova.streaming.api.checkpoint.StreamState, CdcSourceReader> sourceReaderFactory,
        Function<org.pangrova.streaming.api.checkpoint.StreamState, CdcWriteSink> writeSinkFactory
) {

    public StreamSchedulerContext {
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(offsetStore, "offsetStore must not be null");
        Objects.requireNonNull(commitCoordinator, "commitCoordinator must not be null");
        Objects.requireNonNull(converterRegistry, "converterRegistry must not be null");
        Objects.requireNonNull(watermarkTracker, "watermarkTracker must not be null");
        Objects.requireNonNull(maintenanceScheduler, "maintenanceScheduler must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(sourceReaderFactory, "sourceReaderFactory must not be null");
        Objects.requireNonNull(writeSinkFactory, "writeSinkFactory must not be null");
    }
}
