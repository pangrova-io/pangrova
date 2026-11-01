package org.pangrova.coordinator.streaming;

import org.pangrova.spi.streaming.store.StreamOffsetStore;
import org.pangrova.spi.streaming.store.StreamStateStore;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.streaming.source.CdcSourceReader;
import org.pangrova.spi.streaming.sink.CdcWriteSink;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.cdc.commit.CdcCommitCoordinator;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;

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
 * @param accessControl       write authorization; null disables the check (single-node/tests)
 * @param metricsProvider     CDC pipeline metrics; null yields the no-op recorder
 */
public record StreamSchedulerContext(
        StreamStateStore stateStore,
        StreamOffsetStore offsetStore,
        CdcCommitCoordinator commitCoordinator,
        CdcRecordConverterRegistry converterRegistry,
        WatermarkTracker watermarkTracker,
        MaintenanceScheduler maintenanceScheduler,
        StreamConfig config,
        Function<org.pangrova.spi.streaming.store.StreamState, CdcSourceReader> sourceReaderFactory,
        Function<org.pangrova.spi.streaming.store.StreamState, CdcWriteSink> writeSinkFactory,
        AccessControl accessControl,
        MetricsProvider metricsProvider
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

    /** Test/single-node constructor without authorization and metrics. */
    public StreamSchedulerContext(
            StreamStateStore stateStore, StreamOffsetStore offsetStore,
            CdcCommitCoordinator commitCoordinator, CdcRecordConverterRegistry converterRegistry,
            WatermarkTracker watermarkTracker, MaintenanceScheduler maintenanceScheduler,
            StreamConfig config,
            Function<org.pangrova.spi.streaming.store.StreamState, CdcSourceReader> sourceReaderFactory,
            Function<org.pangrova.spi.streaming.store.StreamState, CdcWriteSink> writeSinkFactory) {
        this(stateStore, offsetStore, commitCoordinator, converterRegistry, watermarkTracker,
                maintenanceScheduler, config, sourceReaderFactory, writeSinkFactory, null, null);
    }
}
