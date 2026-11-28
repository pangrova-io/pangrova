package org.pangrova.streaming.coordinator;

import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;

import java.util.Objects;

/** Durable stream state and source validation used by CDC DDL. */
public record StreamDdlContext(
        StreamStateStore stateStore,
        CdcRecordConverterRegistry converters,
        int maxStreamsPerTenant,
        CdcSourceRegistry sources) {

    public StreamDdlContext {
        Objects.requireNonNull(stateStore, "stateStore");
        Objects.requireNonNull(converters, "converters");
        if (maxStreamsPerTenant <= 0) {
            throw new IllegalArgumentException("maxStreamsPerTenant must be positive");
        }
        Objects.requireNonNull(sources, "sources");
    }
}
