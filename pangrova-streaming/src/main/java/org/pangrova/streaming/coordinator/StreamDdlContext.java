package org.pangrova.streaming.coordinator;

import org.pangrova.streaming.api.checkpoint.StreamOffsetStore;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;

import java.util.Objects;

/**
 * Aggregated dependencies for {@link StreamDdlExecutor}.
 *
 * <p>Keeps the executor constructor at 1 param (this record),
 * satisfying the max-4-constructor-params rule.</p>
 *
 * @param stateStore  persistent stream state store
 * @param offsetStore persistent stream offset store (needed for drop cleanup)
 * @param scheduler   stream pipeline scheduler for start/stop lifecycle
 * @param config      system-level stream configuration
 */
public record StreamDdlContext(
        StreamStateStore stateStore,
        StreamOffsetStore offsetStore,
        StreamScheduler scheduler,
        StreamConfig config
) {

    public StreamDdlContext {
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(offsetStore, "offsetStore must not be null");
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        Objects.requireNonNull(config, "config must not be null");
    }
}
