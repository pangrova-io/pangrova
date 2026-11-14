package org.pangrova.persistence.streaming.memory;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.api.watermark.WatermarkStore;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link WatermarkStore} backed by {@link ConcurrentHashMap}.
 * Default implementation when no external persistence is configured.
 */
@ThreadSafe
public final class InMemoryWatermarkStore implements WatermarkStore {

    private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();

    @Override
    public Instant advance(String tableName, Instant watermark) {
        return data.merge(tableName, watermark, (previous, proposed) -> previous.isAfter(proposed) ? previous : proposed);
    }

    @Override
    public Optional<Instant> load(String tableName) {
        return Optional.ofNullable(data.get(tableName));
    }
}
