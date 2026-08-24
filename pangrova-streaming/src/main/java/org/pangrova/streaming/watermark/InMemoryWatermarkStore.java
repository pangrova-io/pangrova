package org.pangrova.streaming.watermark;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.streaming.WatermarkStore;

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
    public void save(String tableName, Instant watermark) {
        data.put(tableName, watermark);
    }

    @Override
    public Optional<Instant> load(String tableName) {
        return Optional.ofNullable(data.get(tableName));
    }
}
