package org.pangrova.streaming.api.watermark;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.Optional;

/**
 * SPI for persisting watermark positions.
 *
 * <p>Implementations provide durable storage for CDC watermark state,
 * ensuring watermark positions survive coordinator restarts.</p>
 */
@Experimental
public interface WatermarkStore {

    /**
     * Persists the watermark position for the given source.
     *
     * @param tableName the source table identifier
     * @param watermark the watermark timestamp to persist
     */
    void save(String tableName, Instant watermark);

    /**
     * Loads the last persisted watermark position for the given source.
     *
     * @param tableName the source table identifier
     * @return the watermark if previously saved, empty otherwise
     */
    Optional<Instant> load(String tableName);
}
