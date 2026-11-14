package org.pangrova.streaming.api.checkpoint;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;

/**
 * Persistent store for committed Kafka offsets per ingestion stream.
 *
 * <p>Offsets are persisted atomically per batch after successful writes,
 * enabling restart recovery with at-least-once semantics. The foreign key
 * on {@code ingestion_stream_offsets} cascades deletes when a stream is
 * dropped from {@code ingestion_stream_state}.</p>
 *
 * <p>Implementations must be thread-safe.</p>
 */
@Experimental
public interface StreamOffsetStore {

    /**
     * Persist committed offsets for a stream. Upsert semantics:
     * existing offsets for the same (stream, topic, partition) are replaced.
     * All offsets in the map are persisted atomically.
     *
     * @param streamName unique stream identifier
     * @param offsets    map of topic-partition to committed offset
     */
    void saveOffsets(String streamName, Map<TopicPartitionOffset, Long> offsets);

    /**
     * Load the last committed offsets for a stream.
     *
     * @param streamName unique stream identifier
     * @return map of topic-partition to committed offset, empty map if none found
     */
    Map<TopicPartitionOffset, Long> loadOffsets(String streamName);

    /**
     * Delete all committed offsets for a stream.
     *
     * @param streamName unique stream identifier
     */
    void deleteOffsets(String streamName);
}
