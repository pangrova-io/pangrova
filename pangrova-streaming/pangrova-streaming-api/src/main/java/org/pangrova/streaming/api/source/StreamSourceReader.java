package org.pangrova.streaming.api.source;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Kafka-agnostic SPI for raw message consumption.
 *
 * <p>Abstracts batch polling, explicit offset management, and seek-based restart
 * recovery so that {@code pangrova-streaming} can process CDC batches without
 * depending on Kafka client types.</p>
 *
 * <p>Contains zero Kafka client type references — only primitive types,
 * standard Java types, and SPI-defined records ({@link SourceRecord},
 * {@link TopicPartitionOffset}).</p>
 *
 * <p>Implementations must be thread-safe if shared across pipelines.</p>
 */
@Experimental
public interface StreamSourceReader extends AutoCloseable {

    /**
     * Poll a batch of raw records from the source.
     *
     * @param maxRecords maximum number of records to return
     * @param timeout    maximum time to wait for records
     * @return list of raw records (empty if none available within timeout)
     */
    List<SourceRecord> pollBatch(int maxRecords, Duration timeout);

    /**
     * Commit offsets for explicit offset management.
     *
     * <p>After a successful write, the pipeline commits the offsets of the
     * records that were written, enabling at-least-once delivery semantics.</p>
     *
     * @param offsets map of topic-partition to the next offset to consume
     */
    void commitOffsets(Map<TopicPartitionOffset, Long> offsets);

    /**
     * Seek to specific offsets for restart recovery.
     *
     * <p>Used after coordinator restart or HA failover to resume consumption
     * from the last committed offsets stored in the metadata store.</p>
     *
     * @param offsets map of topic-partition to the offset to seek to
     */
    void seekToOffsets(Map<TopicPartitionOffset, Long> offsets);

    @Override
    void close();
}
