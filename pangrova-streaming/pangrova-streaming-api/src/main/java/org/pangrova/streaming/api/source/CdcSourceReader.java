package org.pangrova.streaming.api.source;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Kafka-agnostic SPI for CDC source message consumption.
 *
 * <p>Abstracts batch polling, explicit offset management, and seek-based restart
 * recovery so that {@code pangrova-streaming} can process CDC batches without
 * depending on Kafka client types.</p>
 *
 * <p>This interface is part of the <em>declarative incremental data transformation</em>
 * subsystem. It reads change events from an external source (e.g., Kafka topic fed by
 * Debezium) for CDC-driven incremental ETL and materialized view refresh. It is NOT
 * a general-purpose stream processing source operator.</p>
 *
 * <p>Contains zero Kafka client type references — only primitive types,
 * standard Java types, and SPI-defined records ({@link SourceRecord},
 * {@link TopicPartitionOffset}).</p>
 *
 * <p>One pipeline owns one reader. Poll, seek, and commit are serialized. The owner closes
 * the reader after worker operations drain or when pipeline setup fails.</p>
 */
@Experimental
public interface CdcSourceReader extends AutoCloseable {

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

    /** Last known unconsumed offset distance across assigned partitions, sampled on the polling thread. */
    default OptionalLong currentLag() {
        return OptionalLong.empty();
    }

    /**
     * Returns the current exclusive end offset of each source partition.
     *
     * <p>Bounded Worker ingestion uses this to prove that an admitted interval is
     * exhausted when compacted logs contain gaps and no record reaches its end.
     * Implementations that cannot expose exact log ends must reject the call rather
     * than fabricate completion.</p>
     *
     * @param partitions exact topic partitions requested by the admitted task
     * @return current exclusive end offset for each requested partition
     */
    default Map<TopicPartitionOffset, Long> endOffsets(
            Collection<TopicPartitionOffset> partitions) {
        throw new UnsupportedOperationException(
                "Source does not expose exact end offsets");
    }

    /**
     * Returns the exact partitions known for this reader's subscribed source.
     * The Coordinator uses this to allocate one bounded range per partition.
     */
    default List<TopicPartitionOffset> partitions() {
        throw new UnsupportedOperationException(
                "Source does not expose its partition inventory");
    }

    @Override
    void close();
}
