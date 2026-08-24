package org.pangrova.spi.streaming;

/**
 * Kafka-agnostic identifier for a topic-partition pair.
 *
 * <p>Used as a key in offset maps for {@link CdcSourceReader#commitOffsets}
 * and {@link CdcSourceReader#seekToOffsets}. Contains zero Kafka client type
 * references — only standard Java types.</p>
 *
 * @param topic     topic name
 * @param partition partition number
 */
public record TopicPartitionOffset(String topic, int partition) {

    public TopicPartitionOffset {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative, got: " + partition);
        }
    }
}
