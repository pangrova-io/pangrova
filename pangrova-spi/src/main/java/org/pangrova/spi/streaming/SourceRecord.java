package org.pangrova.spi.streaming;

/**
 * Kafka-agnostic raw record from a message source.
 *
 * <p>Carries raw byte arrays for key and value, plus topic/partition/offset metadata.
 * Contains zero Kafka client type references — only primitive types and standard Java types.</p>
 *
 * @param topic     source topic name
 * @param partition source partition number
 * @param offset    record offset within the partition
 * @param key       raw key bytes (nullable)
 * @param value     raw value bytes (nullable)
 * @param timestamp record timestamp in epoch milliseconds
 */
public record SourceRecord(
        String topic,
        int partition,
        long offset,
        byte[] key,
        byte[] value,
        long timestamp
) {

    public SourceRecord {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative, got: " + partition);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative, got: " + offset);
        }
    }
}
