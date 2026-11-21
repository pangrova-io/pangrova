package org.pangrova.streaming.source.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.SourceUnavailableException;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;

/**
 * Kafka-backed implementation of {@link CdcSourceReader}.
 *
 * <p>Translates between Kafka consumer types and the Kafka-agnostic SPI types,
 * keeping Kafka client dependency confined to {@code pangrova-streaming-source-kafka}.</p>
 */
final class KafkaCdcSourceReader implements CdcSourceReader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCdcSourceReader.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;
    // Accessed only by the consumer's pipeline thread, including rebalance callbacks.
    private final Map<TopicPartition, Long> pendingSeekOffsets = new HashMap<>();

    /**
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic            topic to subscribe to
     * @param groupId          consumer group ID
     * @param extraConfig      additional consumer configuration (may be empty)
     */
    KafkaCdcSourceReader(String bootstrapServers, String topic,
                                   String groupId, Map<String, String> extraConfig) {
        this.topic = topic;
        Properties props = new Properties();
        if (extraConfig != null) {
            props.putAll(extraConfig);
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<byte[], byte[]> acquired = null;
        try {
            acquired = new KafkaConsumer<>(props);
            this.consumer = acquired;
            this.consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Publication and explicit source acknowledgement belong to the pipeline.
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    applyPendingSeeks(partitions);
                }
            });
        } catch (RuntimeException | Error failure) {
            if (acquired != null) {
                try {
                    acquired.close();
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (failure instanceof Error fatal) {
                throw fatal;
            }
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Failed to create Kafka consumer for topic=" + topic, failure);
        }
    }

    @Override
    public OptionalLong currentLag() {
        var assigned = consumer.assignment();
        if (assigned.isEmpty()) return OptionalLong.empty();
        long total = 0;
        for (TopicPartition partition : assigned) {
            OptionalLong lag = consumer.currentLag(partition);
            if (lag.isEmpty() || lag.getAsLong() < 0) return OptionalLong.empty();
            if (Long.MAX_VALUE - total < lag.getAsLong()) return OptionalLong.empty();
            total += lag.getAsLong();
        }
        return OptionalLong.of(total);
    }

    @Override
    public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
            List<SourceRecord> result = convertRecords(records, maxRecords);
            if (result.size() < records.count()) {
                seekBackTruncatedPartitions(records, result);
            }
            return result;
        } catch (TimeoutException e) {
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Kafka broker unreachable for topic=" + topic, e);
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Kafka consumer interrupted for topic=" + topic, e);
        }
    }

    /**
     * Seeks the consumer back for partitions that had records truncated due to maxRecords limit.
     *
     * <p>When poll() returns more records than maxRecords, the consumer's internal position
     * has already advanced past all fetched records. Without seeking back, the unconsumed
     * records would be permanently lost. This method computes the first unconsumed offset
     * per partition and seeks the consumer back to that position.</p>
     */
    private void seekBackTruncatedPartitions(ConsumerRecords<byte[], byte[]> allRecords,
                                             List<SourceRecord> consumed) {
        Map<TopicPartition, Long> consumedNextOffsets = new HashMap<>();
        for (SourceRecord record : consumed) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            consumedNextOffsets.merge(tp, record.offset() + 1, Math::max);
        }

        for (TopicPartition tp : allRecords.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = allRecords.records(tp);
            if (partitionRecords.isEmpty()) {
                continue;
            }
            Long nextConsumed = consumedNextOffsets.get(tp);
            if (nextConsumed == null) {
                consumer.seek(tp, partitionRecords.get(0).offset());
            } else {
                long lastPolledOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                if (lastPolledOffset >= nextConsumed) {
                    consumer.seek(tp, nextConsumed);
                }
            }
        }
        LOG.debug("event=seek_back_truncated topic={} consumedCount={} totalPolled={}",
                topic, consumed.size(), allRecords.count());
    }

    private static List<SourceRecord> convertRecords(ConsumerRecords<byte[], byte[]> records,
                                                     int maxRecords) {
        List<SourceRecord> result = new ArrayList<>(Math.min(records.count(), maxRecords));
        int count = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (count >= maxRecords) {
                break;
            }
            result.add(new SourceRecord(
                    record.topic(), record.partition(), record.offset(),
                    record.key(), record.value(), record.timestamp()));
            count++;
        }
        return result;
    }

    @Override
    public List<TopicPartitionOffset> partitions() {
        try {
            var description = consumer.partitionsFor(topic);
            if (description == null || description.isEmpty()) {
                throw new SourceUnavailableException(
                        PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                        "Kafka source has no partitions for topic=" + topic);
            }
            return description.stream()
                    .map(info -> new TopicPartitionOffset(info.topic(), info.partition()))
                    .sorted(java.util.Comparator.comparingInt(TopicPartitionOffset::partition))
                    .toList();
        } catch (TimeoutException e) {
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Kafka partition lookup timed out for topic=" + topic, e);
        }
    }

    @Override
    public Map<TopicPartitionOffset, Long> endOffsets(
            Collection<TopicPartitionOffset> partitions) {
        try {
            Map<TopicPartition, Long> actual = consumer.endOffsets(
                    partitions.stream()
                            .map(offset -> new TopicPartition(offset.topic(), offset.partition()))
                            .toList());
            Map<TopicPartitionOffset, Long> result = new HashMap<>();
            actual.forEach((partition, end) -> result.put(
                    new TopicPartitionOffset(partition.topic(), partition.partition()), end));
            return Map.copyOf(result);
        } catch (TimeoutException e) {
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Kafka end-offset lookup timed out for topic=" + topic, e);
        }
    }

    @Override
    public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
        Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = new HashMap<>();
        for (var entry : offsets.entrySet()) {
            TopicPartition tp = new TopicPartition(entry.getKey().topic(), entry.getKey().partition());
            kafkaOffsets.put(tp, new OffsetAndMetadata(entry.getValue()));
        }
        try {
            consumer.commitSync(kafkaOffsets);
        } catch (TimeoutException e) {
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Kafka offset commit timed out for topic=" + topic, e);
        }
    }

    @Override
    public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
        Map<TopicPartition, Long> requested = new HashMap<>();
        for (var entry : offsets.entrySet()) {
            if (!topic.equals(entry.getKey().topic()) || entry.getValue() == null || entry.getValue() < 0) {
                throw new IllegalArgumentException("Invalid recovery offset for subscribed topic=" + topic);
            }
            TopicPartition tp = new TopicPartition(entry.getKey().topic(), entry.getKey().partition());
            requested.put(tp, entry.getValue());
        }
        pendingSeekOffsets.putAll(requested);
        // subscribe() assigns no partitions until poll(). The callback applies restart
        // checkpoints before fetching records; already assigned partitions can seek now.
        applyPendingSeeks(consumer.assignment());
    }

    private void applyPendingSeeks(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Long offset = pendingSeekOffsets.get(partition);
            if (offset != null) {
                consumer.seek(partition, offset);
                pendingSeekOffsets.remove(partition);
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
