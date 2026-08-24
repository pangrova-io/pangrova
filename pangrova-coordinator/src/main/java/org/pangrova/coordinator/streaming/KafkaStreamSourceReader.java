package org.pangrova.coordinator.streaming;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.SourceUnavailableException;
import org.pangrova.spi.streaming.SourceRecord;
import org.pangrova.spi.streaming.StreamSourceReader;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka-backed implementation of {@link StreamSourceReader}.
 *
 * <p>Translates between Kafka consumer types and the Kafka-agnostic SPI types,
 * keeping Kafka client dependency confined to {@code pangrova-coordinator}.</p>
 */
public final class KafkaStreamSourceReader implements StreamSourceReader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamSourceReader.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;

    /**
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic            topic to subscribe to
     * @param groupId          consumer group ID
     * @param extraConfig      additional consumer configuration (may be empty)
     */
    public KafkaStreamSourceReader(String bootstrapServers, String topic,
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

        try {
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(List.of(topic));
        } catch (Exception e) {
            throw new SourceUnavailableException(
                    PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                    "Failed to create Kafka consumer for topic=" + topic, e);
        }
    }

    @Override
    public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
            return convertRecords(records, maxRecords);
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
        for (var entry : offsets.entrySet()) {
            TopicPartition tp = new TopicPartition(entry.getKey().topic(), entry.getKey().partition());
            consumer.seek(tp, entry.getValue());
        }
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOG.warn("event=kafka_consumer_close_error topic={} error={}", topic, e.getMessage());
        }
    }
}
