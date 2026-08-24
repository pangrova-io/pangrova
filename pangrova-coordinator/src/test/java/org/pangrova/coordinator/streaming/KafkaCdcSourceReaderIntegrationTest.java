package org.pangrova.coordinator.streaming;

import org.pangrova.spi.streaming.SourceRecord;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying Kafka offset seek-back on truncation.
 *
 * <p>When maxRecords is less than the number of records returned by poll(),
 * the consumer must seek back so that truncated records are not lost and
 * are delivered on subsequent polls.</p>
 */
@Testcontainers
class KafkaCdcSourceReaderIntegrationTest {

    private static final String TOPIC = "seek-back-test";
    private static final String GROUP_ID = "seek-back-test-group";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private KafkaCdcSourceReader reader;
    private KafkaProducer<byte[], byte[]> producer;

    @BeforeEach
    void setUp() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Map<String, String> extraConfig = new HashMap<>();
        extraConfig.put("auto.offset.reset", "earliest");
        reader = new KafkaCdcSourceReader(
                KAFKA.getBootstrapServers(), TOPIC, GROUP_ID, extraConfig);
    }

    @AfterEach
    void tearDown() {
        if (reader != null) {
            reader.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void truncatedRecordsAreNotLost() {
        int totalRecords = 10;
        int maxRecordsPerPoll = 5;

        for (int i = 0; i < totalRecords; i++) {
            byte[] value = ("record-" + i).getBytes(StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>(TOPIC, value));
        }
        producer.flush();

        List<SourceRecord> allConsumed = new ArrayList<>();
        int maxAttempts = 20;
        int attempts = 0;

        while (allConsumed.size() < totalRecords && attempts < maxAttempts) {
            List<SourceRecord> batch = reader.pollBatch(maxRecordsPerPoll, Duration.ofSeconds(5));
            if (!batch.isEmpty()) {
                allConsumed.addAll(batch);
                Map<TopicPartitionOffset, Long> offsets = extractMaxOffsets(batch);
                reader.commitOffsets(offsets);
            }
            attempts++;
        }

        assertThat(allConsumed)
                .as("All %d records should be consumed across multiple polls with maxRecords=%d",
                        totalRecords, maxRecordsPerPoll)
                .hasSize(totalRecords);

        List<String> values = allConsumed.stream()
                .map(r -> new String(r.value(), StandardCharsets.UTF_8))
                .toList();
        for (int i = 0; i < totalRecords; i++) {
            assertThat(values).contains("record-" + i);
        }
    }

    private Map<TopicPartitionOffset, Long> extractMaxOffsets(List<SourceRecord> batch) {
        Map<TopicPartitionOffset, Long> offsets = new HashMap<>();
        for (SourceRecord record : batch) {
            TopicPartitionOffset key = new TopicPartitionOffset(record.topic(), record.partition());
            offsets.merge(key, record.offset() + 1, Math::max);
        }
        return offsets;
    }
}
