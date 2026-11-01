package org.pangrova.coordinator.streaming;

import org.pangrova.streaming.coordinator.KafkaCdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test verifying Kafka offset seek-back on truncation.
 *
 * <p>When maxRecords is less than the number of records returned by poll(),
 * the consumer must seek back so that truncated records are not lost and
 * are delivered on subsequent polls.</p>
 */
@Testcontainers
class KafkaCdcSourceReaderIntegrationTest extends ContainerTestBase {

    private String topic;
    private String groupId;

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private KafkaCdcSourceReader reader;
    private KafkaProducer<byte[], byte[]> producer;

    @BeforeEach
    void setUp() throws Exception {
        topic = "seek-back-" + UUID.randomUUID();
        groupId = "recovery-" + UUID.randomUUID();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Map<String, String> extraConfig = new HashMap<>();
        extraConfig.put("auto.offset.reset", "earliest");
        reader = new KafkaCdcSourceReader(
                KAFKA.getBootstrapServers(), topic, groupId, extraConfig);
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
            producer.send(new ProducerRecord<>(topic, value));
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

    @Test
    void restartCheckpointsOverrideKafkaOffsetsBeforeFirstAssignment() throws Exception {
        for (int partition = 0; partition < 2; partition++) {
            for (int offset = 0; offset < 5; offset++) {
                producer.send(new ProducerRecord<>(topic, partition, null,
                        (partition + ":" + offset).getBytes(StandardCharsets.UTF_8)))
                        .get(30, TimeUnit.SECONDS);
            }
        }
        List<SourceRecord> original = pollAtLeast(10, 10);
        assertThat(original).hasSize(10);
        reader.commitOffsets(extractMaxOffsets(original));
        reader.close();
        reader = new KafkaCdcSourceReader(KAFKA.getBootstrapServers(), topic, groupId,
                Map.of("auto.offset.reset", "earliest"));

        // This is the production startup order: JDBC recovery precedes the first poll.
        reader.seekToOffsets(Map.of(new TopicPartitionOffset(topic, 0), 2L,
                new TopicPartitionOffset(topic, 1), 4L));

        assertThat(pollAtLeast(4, 10).stream().map(record -> record.partition() + ":" + record.offset()))
                .containsExactlyInAnyOrder("0:2", "0:3", "0:4", "1:4");
    }

    @Test
    void explicitSeekAlsoAppliesToAlreadyAssignedPartitions() throws Exception {
        for (int offset = 0; offset < 5; offset++) {
            producer.send(new ProducerRecord<>(topic, 0, null, new byte[] {(byte) offset}))
                    .get(30, TimeUnit.SECONDS);
        }
        assertThat(pollAtLeast(2, 2)).hasSize(2);

        reader.seekToOffsets(Map.of(new TopicPartitionOffset(topic, 0), 4L));

        assertThat(pollAtLeast(1, 10)).extracting(SourceRecord::offset).containsExactly(4L);
    }

    @Test
    void recoveryCannotSilentlyQueueOffsetsForAnotherTopic() {
        assertThatThrownBy(() -> reader.seekToOffsets(Map.of(new TopicPartitionOffset("other", 0), 1L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.seekToOffsets(Map.of(new TopicPartitionOffset(topic, 0), -1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private List<SourceRecord> pollAtLeast(int count, int maxBatchSize) {
        var records = new ArrayList<SourceRecord>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (records.size() < count && System.nanoTime() < deadline) {
            records.addAll(reader.pollBatch(maxBatchSize, Duration.ofSeconds(1)));
        }
        return records;
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
