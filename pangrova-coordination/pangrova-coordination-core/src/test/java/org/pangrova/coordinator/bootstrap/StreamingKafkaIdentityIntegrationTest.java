package org.pangrova.coordinator.bootstrap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.checkpoint.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.cdc.CdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class StreamingKafkaIdentityIntegrationTest extends ContainerTestBase {
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Test
    void sameNameNewCreationDoesNotInheritThePreviousKafkaConsumerGroupOffsets() throws Exception {
        String topic = "identity-" + UUID.randomUUID();
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", ByteArraySerializer.class.getName(),
                "value.serializer", ByteArraySerializer.class.getName()))) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, new byte[] {(byte) i})).get(30, TimeUnit.SECONDS);
            }
        }
        var original = state(topic);
        try (var reader = StreamingBootstrap.createSourceReader(original)) {
            var records = consume(reader, 5);
            assertThat(records).hasSize(5);
            Map<TopicPartitionOffset, Long> offsets = new HashMap<>();
            for (var record : records) {
                offsets.merge(new TopicPartitionOffset(record.topic(), record.partition()),
                        record.offset() + 1, Math::max);
            }
            reader.commitOffsets(offsets);
        }
        var replacement = state(topic);
        assertThat(replacement.streamName()).isEqualTo(original.streamName());
        assertThat(replacement.checkpointId()).isNotEqualTo(original.checkpointId());
        try (var reader = StreamingBootstrap.createSourceReader(replacement)) {
            assertThat(consume(reader, 5)).extracting(SourceRecord::offset).containsExactly(0L, 1L, 2L, 3L, 4L);
        }
    }

    private static List<SourceRecord> consume(CdcSourceReader reader, int count) {
        var records = new ArrayList<SourceRecord>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (records.size() < count && System.nanoTime() < deadline) {
            records.addAll(reader.pollBatch(10, Duration.ofSeconds(1)));
        }
        return records;
    }

    private static StreamState state(String topic) {
        return StreamState.builder().streamName("same-stream").tenantId("tenant")
                .lifecycleState(StreamLifecycleState.RUNNING).targetTable("catalog.db.target")
                .sourceFormat("debezium-json").sourceProperties(Map.of(
                        "bootstrap.servers", KAFKA.getBootstrapServers(), "topic", topic,
                        "auto.offset.reset", "earliest"))
                .createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build();
    }
}
