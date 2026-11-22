package org.pangrova.streaming.cdc;

import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.streaming.exception.CdcIngestCancelledException;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcRangeIngestorTest {

    private static final CdcRecordConverter CONVERTER = new CdcRecordConverter() {
        @Override
        public String formatId() {
            return "fixture";
        }

        @Override
        public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(CdcOperation.INSERT, "target",
                    null, CdcRow.of(Map.of("payload", new String(payload, StandardCharsets.UTF_8))),
                    Instant.EPOCH, 0, Map.of());
        }
    };

    @Test
    void ingestsOnlyAdmittedRangesAndNeverAcknowledgesSourceOffsets() {
        BatchReader reader = new BatchReader(List.of(
                List.of(record("orders", 0, 9), record("orders", 0, 10),
                        record("orders", 0, 11), record("orders", 0, 14)),
                List.of(record("orders", 1, 20), record("orders", 1, 21),
                        record("orders", 1, 22)),
                List.of(record("other", 0, 0))));
        List<List<CdcEnvelope>> written = new ArrayList<>();
        CdcRangeIngestor ingestor = new CdcRangeIngestor(
                spec(), reader, CONVERTER, envelopes -> {
                    written.add(List.copyOf(envelopes));
                    return List.of(new byte[]{(byte) written.size()});
                }, () -> false);

        CdcRangeIngestResult result = ingestor.ingest();

        assertThat(reader.seeks).containsExactlyInAnyOrderEntriesOf(Map.of(
                new TopicPartitionOffset("orders", 0), 10L,
                new TopicPartitionOffset("orders", 1), 20L));
        assertThat(reader.commits).isZero();
        assertThat(result.consumedRecordCount()).isEqualTo(4);
        assertThat(result.decodedEnvelopeCount()).isEqualTo(4);
        assertThat(result.commitFragments()).containsExactly(
                new byte[]{1}, new byte[]{2});
        assertThat(written).hasSize(2);
        assertThat(written.get(0).stream()
                .map(envelope -> envelope.after().getString("payload")).toList())
                .containsExactly("10", "11");
        assertThat(written.get(1).stream()
                .map(envelope -> envelope.after().getString("payload")).toList())
                .containsExactly("20", "21");
    }

    @Test
    void cancellationStopsBeforeAnotherPollAndReturnsNoSyntheticSuccess() {
        AtomicBoolean written = new AtomicBoolean();
        BatchReader reader = new BatchReader(List.of(
                List.of(record("orders", 0, 10), record("orders", 0, 14)),
                List.of(record("orders", 1, 20), record("orders", 1, 22))));
        CdcRangeIngestor ingestor = new CdcRangeIngestor(
                spec(), reader, CONVERTER, envelopes -> {
                    written.set(true);
                    return List.of();
                }, written::get);

        assertThatThrownBy(ingestor::ingest)
                .isInstanceOf(CdcIngestCancelledException.class);
        assertThat(written).isTrue();
        assertThat(reader.commits).isZero();
    }

    @Test
    void orderedRangeStagesOneFragmentAcrossPollBoundaries() {
        var reader = new BatchReader(List.of(List.of(record("orders", 0, 10)),
                List.of(record("orders", 0, 11))));
        List<List<CdcEnvelope>> staged = new ArrayList<>();
        var ingestor = new CdcRangeIngestor(new CdcRangeIngestSpec("stream", "epoch",
                List.of(new CdcTaskRange("orders", 0, 10, 12)),
                1, Duration.ofMillis(1), Duration.ofSeconds(1)),
                reader, CONVERTER, batch -> {
                    staged.add(batch);
                    return List.of(new byte[]{1});
                }, () -> false);

        assertThat(ingestor.ingest(true).commitFragments()).containsExactly(new byte[]{1});
        assertThat(staged).singleElement().satisfies(batch -> assertThat(batch)
                .extracting(envelope -> envelope.after().getString("payload")).containsExactly("10", "11"));
        assertThat(reader.commits).isZero();
    }

    @Test
    void emptyIntervalAtTheCurrentLogEndCompletesWithoutIdleFailure() {
        LogEndReader reader = new LogEndReader(10L);
        CdcRangeIngestor ingestor = new CdcRangeIngestor(
                new CdcRangeIngestSpec("stream", "epoch",
                        List.of(new CdcTaskRange("orders", 0, 10, 14)),
                        10, Duration.ofMillis(1), Duration.ofSeconds(1)),
                reader, CONVERTER, envelopes -> {
                    throw new AssertionError("Empty range must not write a batch");
                }, () -> false);

        CdcRangeIngestResult result = ingestor.ingest();

        assertThat(result.consumedRecordCount()).isZero();
        assertThat(result.commitFragments()).isEmpty();
        assertThat(reader.polls).isEqualTo(1);
    }

    @Test
    void ingestsMultipleDisjointIntervalsOnOnePartition() {
        BatchReader reader = new BatchReader(List.of(List.of(
                record("orders", 0, 10), record("orders", 0, 11),
                record("orders", 0, 15), record("orders", 0, 16))));
        CdcRangeIngestor ingestor = new CdcRangeIngestor(
                new CdcRangeIngestSpec("stream", "epoch",
                        List.of(new CdcTaskRange("orders", 0, 10, 12),
                                new CdcTaskRange("orders", 0, 15, 17)),
                        10, Duration.ofMillis(1), Duration.ofSeconds(1)),
                reader, CONVERTER, envelopes -> List.of(), () -> false);

        CdcRangeIngestResult result = ingestor.ingest();

        assertThat(result.consumedRecordCount()).isEqualTo(4);
        assertThat(reader.seeks).containsExactly(
                Map.entry(new TopicPartitionOffset("orders", 0), 10L));
    }

    @Test
    void rejectsOverlappingIntervalsOnOnePartition() {
        assertThatThrownBy(() -> new CdcRangeIngestSpec("stream", "epoch",
                List.of(new CdcTaskRange("orders", 0, 10, 25),
                        new CdcTaskRange("orders", 0, 20, 30)),
                10, Duration.ofMillis(1), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static CdcRangeIngestSpec spec() {
        return new CdcRangeIngestSpec("stream", "epoch",
                List.of(new CdcTaskRange("orders", 1, 20, 22),
                        new CdcTaskRange("orders", 0, 10, 14)),
                10, Duration.ofMillis(1), Duration.ofSeconds(1));
    }

    private static SourceRecord record(String topic, int partition, long offset) {
        return new SourceRecord(topic, partition, offset, null,
                Long.toString(offset).getBytes(StandardCharsets.UTF_8), 0L);
    }

    private static final class LogEndReader implements CdcSourceReader {
        private final long endOffset;
        private int polls;

        private LogEndReader(long endOffset) {
            this.endOffset = endOffset;
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            polls++;
            return List.of();
        }

        @Override
        public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError("Worker range ingestion must not acknowledge source offsets");
        }

        @Override
        public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
        }

        @Override
        public Map<TopicPartitionOffset, Long> endOffsets(
                java.util.Collection<TopicPartitionOffset> partitions) {
            Map<TopicPartitionOffset, Long> result = new java.util.HashMap<>();
            partitions.forEach(partition -> result.put(partition, endOffset));
            return Map.copyOf(result);
        }

        @Override
        public void close() {
        }
    }

    private static final class BatchReader implements CdcSourceReader {
        private final List<List<SourceRecord>> batches;
        private int nextBatch;
        private Map<TopicPartitionOffset, Long> seeks = Map.of();
        private int commits;

        private BatchReader(List<List<SourceRecord>> batches) {
            this.batches = List.copyOf(batches);
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            if (nextBatch >= batches.size()) {
                throw new AssertionError("Ingestor polled beyond the fixture batches");
            }
            return batches.get(nextBatch++);
        }

        @Override
        public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            commits++;
            throw new AssertionError("Worker range ingestion must not acknowledge source offsets");
        }

        @Override
        public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
            seeks = Map.copyOf(offsets);
        }

        @Override
        public Map<TopicPartitionOffset, Long> endOffsets(
                java.util.Collection<TopicPartitionOffset> partitions) {
            Map<TopicPartitionOffset, Long> result = new java.util.HashMap<>();
            for (TopicPartitionOffset partition : partitions) {
                result.put(partition, partition.topic().equals("orders")
                        ? partition.partition() == 0 ? 14L : 22L
                        : 0L);
            }
            return Map.copyOf(result);
        }

        @Override
        public OptionalLong currentLag() {
            return OptionalLong.empty();
        }

        @Override
        public void close() {
        }
    }
}
