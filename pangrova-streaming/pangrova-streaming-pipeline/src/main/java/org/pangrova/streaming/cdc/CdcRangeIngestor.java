package org.pangrova.streaming.cdc;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.SourceUnavailableException;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.exception.CdcIngestCancelledException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Executes one bounded CDC range without committing source offsets or publishing the target.
 *
 * <p>The reader is positioned at every admitted partition start. Records outside the
 * exact intervals are ignored, and an offset at or past an exclusive end proves that
 * interval exhausted. The caller owns the reader and writer lifecycle; this class
 * neither acknowledges offsets nor claims catalog publication.</p>
 */
@Experimental
public final class CdcRangeIngestor {

    private final CdcRangeIngestSpec spec;
    private final CdcSourceReader reader;
    private final CdcRecordConverter converter;
    private final CdcRangeBatchWriter writer;
    private final BooleanSupplier cancelled;

    /** Creates a one-shot ingestor over the exact admitted ranges and caller-owned resources. */
    public CdcRangeIngestor(
            CdcRangeIngestSpec spec,
            CdcSourceReader reader,
            CdcRecordConverter converter,
            CdcRangeBatchWriter writer,
            BooleanSupplier cancelled) {
        this.spec = Objects.requireNonNull(spec, "spec");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.cancelled = Objects.requireNonNull(cancelled, "cancelled");
    }

    /** Reads through every admitted interval and stages the decoded batches without publishing. */
    public CdcRangeIngestResult ingest() {
        Map<TopicPartitionOffset, List<Cursor>> cursors = new HashMap<>();
        Map<TopicPartitionOffset, Long> starts = new HashMap<>();
        int cursorCount = 0;
        for (CdcTaskRange range : spec.ranges()) {
            cursors.computeIfAbsent(range.partitionKey(), ignored -> new ArrayList<>())
                    .add(new Cursor(range));
            starts.putIfAbsent(range.partitionKey(), range.startOffset());
            cursorCount++;
        }
        reader.seekToOffsets(Map.copyOf(starts));

        long consumed = 0;
        long decoded = 0;
        List<byte[]> fragments = new ArrayList<>();
        Set<Cursor> completed = new HashSet<>();
        long lastProgress = System.nanoTime();
        long idleNanos = spec.idleTimeout().toNanos();

        while (completed.size() < cursorCount) {
            requireNotCancelled();
            List<SourceRecord> batch = reader.pollBatch(
                    spec.maxRecordsPerPoll(), spec.pollTimeout());
            if (batch == null) {
                throw new IllegalStateException("CDC source returned a null batch");
            }
            if (batch.isEmpty()) {
                if (completeExhaustedRanges(cursors, completed)) {
                    lastProgress = System.nanoTime();
                    continue;
                }
                if (System.nanoTime() - lastProgress >= idleNanos) {
                    throw new SourceUnavailableException(
                            PangrovaErrorCode.STREAM_SOURCE_UNAVAILABLE,
                            "CDC source produced no admitted records before the idle deadline for stream "
                                    + spec.streamId());
                }
                continue;
            }
            List<CdcEnvelope> envelopes = new ArrayList<>();
            boolean progressed = false;
            for (SourceRecord record : batch) {
                List<Cursor> partitionCursors = cursors.get(
                        new TopicPartitionOffset(record.topic(), record.partition()));
                if (partitionCursors == null) {
                    continue;
                }
                for (Cursor cursor : partitionCursors) {
                    if (completed.contains(cursor)) {
                        continue;
                    }
                    if (record.offset() < cursor.startOffset) {
                        break;
                    }
                    if (record.offset() >= cursor.endOffset) {
                        completed.add(cursor);
                        progressed = true;
                        continue;
                    }
                    if (record.offset() < cursor.nextOffset) {
                        break;
                    }
                    if (record.offset() > cursor.nextOffset) {
                        // Source logs may contain compaction gaps inside one admitted interval.
                        cursor.nextOffset = record.offset();
                    }
                    if (record.value() == null) {
                        throw new IllegalStateException(
                                "CDC record has no payload at " + cursor.key + "@" + record.offset());
                    }
                    CdcEnvelope envelope = Objects.requireNonNull(
                            converter.deserialize(record.value()),
                            "CDC converter returned a null envelope");
                    envelopes.add(envelope);
                    consumed++;
                    decoded++;
                    cursor.nextOffset = record.offset() + 1;
                    progressed = true;
                    if (cursor.nextOffset >= cursor.endOffset) {
                        completed.add(cursor);
                    }
                    break;
                }
            }
            if (!envelopes.isEmpty()) {
                requireNotCancelled();
                List<byte[]> actual = writer.writeBatch(List.copyOf(envelopes));
                if (actual == null || actual.stream().anyMatch(Objects::isNull)) {
                    throw new IllegalStateException("CDC batch writer returned a null commit fragment");
                }
                actual.forEach(fragment -> fragments.add(fragment.clone()));
            }
            if (progressed) {
                lastProgress = System.nanoTime();
            }
        }
        return new CdcRangeIngestResult(consumed, decoded, fragments);
    }

    private boolean completeExhaustedRanges(
            Map<TopicPartitionOffset, List<Cursor>> cursors,
            Set<Cursor> completed) {
        List<TopicPartitionOffset> remaining = cursors.entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(
                        cursor -> !completed.contains(cursor)))
                .map(Map.Entry::getKey)
                .toList();
        if (remaining.isEmpty()) {
            return false;
        }
        Map<TopicPartitionOffset, Long> endOffsets;
        try {
            endOffsets = reader.endOffsets(remaining);
        } catch (UnsupportedOperationException unavailable) {
            return false;
        }
        boolean progressed = false;
        for (TopicPartitionOffset key : remaining) {
            Long sourceEnd = endOffsets.get(key);
            if (sourceEnd == null) {
                throw new IllegalStateException(
                        "CDC source omitted its exact end offset for " + key);
            }
            for (Cursor cursor : cursors.get(key)) {
                if (!completed.contains(cursor) && sourceEnd <= cursor.nextOffset) {
                    completed.add(cursor);
                    progressed = true;
                }
            }
        }
        return progressed;
    }

    private void requireNotCancelled() {
        if (cancelled.getAsBoolean()) {
            throw new CdcIngestCancelledException(spec.streamId());
        }
    }

    private static final class Cursor {
        private final TopicPartitionOffset key;
        private final long startOffset;
        private final long endOffset;
        private long nextOffset;

        private Cursor(CdcTaskRange range) {
            this.key = range.partitionKey();
            this.startOffset = range.startOffset();
            this.endOffset = range.endOffset();
            this.nextOffset = range.startOffset();
        }
    }
}
