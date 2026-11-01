package org.pangrova.streaming.commit;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Exact source ranges for one publication, including every topic and partition. */
public record CdcBatchCheckpoint(List<OffsetRange> ranges) {
    /** Freeze ranges in canonical order and require one range per source partition. */
    public CdcBatchCheckpoint {
        Objects.requireNonNull(ranges, "ranges must not be null");
        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("A CDC checkpoint must contain at least one range");
        }
        ranges = List.copyOf(ranges.stream().sorted(Comparator.comparing(OffsetRange::topic)
                .thenComparingInt(OffsetRange::partition)).toList());
        var partitions = new HashSet<TopicPartitionOffset>();
        for (var range : ranges) {
            if (!partitions.add(new TopicPartitionOffset(range.topic(), range.partition()))) {
                throw new IllegalArgumentException("Duplicate topic/partition in CDC checkpoint");
            }
        }
    }

    /** Build the checkpoint from raw records before conversion can remove source identities. */
    public static CdcBatchCheckpoint fromRecords(List<SourceRecord> records) {
        Map<TopicPartitionOffset, OffsetRange> ranges = new HashMap<>();
        for (var record : records) {
            var key = new TopicPartitionOffset(record.topic(), record.partition());
            var range = new OffsetRange(record.topic(), record.partition(), record.offset(), record.offset());
            ranges.merge(key, range, (left, right) -> new OffsetRange(left.topic(), left.partition(),
                    Math.min(left.startOffset(), right.startOffset()),
                    Math.max(left.endOffset(), right.endOffset())));
        }
        return new CdcBatchCheckpoint(List.copyOf(ranges.values()));
    }

    /** Source positions to acknowledge or restore once this checkpoint is committed. */
    public Map<TopicPartitionOffset, Long> nextOffsets() {
        Map<TopicPartitionOffset, Long> offsets = new HashMap<>();
        for (var range : ranges) {
            offsets.put(new TopicPartitionOffset(range.topic(), range.partition()), range.endOffset() + 1);
        }
        return Map.copyOf(offsets);
    }

    /** Length-prefix topics so delimiters in source names cannot alias another checkpoint. */
    public String fingerprint() {
        var canonical = new StringBuilder();
        for (var range : ranges) {
            canonical.append(range.topic().length()).append(':').append(range.topic())
                    .append(':').append(range.partition()).append(':').append(range.startOffset())
                    .append(':').append(range.endOffset()).append(';');
        }
        return DigestUtils.sha256Hex(canonical.toString());
    }
}
