package org.pangrova.connector.api.commit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Connector-bound CDC source identity carried with a durable distributed commit. */
public record CdcSourceCommitIdentity(
        String streamId,
        String sourceEpoch,
        List<OffsetRange> ranges) {

    public CdcSourceCommitIdentity {
        if (streamId == null || streamId.isBlank()) {
            throw new IllegalArgumentException("CDC source identity requires a stream ID");
        }
        if (sourceEpoch == null || sourceEpoch.isBlank()) {
            throw new IllegalArgumentException("CDC source identity requires a source ownership epoch");
        }
        List<OffsetRange> canonical = new ArrayList<>(Objects.requireNonNull(ranges, "ranges"));
        if (canonical.isEmpty()) {
            throw new IllegalArgumentException("CDC source identity requires at least one bounded range");
        }
        canonical.sort(Comparator.comparing(OffsetRange::topic)
                .thenComparingInt(OffsetRange::partition)
                .thenComparingLong(OffsetRange::startOffset)
                .thenComparingLong(OffsetRange::endOffset));
        OffsetRange previous = null;
        for (OffsetRange range : canonical) {
            if (previous != null && previous.topic().equals(range.topic())
                    && previous.partition() == range.partition()
                    && range.startOffset() < previous.endOffset()) {
                throw new IllegalArgumentException(
                        "CDC source identity contains overlapping ranges on one partition");
            }
            previous = range;
        }
        ranges = List.copyOf(canonical);
    }

    /** Exact inclusive-start, exclusive-end offset interval of one topic partition. */
    public record OffsetRange(String topic, int partition, long startOffset, long endOffset) {
        public OffsetRange {
            if (topic == null || topic.isBlank() || partition < 0
                    || startOffset < 0 || endOffset <= startOffset) {
                throw new IllegalArgumentException(
                        "CDC offset range is not a bounded topic partition interval");
            }
        }
    }
}
