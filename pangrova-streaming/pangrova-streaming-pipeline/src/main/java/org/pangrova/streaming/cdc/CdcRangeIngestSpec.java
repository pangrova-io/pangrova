package org.pangrova.streaming.cdc;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Immutable Worker-side execution request for one exact set of CDC source intervals. */
@Experimental
public record CdcRangeIngestSpec(
        String streamId,
        String sourceEpoch,
        List<CdcTaskRange> ranges,
        int maxRecordsPerPoll,
        Duration pollTimeout,
        Duration idleTimeout) {

    public CdcRangeIngestSpec {
        if (streamId == null || streamId.isBlank() || sourceEpoch == null || sourceEpoch.isBlank()) {
            throw new IllegalArgumentException("CDC range ingest requires an exact stream and source epoch");
        }
        List<CdcTaskRange> canonical = new ArrayList<>(Objects.requireNonNull(ranges, "ranges"));
        if (canonical.isEmpty() || maxRecordsPerPoll <= 0
                || pollTimeout == null || pollTimeout.isNegative() || pollTimeout.isZero()
                || idleTimeout == null || idleTimeout.isNegative() || idleTimeout.isZero()) {
            throw new IllegalArgumentException("CDC range ingest requires bounded ranges and positive timeouts");
        }
        canonical.sort(Comparator.comparing(CdcTaskRange::topic)
                .thenComparingInt(CdcTaskRange::partition)
                .thenComparingLong(CdcTaskRange::startOffset)
                .thenComparingLong(CdcTaskRange::endOffset));
        CdcTaskRange previous = null;
        for (CdcTaskRange range : canonical) {
            if (previous != null && previous.topic().equals(range.topic())
                    && previous.partition() == range.partition()
                    && range.startOffset() < previous.endOffset()) {
                throw new IllegalArgumentException(
                        "CDC range ingest contains overlapping intervals on one partition");
            }
            previous = range;
        }
        ranges = List.copyOf(canonical);
    }
}
