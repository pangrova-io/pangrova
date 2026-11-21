package org.pangrova.streaming.cdc;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Splits admitted source offsets into deterministic bounded Worker ranges.
 *
 * <p>Starts and ends are exclusive-end boundaries captured by the admission owner.
 * The allocator never invents offsets, merges partitions, or drops a gap: each
 * emitted range is a contiguous subset of one admitted partition interval.</p>
 */
@Experimental
public final class CdcRangeAllocator {

    private final Map<TopicPartitionOffset, Long> startOffsets;
    private final Map<TopicPartitionOffset, Long> endOffsets;
    private final long maxRecordsPerRange;

    /** Creates an allocator over matching admission-start and exclusive-end maps. */
    public CdcRangeAllocator(
            Map<TopicPartitionOffset, Long> startOffsets,
            Map<TopicPartitionOffset, Long> endOffsets,
            long maxRecordsPerRange) {
        this.startOffsets = Map.copyOf(Objects.requireNonNull(startOffsets, "startOffsets"));
        this.endOffsets = Map.copyOf(Objects.requireNonNull(endOffsets, "endOffsets"));
        if (maxRecordsPerRange <= 0
                || !this.startOffsets.keySet().equals(this.endOffsets.keySet())) {
            throw new IllegalArgumentException(
                    "CDC range allocation requires matching partitions and a positive range bound");
        }
        this.maxRecordsPerRange = maxRecordsPerRange;
        for (var entry : this.startOffsets.entrySet()) {
            long start = Objects.requireNonNull(entry.getValue(), "start offset");
            long end = Objects.requireNonNull(this.endOffsets.get(entry.getKey()), "end offset");
            if (start < 0 || end < start) {
                throw new IllegalArgumentException(
                        "CDC range allocation has an invalid interval for " + entry.getKey());
            }
        }
    }

    /** Returns every contiguous bounded range in topic/partition/offset order. */
    public List<CdcTaskRange> allocate() {
        List<CdcTaskRange> ranges = new ArrayList<>();
        entries().forEach(entry -> {
            TopicPartitionOffset partition = entry.getKey();
            long start = entry.getValue();
            long end = endOffsets.get(partition);
            while (start < end) {
                long next = end - start > maxRecordsPerRange
                        ? start + maxRecordsPerRange : end;
                ranges.add(new CdcTaskRange(
                        partition.topic(), partition.partition(), start, next));
                start = next;
            }
        });
        return List.copyOf(ranges);
    }

    /** Groups allocated ranges into deterministic Worker tasks. */
    public List<List<CdcTaskRange>> tasks(int rangesPerTask) {
        if (rangesPerTask <= 0) {
            throw new IllegalArgumentException("rangesPerTask must be positive");
        }
        List<CdcTaskRange> ranges = allocate();
        List<List<CdcTaskRange>> tasks = new ArrayList<>();
        for (int index = 0; index < ranges.size(); index += rangesPerTask) {
            tasks.add(List.copyOf(ranges.subList(
                    index, Math.min(ranges.size(), index + rangesPerTask))));
        }
        return List.copyOf(tasks);
    }

    private List<Map.Entry<TopicPartitionOffset, Long>> entries() {
        return startOffsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator
                        .comparing(TopicPartitionOffset::topic)
                        .thenComparingInt(TopicPartitionOffset::partition)))
                .toList();
    }
}
