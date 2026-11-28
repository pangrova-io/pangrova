package org.pangrova.coordinator.query.stage;

import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.adaptive.PartitionGroup;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.ReadRange;

import java.util.List;
import java.util.Map;

/** Immutable mapping from physical consumer task IDs to the published inputs they read. */
public record ShuffleInputLayout(int originalCount, List<Consumer> consumers) {
    public static final ShuffleInputLayout DEFAULT = new ShuffleInputLayout(0, List.of());

    public ShuffleInputLayout {
        consumers = List.copyOf(consumers);
        int next = 0;
        Consumer previous = null;
        for (var consumer : consumers) {
            var group = consumer.partitions();
            if (group.startInclusive() == next) {
                next = group.endExclusive();
            } else if (previous == null || previous.ranges().isEmpty() || consumer.ranges().isEmpty()
                    || group.startInclusive() != previous.partitions().startInclusive()
                    || group.endExclusive() != next) {
                throw new IllegalArgumentException("Shuffle inputs must cover the original partitions in order");
            }
            previous = consumer;
        }
        if (next != originalCount) {
            throw new IllegalArgumentException("Shuffle inputs differ from the original partition count");
        }
    }

    /** Adapts the same whole-partition groups used by live and durable consumers. */
    public static ShuffleInputLayout coalesced(CoalesceResult inputs) {
        return inputs.isCoalesced() ? new ShuffleInputLayout(inputs.originalCount(), inputs.groups().stream()
                .map(group -> new Consumer(group, Map.of())).toList()) : DEFAULT;
    }

    /** Replaces one original partition's publication on each exchange listed in ranges. */
    public record Consumer(PartitionGroup partitions, Map<String, ReadRange> ranges) {
        public Consumer {
            ranges = Map.copyOf(ranges);
            if (!ranges.isEmpty() && partitions.partitionCount() != 1) {
                throw new IllegalArgumentException("Certified Shuffle ranges belong to one original partition");
            }
        }
    }
}
