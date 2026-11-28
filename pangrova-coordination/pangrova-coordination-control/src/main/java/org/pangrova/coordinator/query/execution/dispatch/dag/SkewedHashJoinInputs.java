package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.stage.ShuffleInputLayout;
import org.pangrova.coordinator.query.stage.ShuffleInputLayout.Consumer;
import org.pangrova.planner.adaptivequery.AdaptiveQueryConfig;
import org.pangrova.planner.analysis.PlanFragment.AlignedHashJoin;
import org.pangrova.planner.api.adaptivequery.CoalesceResult;
import org.pangrova.planner.api.adaptivequery.PartitionGroup;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.Partition;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.ReadRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Chooses complete, certified batch intervals using the frozen publications of an aligned hash join. */
final class SkewedHashJoinInputs {
    private static final int MAX_TASKS_PER_BUCKET = ShuffleCommitReceipt.MAX_READ_RANGES_PER_PARTITION;
    private static final int MIN_PEAK_REDUCTION_DIVISOR = 4;

    private SkewedHashJoinInputs() {}

    static ShuffleInputLayout select(AlignedHashJoin join, ShuffleCommitReceipt left, ShuffleCommitReceipt right,
            CoalesceResult coalesced, AdaptiveQueryConfig config) {
        long leftThreshold = threshold(left, join.splitLeft(), config);
        long rightThreshold = threshold(right, join.splitRight(), config);
        if (leftThreshold == Long.MAX_VALUE && rightThreshold == Long.MAX_VALUE) {
            return ShuffleInputLayout.coalesced(coalesced);
        }
        int originalCount = left.partitions().size();
        // Allow one maximally split bucket even in a narrow stage. In a wide stage
        // limit additional tasks to the original cohort size.
        int extraTasks = Math.max(originalCount, MAX_TASKS_PER_BUCKET - 1);
        var groups = groups(left, right, coalesced);
        var hot = new ArrayList<Bucket>();
        for (var group : groups) {
            if (group.partitionCount() != 1) continue;
            int id = group.startInclusive();
            var l = left.partitions().get(id);
            var r = right.partitions().get(id);
            var candidate = choose(l, r, leftThreshold, rightThreshold, MAX_TASKS_PER_BUCKET - 1);
            if (candidate != null) hot.add(new Bucket(id, l, r, candidate));
        }
        // Spend the bounded expansion first on buckets with the most bytes in their original task.
        hot.sort(Comparator.comparingLong(Bucket::originalBytes).reversed()
                .thenComparingInt(Bucket::partitionId));
        var selected = new HashMap<Integer, Candidate>();
        for (var bucket : hot) {
            if (extraTasks == 0) break;
            var candidate = bucket.candidate();
            if (candidate.tasks() - 1 > extraTasks) {
                candidate = choose(bucket.left(), bucket.right(), leftThreshold, rightThreshold, extraTasks);
            }
            if (candidate == null) continue;
            selected.put(bucket.partitionId(), candidate);
            extraTasks -= candidate.tasks() - 1;
        }
        if (selected.isEmpty()) return ShuffleInputLayout.coalesced(coalesced);
        var consumers = new ArrayList<Consumer>(groups.size());
        for (var group : groups) {
            int id = group.startInclusive();
            var candidate = selected.get(id);
            if (candidate == null) {
                consumers.add(new Consumer(group, Map.of()));
            } else {
                candidate.append(join, id, left.partitions().get(id), right.partitions().get(id), consumers);
            }
        }
        return new ShuffleInputLayout(originalCount, consumers);
    }

    private static List<PartitionGroup> groups(ShuffleCommitReceipt left, ShuffleCommitReceipt right,
            CoalesceResult coalesced) {
        if (coalesced.isCoalesced()) return coalesced.groups();
        var groups = new ArrayList<PartitionGroup>(left.partitions().size());
        for (int id = 0; id < left.partitions().size(); id++) {
            groups.add(new PartitionGroup(id, id + 1,
                    sum(left.partitions().get(id).bytes(), right.partitions().get(id).bytes())));
        }
        return groups;
    }

    private static long threshold(ShuffleCommitReceipt receipt, boolean allowed, AdaptiveQueryConfig config) {
        // A bucket over the coalescing target is already isolated by the adjacent grouping rule.
        long floor = Math.max(config.skewPartitionMinBytes(), config.targetPartitionBytes());
        if (!allowed || receipt.partitions().values().stream()
                .noneMatch(partition -> partition.bytes() > floor && !partition.readRanges().isEmpty())) {
            return Long.MAX_VALUE;
        }
        long[] bytes = receipt.partitions().values().stream().mapToLong(Partition::bytes).toArray();
        Arrays.sort(bytes);
        int middle = bytes.length / 2;
        double median = (bytes.length & 1) == 0
                ? bytes[middle - 1] + (bytes[middle] - bytes[middle - 1]) / 2.0 : bytes[middle];
        return Math.max(floor, (long) (median * config.skewPartitionThresholdRatio()));
    }

    private static Candidate choose(Partition left, Partition right, long leftThreshold, long rightThreshold,
            int extraTasks) {
        var leftRanges = left.bytes() > leftThreshold ? left.readRanges() : List.<ReadRange>of();
        var rightRanges = right.bytes() > rightThreshold ? right.readRanges() : List.<ReadRange>of();
        Candidate best = null;
        if (!leftRanges.isEmpty()) {
            best = candidate(left, right, leftRanges, List.of(), extraTasks);
        }
        if (!rightRanges.isEmpty()) {
            best = better(best, candidate(left, right, List.of(), rightRanges, extraTasks));
        }
        if (!leftRanges.isEmpty() && !rightRanges.isEmpty()) {
            best = better(best, candidate(left, right, leftRanges, rightRanges, extraTasks));
        }
        return best;
    }

    private static Candidate candidate(Partition left, Partition right,
            List<ReadRange> leftRanges, List<ReadRange> rightRanges, int extraTasks) {
        int leftCount = Math.max(1, leftRanges.size());
        int rightCount = Math.max(1, rightRanges.size());
        int tasks = leftCount * rightCount;
        if (tasks > MAX_TASKS_PER_BUCKET || tasks - 1 > extraTasks) return null;
        long original = sum(left.bytes(), right.bytes());
        long extra = sum(multiply(left.bytes(), rightCount - 1), multiply(right.bytes(), leftCount - 1));
        long peak = sum(peak(left, leftRanges), peak(right, rightRanges));
        return extra <= original && peak <= original - original / MIN_PEAK_REDUCTION_DIVISOR
                ? new Candidate(leftRanges, rightRanges, tasks, extra, peak) : null;
    }

    private static Candidate better(Candidate current, Candidate alternative) {
        if (current == null) return alternative;
        if (alternative == null) return current;
        int compare = Long.compare(alternative.peakBytes(), current.peakBytes());
        if (compare == 0) compare = Long.compare(alternative.extraBytes(), current.extraBytes());
        if (compare == 0) compare = Integer.compare(alternative.tasks(), current.tasks());
        return compare < 0 ? alternative : current;
    }

    private static long peak(Partition partition, List<ReadRange> ranges) {
        return ranges.stream().mapToLong(ReadRange::bytes).max().orElse(partition.bytes());
    }

    private static long sum(long left, long right) {
        return left + Math.min(Long.MAX_VALUE - left, right);
    }

    private static long multiply(long bytes, int copies) {
        return copies == 0 ? 0 : bytes > Long.MAX_VALUE / copies ? Long.MAX_VALUE : bytes * copies;
    }

    private record Bucket(int partitionId, Partition left, Partition right, Candidate candidate) {
        long originalBytes() {
            return sum(left.bytes(), right.bytes());
        }
    }

    private record Candidate(List<ReadRange> left, List<ReadRange> right, int tasks, long extraBytes, long peakBytes) {
        void append(AlignedHashJoin join, int id, Partition leftPartition, Partition rightPartition,
                List<Consumer> consumers) {
            for (int leftIndex = 0; leftIndex < Math.max(1, left.size()); leftIndex++) {
                ReadRange l = left.isEmpty() ? null : left.get(leftIndex);
                for (int rightIndex = 0; rightIndex < Math.max(1, right.size()); rightIndex++) {
                    ReadRange r = right.isEmpty() ? null : right.get(rightIndex);
                    var group = new PartitionGroup(id, id + 1,
                            sum(l == null ? leftPartition.bytes() : l.bytes(),
                                    r == null ? rightPartition.bytes() : r.bytes()));
                    consumers.add(new Consumer(group, ranges(join, l, r)));
                }
            }
        }

        private Map<String, ReadRange> ranges(AlignedHashJoin join, ReadRange leftRange, ReadRange rightRange) {
            if (leftRange == null) return Map.of(join.rightExchangeId(), rightRange);
            if (rightRange == null) return Map.of(join.leftExchangeId(), leftRange);
            return Map.of(join.leftExchangeId(), leftRange, join.rightExchangeId(), rightRange);
        }
    }
}
