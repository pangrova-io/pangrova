package org.pangrova.planner.runtimefilter;

import org.pangrova.connector.api.enums.DynamicFilterType;
import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies runtime filters to prune partitions and row groups
 * whose value ranges do not overlap with the filter range.
 */
public final class RuntimeFilterApplicator {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeFilterApplicator.class);

    private final AtomicLong totalEvaluated = new AtomicLong();
    private final AtomicLong totalPruned = new AtomicLong();

    /**
     * Prune partitions whose value ranges fall entirely outside the filter range.
     * For BLOOM filters, evaluates partition column values against the Bloom filter.
     */
    public List<PartitionRange> prunePartitions(List<PartitionRange> partitions,
                                                RuntimeFilter filter) {
        return applyFilter(partitions, filter);
    }

    /**
     * Prune row groups whose column statistics do not overlap with the filter range.
     * Same logic as partition pruning applied to row group statistics.
     */
    public List<PartitionRange> pruneRowGroups(List<PartitionRange> rowGroups,
                                               RuntimeFilter filter) {
        return applyFilter(rowGroups, filter);
    }

    /** Total partitions/row groups evaluated across all calls. */
    public long totalEvaluated() {
        return totalEvaluated.get();
    }

    /** Total partitions/row groups pruned across all calls. */
    public long totalPruned() {
        return totalPruned.get();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<PartitionRange> applyFilter(List<PartitionRange> ranges,
                                             RuntimeFilter filter) {
        int inputSize = ranges.size();
        List<PartitionRange> result;

        if (filter.filterType() == DynamicFilterType.BLOOM) {
            result = applyBloomFilter(ranges, filter);
        } else {
            Comparable filterMin = filter.rawMin();
            Comparable filterMax = filter.rawMax();
            result = ranges.stream()
                    .filter(r -> overlaps(r, filterMin, filterMax))
                    .toList();
        }

        int pruned = inputSize - result.size();
        totalEvaluated.addAndGet(inputSize);
        totalPruned.addAndGet(pruned);

        if (pruned > 0) {
            LOG.debug("event=runtime_filter_pruning filter_type={} column={} evaluated={} pruned={}",
                    filter.filterType(), filter.column(), inputSize, pruned);
        }
        return result;
    }

    private List<PartitionRange> applyBloomFilter(List<PartitionRange> ranges,
                                                  RuntimeFilter filter) {
        SplitBlockBloomFilter bloom = filter.bloomFilter();
        if (bloom == null) {
            return List.copyOf(ranges);
        }
        return ranges.stream()
                .filter(r -> bloomMightContainPartition(r, bloom))
                .toList();
    }

    /**
     * Tests whether a partition might contain values present in the Bloom filter.
     * For partition columns, min == max (single partition value), so we test min.
     * For range partitions (min != max), we conservatively keep the partition
     * since the Bloom filter cannot prove absence across a range.
     */
    private boolean bloomMightContainPartition(PartitionRange range,
                                              SplitBlockBloomFilter bloom) {
        Object partitionMin = range.min();
        Object partitionMax = range.max();

        if (partitionMin == null) {
            return true;
        }

        // For single-value partitions (min == max), test the exact value
        if (partitionMin.equals(partitionMax)) {
            return bloom.mightContain(partitionMin.toString());
        }

        // For range partitions, conservatively keep — Bloom cannot prove
        // absence across a continuous range
        return true;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean overlaps(PartitionRange range,
                             Comparable filterMin, Comparable filterMax) {
        try {
            Comparable rangeMax = (Comparable) range.max();
            Comparable rangeMin = (Comparable) range.min();
            return rangeMax.compareTo(filterMin) >= 0
                    && rangeMin.compareTo(filterMax) <= 0;
        } catch (ClassCastException e) {
            LOG.warn("event=runtime_filter_type_mismatch filter_column={} reason={}",
                    filterMin.getClass().getSimpleName(), e.getMessage());
            return true;
        }
    }
}
