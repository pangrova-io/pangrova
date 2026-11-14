package org.pangrova.planner.cost;

/**
 * Multi-dimensional cost model for the Pangrova query planner.
 *
 * <p>Provides base cost estimation across CPU, I/O, memory, and network
 * dimensions. Subclasses extend with distribution-aware or statistics-aware
 * cost estimation.</p>
 *
 * <p>Default implementations use simple heuristics. Override in subclasses
 * for workload-specific tuning.</p>
 */
public class PangrovaCostModel {

    /** Bytes per row estimate for scan cost calculation. */
    private static final double DEFAULT_BYTES_PER_ROW = 128.0;

    /** CPU cost per row for filter evaluation. */
    private static final double FILTER_CPU_PER_ROW = 0.5;

    /** CPU cost per row for projection. */
    private static final double PROJECT_CPU_PER_ROW = 0.2;

    /** CPU cost per row for aggregation. */
    private static final double AGGREGATE_CPU_PER_ROW = 1.5;

    /**
     * Estimate the base cost of processing the given number of rows.
     */
    public double estimateRowProcessingCost(long rows) {
        return rows;
    }

    /**
     * Estimate CPU cost for a table scan reading the given number of rows.
     */
    public double estimateScanCpuCost(long rows) {
        return rows;
    }

    /**
     * Estimate I/O cost for reading the given number of bytes from storage.
     */
    public double estimateIoCost(long bytes) {
        return bytes;
    }

    /**
     * Estimate I/O cost (in bytes) for scanning a table with the given row count.
     */
    public double estimateScanIoCost(long rows) {
        return rows * DEFAULT_BYTES_PER_ROW;
    }

    /**
     * Estimate CPU cost for evaluating a filter predicate over the given rows.
     */
    public double estimateFilterCpuCost(long rows) {
        return rows * FILTER_CPU_PER_ROW;
    }

    /**
     * Estimate CPU cost for projecting columns over the given rows.
     */
    public double estimateProjectCpuCost(long rows) {
        return rows * PROJECT_CPU_PER_ROW;
    }

    /**
     * Estimate CPU cost for aggregation over the given rows.
     */
    public double estimateAggregateCpuCost(long rows) {
        return rows * AGGREGATE_CPU_PER_ROW;
    }

    /**
     * Estimate memory cost for a hash join build side with the given row count.
     */
    public double estimateHashBuildMemory(long buildRows) {
        return buildRows * DEFAULT_BYTES_PER_ROW;
    }
}
