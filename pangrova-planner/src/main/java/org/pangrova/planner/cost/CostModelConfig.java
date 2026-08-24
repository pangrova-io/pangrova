package org.pangrova.planner.cost;

/**
 * Configurable cost model parameters for the Pangrova query optimizer.
 * Controls per-operator cost weights for CPU, I/O, network, and memory spill.
 *
 * @param cpuTupleCost          cost to process one row
 * @param cpuOperatorCost       cost per operator invocation (compare, hash, eval)
 * @param seqIoCostPerByte      sequential read cost per byte
 * @param randomIoCostPerByte   random read cost per byte
 * @param networkCostPerByte    network transfer cost per byte
 * @param hashBuildCostPerRow   hash join build-side per-row cost
 * @param hashProbeCostPerRow   hash join probe-side per-row cost
 * @param sortCostPerRow        sort per-row cost (log factor applied separately)
 * @param memorySpillPenalty    multiplier when hash table exceeds available memory
 * @param availableMemoryPerTask per-task memory budget in bytes
 */
public record CostModelConfig(
        double cpuTupleCost,
        double cpuOperatorCost,
        double seqIoCostPerByte,
        double randomIoCostPerByte,
        double networkCostPerByte,
        double hashBuildCostPerRow,
        double hashProbeCostPerRow,
        double sortCostPerRow,
        double memorySpillPenalty,
        long availableMemoryPerTask
) {

    /** Default profile: SSD storage + 25Gbps network. */
    public static final CostModelConfig SSD_DEFAULT = new CostModelConfig(
            0.01, 0.005, 0.000122, 0.000150, 0.000300,
            0.03, 0.015, 0.04, 10.0, 256L * 1024 * 1024);

    /** Profile: S3/OSS remote object storage. */
    public static final CostModelConfig REMOTE_STORAGE = new CostModelConfig(
            0.01, 0.005, 0.00122, 0.00244, 0.000300,
            0.03, 0.015, 0.04, 10.0, 256L * 1024 * 1024);
}
