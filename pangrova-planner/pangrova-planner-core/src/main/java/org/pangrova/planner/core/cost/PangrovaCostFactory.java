package org.pangrova.planner.core.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * Produces five-dimensional cost vectors for the Pangrova Cascades CBO.
 * Wired into VolcanoPlanner to replace Calcite's default scalar cost model.
 *
 * <p>The memory dimension is estimated as rows × bytesPerRow so that operators
 * with large intermediate state (hash builds, aggregations) incur a spill penalty
 * when they exceed the per-task memory budget. The network dimension captures
 * shuffle and broadcast transfer costs.</p>
 */
public final class PangrovaCostFactory implements RelOptCostFactory {

    /** Default bytes per row for memory estimation when no schema info is available. */
    static final double DEFAULT_BYTES_PER_ROW = 128.0;

    private final CostModelConfig config;

    public PangrovaCostFactory(CostModelConfig config) {
        this.config = config;
    }

    /**
     * Creates a cost with memory estimated from rows × bytesPerRow.
     * Network is left at zero for non-shuffle operators; use
     * {@link #makeCostWithShuffle} or {@link #makeCostWithBroadcast} for
     * operators that transfer data across the network.
     */
    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
        double memory = rowCount * DEFAULT_BYTES_PER_ROW;
        return new PangrovaRelOptCost(rowCount, cpu, io, memory, 0, config);
    }

    @Override
    public RelOptCost makeHugeCost() {
        return PangrovaRelOptCost.INFINITY;
    }

    @Override
    public RelOptCost makeInfiniteCost() {
        return PangrovaRelOptCost.INFINITY;
    }

    @Override
    public RelOptCost makeTinyCost() {
        return new PangrovaRelOptCost(1, 1, 0, 0, 0, config);
    }

    @Override
    public RelOptCost makeZeroCost() {
        return PangrovaRelOptCost.ZERO;
    }

    /** Creates a full 5-dimensional cost with memory and network dimensions. */
    public PangrovaRelOptCost makeFiveDimCost(double rows, double cpu, double io,
                                            double memory, double network) {
        return new PangrovaRelOptCost(rows, cpu, io, memory, network, config);
    }

    /**
     * Creates a cost for shuffle (hash-exchange) operators.
     * Network cost = rows × bytesPerRow × networkCostPerByte.
     */
    public PangrovaRelOptCost makeCostWithShuffle(double rows, double cpu, double io,
                                                double memory) {
        double network = rows * DEFAULT_BYTES_PER_ROW * config.networkCostPerByte();
        return new PangrovaRelOptCost(rows, cpu, io, memory, network, config);
    }

    /**
     * Creates a cost for broadcast operators.
     * Network cost = rows × bytesPerRow × networkCostPerByte × fanout,
     * where fanout represents the number of target nodes receiving the broadcast.
     */
    public PangrovaRelOptCost makeCostWithBroadcast(double rows, double cpu, double io,
                                                  double memory, int fanout) {
        double network = rows * DEFAULT_BYTES_PER_ROW * config.networkCostPerByte() * fanout;
        return new PangrovaRelOptCost(rows, cpu, io, memory, network, config);
    }

    public CostModelConfig config() {
        return config;
    }
}
