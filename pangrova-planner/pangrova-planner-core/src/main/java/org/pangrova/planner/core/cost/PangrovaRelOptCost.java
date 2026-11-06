package org.pangrova.planner.core.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;

/**
 * Five-dimensional cost vector for the Pangrova CBO: rows, cpu, io, memory, network.
 * Memory pressure causes non-linear cost jumps via spill penalty multiplier.
 */
public final class PangrovaRelOptCost implements RelOptCost {

    static final PangrovaRelOptCost INFINITY = new PangrovaRelOptCost(
            Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
            Double.MAX_VALUE, Double.MAX_VALUE, CostModelConfig.SSD_DEFAULT);
    static final PangrovaRelOptCost ZERO = new PangrovaRelOptCost(
            0, 0, 0, 0, 0, CostModelConfig.SSD_DEFAULT);

    private final double rows;
    private final double cpu;
    private final double io;
    private final double memory;
    private final double network;
    private final CostModelConfig config;

    PangrovaRelOptCost(double rows, double cpu, double io,
                            double memory, double network, CostModelConfig config) {
        this.rows = rows;
        this.cpu = cpu;
        this.io = io;
        this.memory = memory;
        this.network = network;
        this.config = config;
    }

    @Override public double getRows() { return rows; }
    @Override public double getCpu() { return cpu + network; }
    @Override public double getIo() { return io; }
    public double getMemory() { return memory; }
    public double getNetwork() { return network; }

    @Override
    public boolean isInfinite() {
        return this == INFINITY
                || rows == Double.MAX_VALUE
                || cpu == Double.MAX_VALUE
                || io == Double.MAX_VALUE;
    }

    @Override
    public boolean isLe(RelOptCost other) {
        if (other instanceof PangrovaRelOptCost a) {
            return effectiveCost(this) <= effectiveCost(a);
        }
        return getRows() <= other.getRows();
    }

    @Override
    public boolean isLt(RelOptCost other) {
        if (other instanceof PangrovaRelOptCost a) {
            return effectiveCost(this) < effectiveCost(a);
        }
        return getRows() < other.getRows();
    }

    @Override
    public boolean equals(RelOptCost other) {
        if (other instanceof PangrovaRelOptCost a) {
            return Double.compare(effectiveCost(this), effectiveCost(a)) == 0;
        }
        return false;
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        if (other instanceof PangrovaRelOptCost a) {
            return Math.abs(effectiveCost(this) - effectiveCost(a)) < RelOptUtil.EPSILON;
        }
        return false;
    }

    @Override
    public RelOptCost plus(RelOptCost other) {
        if (other instanceof PangrovaRelOptCost a) {
            return new PangrovaRelOptCost(
                    rows + a.rows, cpu + a.cpu, io + a.io,
                    Math.max(memory, a.memory), network + a.network, config);
        }
        return new PangrovaRelOptCost(
                rows + other.getRows(), cpu + other.getCpu(), io + other.getIo(),
                memory, network, config);
    }

    @Override
    public RelOptCost minus(RelOptCost other) {
        if (other instanceof PangrovaRelOptCost a) {
            return new PangrovaRelOptCost(
                    rows - a.rows, cpu - a.cpu, io - a.io,
                    memory, network - a.network, config);
        }
        return new PangrovaRelOptCost(
                rows - other.getRows(), cpu - other.getCpu(), io - other.getIo(),
                memory, network, config);
    }

    @Override
    public RelOptCost multiplyBy(double factor) {
        return new PangrovaRelOptCost(
                rows * factor, cpu * factor, io * factor,
                memory * factor, network * factor, config);
    }

    @Override
    public double divideBy(RelOptCost other) {
        double otherEffective = (other instanceof PangrovaRelOptCost a)
                ? effectiveCost(a) : other.getRows();
        return otherEffective > 0 ? effectiveCost(this) / otherEffective : 1.0;
    }

    @Override
    public String toString() {
        return String.format("{rows=%.1f, cpu=%.1f, io=%.1f, mem=%.1f, net=%.1f, eff=%.1f}",
                rows, cpu, io, memory, network, effectiveCost(this));
    }

    private static double effectiveCost(PangrovaRelOptCost c) {
        double spillMultiplier = (c.memory > c.config.availableMemoryPerTask())
                ? c.config.memorySpillPenalty() : 1.0;
        // Calcite's logical Join can report zero CPU/IO. Ignoring its rows
        // makes it free and prevents every physical join/exchange alternative
        // from ever winning the Volcano search.
        return (c.rows * c.config.cpuTupleCost()
                + c.cpu + c.io + c.network) * spillMultiplier;
    }
}
