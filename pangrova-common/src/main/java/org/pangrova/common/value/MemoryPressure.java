package org.pangrova.common.value;

/**
 * Five-level memory pressure classification based on memory usage ratio.
 *
 * <p>Shared across the control plane: the worker classifies its local memory
 * state, the scheduler uses the level for placement, and the coordinator derives
 * cluster pressure from worker heartbeats. Lives in {@code common} because
 * scheduler and worker must not depend on each other.</p>
 */
public enum MemoryPressure {

    NONE(0.0, 0.50),
    LOW(0.50, 0.70),
    MEDIUM(0.70, 0.85),
    HIGH(0.85, 0.95),
    CRITICAL(0.95, 1.0);

    private final double lowerBound;
    private final double upperBound;

    MemoryPressure(double lowerBound, double upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public double getLowerBound() { return lowerBound; }
    public double getUpperBound() { return upperBound; }

    /**
     * Classifies a memory usage ratio into the level whose bounds contain it.
     *
     * @param ratio memory usage ratio, normally in {@code [0.0, 1.0]}
     * @return the matching level, saturating at {@link #NONE} below the first bound
     */
    public static MemoryPressure fromRatio(double ratio) {
        if (ratio >= CRITICAL.getLowerBound()) return CRITICAL;
        if (ratio >= HIGH.getLowerBound()) return HIGH;
        if (ratio >= MEDIUM.getLowerBound()) return MEDIUM;
        if (ratio >= LOW.getLowerBound()) return LOW;
        return NONE;
    }
}
