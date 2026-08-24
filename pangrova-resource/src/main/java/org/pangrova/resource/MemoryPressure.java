package org.pangrova.resource;

/**
 * Five-level memory pressure classification based on usage ratio.
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

    public static MemoryPressure fromRatio(double ratio) {
        if (ratio >= 0.95) return CRITICAL;
        if (ratio >= 0.85) return HIGH;
        if (ratio >= 0.70) return MEDIUM;
        if (ratio >= 0.50) return LOW;
        return NONE;
    }
}
