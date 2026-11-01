package org.pangrova.spi.indexing.data.spatial;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Arrays;

/**
 * Axis-aligned bounding box for spatial indexing.
 *
 * <p>Immutable. Both coordinate arrays are defensively copied on
 * construction. Accessors return the internal array directly for
 * performance — callers MUST NOT mutate the returned arrays.</p>
 *
 * @param minValues per-dimension minimum coordinates
 * @param maxValues per-dimension maximum coordinates
 */
@Experimental
public record BoundingBox(double[] minValues, double[] maxValues) {

    public BoundingBox {
        if (minValues == null || maxValues == null) {
            throw new IllegalArgumentException("minValues and maxValues must not be null");
        }
        if (minValues.length != maxValues.length) {
            throw new IllegalArgumentException(
                    "minValues and maxValues must have the same length");
        }
        minValues = minValues.clone();
        maxValues = maxValues.clone();
    }

    /**
     * Returns per-dimension minimum coordinates. Callers must not mutate the returned array.
     * Construction-time defensive copy guarantees immutability of the source.
     */
    @Override
    public double[] minValues() {
        return minValues;
    }

    /**
     * Returns per-dimension maximum coordinates. Callers must not mutate the returned array.
     * Construction-time defensive copy guarantees immutability of the source.
     */
    @Override
    public double[] maxValues() {
        return maxValues;
    }

    /**
     * Returns true if this bounding box intersects the other bounding box
     * (their projections overlap on every dimension).
     */
    public boolean mbrIntersects(BoundingBox other) {
        if (other == null) {
            return false;
        }
        if (minValues.length != other.minValues.length) {
            return false;
        }
        for (int i = 0; i < minValues.length; i++) {
            if (minValues[i] > other.maxValues[i] || maxValues[i] < other.minValues[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if this bounding box fully contains the other bounding box
     * (on every dimension, this box's range encloses the other's range).
     */
    public boolean mbrContains(BoundingBox other) {
        if (other == null) {
            return false;
        }
        if (minValues.length != other.minValues.length) {
            return false;
        }
        for (int i = 0; i < minValues.length; i++) {
            if (minValues[i] > other.minValues[i] || maxValues[i] < other.maxValues[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BoundingBox that)) return false;
        return Arrays.equals(minValues, that.minValues)
                && Arrays.equals(maxValues, that.maxValues);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(minValues) + Arrays.hashCode(maxValues);
    }

    @Override
    public String toString() {
        return "BoundingBox[min=" + Arrays.toString(minValues)
                + ", max=" + Arrays.toString(maxValues) + "]";
    }
}
