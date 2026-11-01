package org.pangrova.spi.indexing.data;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.spi.indexing.data.spatial.BoundingBox;

/**
 * Property tests for {@link BoundingBox} immutability and spatial operations.
 *
 * <p>Validates: RT3 — accessor returns internal array without clone for zero-copy hot-path,
 * while construction-time defensive copy prevents external mutation of the source.</p>
 */
class BoundingBoxPropertyTest {

    @Property(tries = 10)
    void constructorDefensivelyCopiesInput(
            @ForAll @DoubleRange(min = -1000, max = 1000) double x,
            @ForAll @DoubleRange(min = -1000, max = 1000) double y) {
        double[] mins = {Math.min(x, y), Math.min(x, y)};
        double[] maxs = {Math.max(x, y), Math.max(x, y)};
        double[] origMins = mins.clone();

        var box = new BoundingBox(mins, maxs);
        mins[0] = Double.MAX_VALUE; // mutate source
        assertThat(box.minValues()[0]).isEqualTo(origMins[0]);
    }

    @Property(tries = 10)
    void accessorReturnsStableReference() {
        var box = new BoundingBox(new double[]{1.0, 2.0}, new double[]{3.0, 4.0});
        double[] first = box.minValues();
        double[] second = box.minValues();
        assertThat(first).isSameAs(second);

        double[] firstMax = box.maxValues();
        double[] secondMax = box.maxValues();
        assertThat(firstMax).isSameAs(secondMax);
    }

    @Property(tries = 10)
    void selfIntersectsAlways(
            @ForAll @DoubleRange(min = -100, max = 100) double x1,
            @ForAll @DoubleRange(min = -100, max = 100) double y1) {
        double lo = Math.min(x1, y1);
        double hi = Math.max(x1, y1);
        var box = new BoundingBox(new double[]{lo}, new double[]{hi});
        assertThat(box.mbrIntersects(box)).isTrue();
    }

    @Property(tries = 10)
    void selfContainsAlways(
            @ForAll @DoubleRange(min = -100, max = 100) double x1,
            @ForAll @DoubleRange(min = -100, max = 100) double y1) {
        double lo = Math.min(x1, y1);
        double hi = Math.max(x1, y1);
        var box = new BoundingBox(new double[]{lo}, new double[]{hi});
        assertThat(box.mbrContains(box)).isTrue();
    }

    @Property(tries = 10)
    void disjointBoxesDoNotIntersect(
            @ForAll @DoubleRange(min = 0, max = 10) double size) {
        var left = new BoundingBox(new double[]{0}, new double[]{size});
        var right = new BoundingBox(new double[]{size + 1}, new double[]{size + 10});
        assertThat(left.mbrIntersects(right)).isFalse();
    }

    @Property(tries = 10)
    void containsImpliesIntersects(
            @ForAll @DoubleRange(min = 0, max = 50) double innerSize,
            @ForAll @DoubleRange(min = 1, max = 50) double margin) {
        var inner = new BoundingBox(new double[]{0}, new double[]{innerSize});
        var outer = new BoundingBox(
                new double[]{-margin}, new double[]{innerSize + margin});
        assertThat(outer.mbrContains(inner)).isTrue();
        assertThat(outer.mbrIntersects(inner)).isTrue();
    }
}
