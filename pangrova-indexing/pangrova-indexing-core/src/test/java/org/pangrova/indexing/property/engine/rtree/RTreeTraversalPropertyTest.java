package org.pangrova.indexing.property.engine.rtree;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.indexing.engine.rtree.provider.RTreeStrategyProvider;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.data.IndexData;
import org.pangrova.spi.connector.index.predicate.SpatialOp;
import org.pangrova.spi.connector.index.predicate.SpatialQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link RTreeStrategyProvider} recursive traversal (RT2 fix).
 *
 * <p><b>Validates: Requirements 4.1, 4.2, 4.3, 4.4</b></p>
 *
 * <p>Core property (no false negatives): for any RTree built from random 2D points
 * and any SpatialQuery whose bounding box contains at least one indexed point,
 * {@code evaluate()} must return {@code KEEP}, never {@code SKIP}.</p>
 */
class RTreeTraversalPropertyTest {

    private final RTreeStrategyProvider provider = new RTreeStrategyProvider();

    // ── No-false-negative properties (task 4.2) ──────────────────────────

    /**
     * INTERSECTS: a query box built around a randomly chosen indexed point
     * (point ± epsilon) must produce KEEP.
     */
    @Property(tries = 10)
    void intersectsQueryAroundIndexedPointReturnsKeep(
            @ForAll("pointSets") List<double[]> points,
            @ForAll @DoubleRange(min = 0.01, max = 10.0) double epsilon) {

        IndexData indexData = buildTree(points);
        double[] chosen = points.get(points.size() / 2);

        SpatialQuery query = new SpatialQuery("geo",
                chosen[0] - epsilon, chosen[1] - epsilon,
                chosen[0] + epsilon, chosen[1] + epsilon,
                SpatialOp.INTERSECTS);

        assertThat(provider.evaluate(indexData, query))
                .as("INTERSECTS around point (%.2f,%.2f) ± %.2f with %d points",
                        chosen[0], chosen[1], epsilon, points.size())
                .isEqualTo(IndexEvalResult.KEEP);
    }

    /**
     * WITHIN: a query box that fully encloses the tree's MBR must produce KEEP,
     * because every leaf node is within the query box.
     */
    @Property(tries = 10)
    void withinQueryEnclosingAllPointsReturnsKeep(
            @ForAll("pointSets") List<double[]> points,
            @ForAll @DoubleRange(min = 1.0, max = 50.0) double margin) {

        IndexData indexData = buildTree(points);
        double[] mbr = computeMbr(points);

        SpatialQuery query = new SpatialQuery("geo",
                mbr[0] - margin, mbr[1] - margin,
                mbr[2] + margin, mbr[3] + margin,
                SpatialOp.WITHIN);

        assertThat(provider.evaluate(indexData, query))
                .as("WITHIN query enclosing all %d points with margin %.2f",
                        points.size(), margin)
                .isEqualTo(IndexEvalResult.KEEP);
    }

    /**
     * CONTAINS: a query box that is a single point matching an indexed point
     * must produce KEEP, because the leaf MBR contains that point.
     */
    @Property(tries = 10)
    void containsQueryAtIndexedPointReturnsKeep(
            @ForAll("pointSets") List<double[]> points) {

        IndexData indexData = buildTree(points);
        double[] chosen = points.get(0);

        // Query box is exactly the chosen point — the leaf's MBR contains it
        SpatialQuery query = new SpatialQuery("geo",
                chosen[0], chosen[1], chosen[0], chosen[1],
                SpatialOp.CONTAINS);

        assertThat(provider.evaluate(indexData, query))
                .as("CONTAINS query at point (%.2f,%.2f) with %d points",
                        chosen[0], chosen[1], points.size())
                .isEqualTo(IndexEvalResult.KEEP);
    }

    /**
     * All three SpatialOp values tested together: pick a random point,
     * build a tight box around it, and verify KEEP for INTERSECTS.
     * Build a box enclosing the full MBR for WITHIN.
     * Build a point-box at the chosen point for CONTAINS.
     */
    @Property(tries = 10)
    void noFalseNegativesForAllOps(
            @ForAll("pointSets") List<double[]> points,
            @ForAll @DoubleRange(min = 0.01, max = 5.0) double eps) {

        IndexData indexData = buildTree(points);
        double[] chosen = points.get(points.size() % points.size());
        double[] mbr = computeMbr(points);

        // INTERSECTS: tight box around chosen point
        assertThat(provider.evaluate(indexData, new SpatialQuery("geo",
                chosen[0] - eps, chosen[1] - eps,
                chosen[0] + eps, chosen[1] + eps,
                SpatialOp.INTERSECTS)))
                .isEqualTo(IndexEvalResult.KEEP);

        // WITHIN: box enclosing entire MBR
        assertThat(provider.evaluate(indexData, new SpatialQuery("geo",
                mbr[0] - eps, mbr[1] - eps,
                mbr[2] + eps, mbr[3] + eps,
                SpatialOp.WITHIN)))
                .isEqualTo(IndexEvalResult.KEEP);

        // CONTAINS: point-box at chosen point
        assertThat(provider.evaluate(indexData, new SpatialQuery("geo",
                chosen[0], chosen[1], chosen[0], chosen[1],
                SpatialOp.CONTAINS)))
                .isEqualTo(IndexEvalResult.KEEP);
    }

    // ── Supplementary traversal properties ───────────────────────────────

    @Property(tries = 10)
    void queryContainingAllPointsReturnsKeep(
            @ForAll @IntRange(min = 1, max = 50) int pointCount) {

        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < pointCount; i++) {
            rows.add(List.of(i * 2.0, i * 3.0));
        }
        IndexData indexData = provider.build(rows.iterator(), Map.of());

        SpatialQuery query = new SpatialQuery("geo", -1, -1, 200, 200, SpatialOp.INTERSECTS);
        assertThat(provider.evaluate(indexData, query)).isEqualTo(IndexEvalResult.KEEP);
    }

    @Property(tries = 10)
    void queryFarAwayReturnsSkip(
            @ForAll @IntRange(min = 1, max = 50) int pointCount) {

        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < pointCount; i++) {
            rows.add(List.of((double) i, (double) i));
        }
        IndexData indexData = provider.build(rows.iterator(), Map.of());

        SpatialQuery query = new SpatialQuery("geo", 9000, 9000, 9999, 9999, SpatialOp.INTERSECTS);
        assertThat(provider.evaluate(indexData, query)).isEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 10)
    void singlePointIntersectsItsOwnBbox(
            @ForAll @DoubleRange(min = -500, max = 500) double x,
            @ForAll @DoubleRange(min = -500, max = 500) double y) {

        List<List<Comparable<?>>> rows = List.of(List.of(x, y));
        IndexData indexData = provider.build(rows.iterator(), Map.of());

        SpatialQuery query = new SpatialQuery("geo", x - 1, y - 1, x + 1, y + 1, SpatialOp.INTERSECTS);
        assertThat(provider.evaluate(indexData, query)).isEqualTo(IndexEvalResult.KEEP);
    }

    @Property(tries = 10)
    void emptyTreeAlwaysSkips(
            @ForAll @DoubleRange(min = -100, max = 100) double x) {

        IndexData indexData = provider.build(List.<List<Comparable<?>>>of().iterator(), Map.of());
        SpatialQuery query = new SpatialQuery("geo", x, x, x + 1, x + 1, SpatialOp.INTERSECTS);
        assertThat(provider.evaluate(indexData, query)).isEqualTo(IndexEvalResult.SKIP);
    }

    // ── Generators & helpers ─────────────────────────────────────────────

    @Provide
    Arbitrary<List<double[]>> pointSets() {
        Arbitrary<double[]> point = Combinators.combine(
                Arbitraries.doubles().between(-1000.0, 1000.0),
                Arbitraries.doubles().between(-1000.0, 1000.0)
        ).as((x, y) -> new double[]{x, y});

        return point.list().ofMinSize(1).ofMaxSize(100);
    }

    private IndexData buildTree(List<double[]> points) {
        List<List<Comparable<?>>> rows = new ArrayList<>(points.size());
        for (double[] p : points) {
            rows.add(List.of(p[0], p[1]));
        }
        return provider.build(rows.iterator(), Map.of());
    }

    private double[] computeMbr(List<double[]> points) {
        double minX = Double.MAX_VALUE, minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
        for (double[] p : points) {
            minX = Math.min(minX, p[0]);
            minY = Math.min(minY, p[1]);
            maxX = Math.max(maxX, p[0]);
            maxY = Math.max(maxY, p[1]);
        }
        return new double[]{minX, minY, maxX, maxY};
    }
}
