package org.pangrova.indexing.property.eval;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.indexing.engine.rtree.provider.RTreeStrategyProvider;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.data.IndexData;
import org.pangrova.spi.indexing.data.spatial.BoundingBox;
import org.pangrova.spi.indexing.data.spatial.RTreeIndexData;
import org.pangrova.spi.indexing.data.spatial.RTreeNode;
import org.pangrova.spi.connector.index.predicate.SpatialOp;
import org.pangrova.spi.connector.index.predicate.SpatialQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for RTree iterative/recursive traversal equivalence.
 *
 * <p><b>Property: RTree Iterative/Recursive Equivalence</b></p>
 *
 * <p>For any random R-Tree built from 2D points and any random query bounding box,
 * the iterative stack-based traversal ({@link RTreeStrategyProvider#evaluate}) produces
 * a match (KEEP) if and only if a brute-force scan of all leaf nodes finds at least one
 * leaf whose MBR matches the query. This guarantees zero false negatives and zero false
 * positives relative to the leaf-level ground truth.</p>
 *
 * <p><b>Validates: Requirements 1.AC6</b></p>
 */
class RTreeTraversalEquivalencePropertyTest {

    private final RTreeStrategyProvider provider = new RTreeStrategyProvider();

    /**
     * Core equivalence property for INTERSECTS: iterative traversal result matches
     * brute-force leaf scan result for random trees and random query boxes.
     */
    @Property(tries = 10)
    void iterativeMatchesBruteForceForIntersects(
            @ForAll("pointSets") List<double[]> points,
            @ForAll("queryBoxes") double[] queryBox) {

        IndexData indexData = buildTree(points);
        SpatialQuery query = toSpatialQuery(queryBox, SpatialOp.INTERSECTS);

        IndexEvalResult iterativeResult = provider.evaluate(indexData, query);
        boolean bruteForceMatch = bruteForceLeafScan(
                (RTreeIndexData) indexData, toBoundingBox(queryBox), SpatialOp.INTERSECTS);

        assertThat(iterativeResult == IndexEvalResult.KEEP)
                .as("INTERSECTS equivalence: iterative=%s, bruteForce=%s, points=%d, query=%s",
                        iterativeResult, bruteForceMatch, points.size(), formatBox(queryBox))
                .isEqualTo(bruteForceMatch);
    }

    /**
     * Core equivalence property for WITHIN: iterative traversal result matches
     * brute-force leaf scan result for random trees and random query boxes.
     */
    @Property(tries = 10)
    void iterativeMatchesBruteForceForWithin(
            @ForAll("pointSets") List<double[]> points,
            @ForAll("queryBoxes") double[] queryBox) {

        IndexData indexData = buildTree(points);
        SpatialQuery query = toSpatialQuery(queryBox, SpatialOp.WITHIN);

        IndexEvalResult iterativeResult = provider.evaluate(indexData, query);
        boolean bruteForceMatch = bruteForceLeafScan(
                (RTreeIndexData) indexData, toBoundingBox(queryBox), SpatialOp.WITHIN);

        assertThat(iterativeResult == IndexEvalResult.KEEP)
                .as("WITHIN equivalence: iterative=%s, bruteForce=%s, points=%d, query=%s",
                        iterativeResult, bruteForceMatch, points.size(), formatBox(queryBox))
                .isEqualTo(bruteForceMatch);
    }

    /**
     * Core equivalence property for CONTAINS: iterative traversal result matches
     * brute-force leaf scan result for random trees and random query boxes.
     */
    @Property(tries = 10)
    void iterativeMatchesBruteForceForContains(
            @ForAll("pointSets") List<double[]> points,
            @ForAll("queryBoxes") double[] queryBox) {

        IndexData indexData = buildTree(points);
        SpatialQuery query = toSpatialQuery(queryBox, SpatialOp.CONTAINS);

        IndexEvalResult iterativeResult = provider.evaluate(indexData, query);
        boolean bruteForceMatch = bruteForceLeafScan(
                (RTreeIndexData) indexData, toBoundingBox(queryBox), SpatialOp.CONTAINS);

        assertThat(iterativeResult == IndexEvalResult.KEEP)
                .as("CONTAINS equivalence: iterative=%s, bruteForce=%s, points=%d, query=%s",
                        iterativeResult, bruteForceMatch, points.size(), formatBox(queryBox))
                .isEqualTo(bruteForceMatch);
    }

    /**
     * All three spatial operations tested together for the same random tree and query.
     * Ensures no operation diverges from its brute-force oracle.
     */
    @Property(tries = 10)
    void allOpsMatchBruteForceForSameTreeAndQuery(
            @ForAll("pointSets") List<double[]> points,
            @ForAll("queryBoxes") double[] queryBox) {

        IndexData indexData = buildTree(points);
        RTreeIndexData rtreeData = (RTreeIndexData) indexData;
        BoundingBox qBox = toBoundingBox(queryBox);

        for (SpatialOp op : SpatialOp.values()) {
            IndexEvalResult iterativeResult = provider.evaluate(
                    indexData, toSpatialQuery(queryBox, op));
            boolean bruteForceMatch = bruteForceLeafScan(rtreeData, qBox, op);

            assertThat(iterativeResult == IndexEvalResult.KEEP)
                    .as("%s equivalence: iterative=%s, bruteForce=%s, points=%d",
                            op, iterativeResult, bruteForceMatch, points.size())
                    .isEqualTo(bruteForceMatch);
        }
    }

    /**
     * Empty tree: iterative traversal returns SKIP for any query box and any operation.
     * Brute-force on zero leaves also returns false. Both must agree.
     */
    @Property(tries = 10)
    void emptyTreeEquivalence(
            @ForAll("queryBoxes") double[] queryBox,
            @ForAll("spatialOps") SpatialOp op) {

        IndexData indexData = provider.build(
                List.<List<Comparable<?>>>of().iterator(), Map.of());

        IndexEvalResult iterativeResult = provider.evaluate(
                indexData, toSpatialQuery(queryBox, op));

        assertThat(iterativeResult)
                .as("Empty tree must return SKIP for %s", op)
                .isEqualTo(IndexEvalResult.SKIP);
    }

    // ── Brute-force oracle ───────────────────────────────────────────────

    /**
     * Ground-truth oracle: linearly scans all leaf nodes and checks if any leaf's
     * MBR matches the query box under the given spatial operation. This is the
     * reference implementation against which the iterative traversal is validated.
     */
    private static boolean bruteForceLeafScan(
            RTreeIndexData rtree, BoundingBox queryBox, SpatialOp op) {
        for (RTreeNode node : rtree.nodes()) {
            if (!node.leaf()) {
                continue;
            }
            if (matchesMbr(node.boundingBox(), queryBox, op)) {
                return true;
            }
        }
        return false;
    }

    /**
     * MBR match logic — mirrors {@code RTreeStrategyProvider.matchesMbr()} exactly.
     * Duplicated here intentionally as the oracle must be independently verifiable.
     */
    private static boolean matchesMbr(BoundingBox nodeBbox, BoundingBox queryBox, SpatialOp op) {
        return switch (op) {
            case INTERSECTS -> nodeBbox.mbrIntersects(queryBox);
            case WITHIN -> queryBox.mbrContains(nodeBbox);
            case CONTAINS -> nodeBbox.mbrContains(queryBox);
        };
    }

    // ── Generators ───────────────────────────────────────────────────────

    @Provide
    Arbitrary<List<double[]>> pointSets() {
        Arbitrary<double[]> point = Combinators.combine(
                Arbitraries.doubles().between(-500.0, 500.0),
                Arbitraries.doubles().between(-500.0, 500.0)
        ).as((x, y) -> new double[]{x, y});

        return point.list().ofMinSize(1).ofMaxSize(200);
    }

    @Provide
    Arbitrary<double[]> queryBoxes() {
        return Combinators.combine(
                Arbitraries.doubles().between(-600.0, 600.0),
                Arbitraries.doubles().between(-600.0, 600.0),
                Arbitraries.doubles().between(0.01, 200.0),
                Arbitraries.doubles().between(0.01, 200.0)
        ).as((cx, cy, hw, hh) -> new double[]{cx - hw, cy - hh, cx + hw, cy + hh});
    }

    @Provide
    Arbitrary<SpatialOp> spatialOps() {
        return Arbitraries.of(SpatialOp.values());
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private IndexData buildTree(List<double[]> points) {
        List<List<Comparable<?>>> rows = new ArrayList<>(points.size());
        for (double[] p : points) {
            rows.add(List.of(p[0], p[1]));
        }
        return provider.build(rows.iterator(), Map.of());
    }

    private static SpatialQuery toSpatialQuery(double[] box, SpatialOp op) {
        return new SpatialQuery("geo", box[0], box[1], box[2], box[3], op);
    }

    private static BoundingBox toBoundingBox(double[] box) {
        return new BoundingBox(new double[]{box[0], box[1]}, new double[]{box[2], box[3]});
    }

    private static String formatBox(double[] box) {
        return String.format("[%.2f,%.2f → %.2f,%.2f]", box[0], box[1], box[2], box[3]);
    }
}
