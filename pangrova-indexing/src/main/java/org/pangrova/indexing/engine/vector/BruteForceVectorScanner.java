package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult.ScoredRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Brute-force (linear scan) vector search for data files without an ANN index.
 *
 * <p>Per F-CHAIN-INDEX-03, when vector index coverage is incomplete, unindexed
 * files must not be silently excluded from ANN results. This scanner performs
 * an exhaustive distance computation against every vector in the file, keeping
 * only the top-K nearest neighbors via a bounded max-heap.</p>
 *
 * <p>Distance computation delegates to {@link VectorDistanceCalculator} which
 * supports L2, cosine, and inner-product metrics. All methods are stateless
 * and thread-safe.</p>
 */
public final class BruteForceVectorScanner {

    private static final Logger LOG = LoggerFactory.getLogger(BruteForceVectorScanner.class);

    private BruteForceVectorScanner() {
        // utility class
    }

    /**
     * Scans all vectors from the given iterator and returns the top-K nearest
     * neighbors to the query vector.
     *
     * <p>The iterator yields rows where the first element is the vector value
     * (either {@code float[]} or {@code List<Number>}). Null/empty rows are
     * skipped (they represent rows with NULL vector columns). The returned
     * {@link ScoredRow} entries have {@code fileId} set to the provided
     * {@code dataFilePath} and {@code rowOffset} set to the 0-based file row
     * position (consistent with F-CHAIN-INDEX-05).</p>
     *
     * @param rows         iterator over data file rows (first column is the vector)
     * @param dataFilePath source data file path for provenance stamping
     * @param request      search request containing query vector, topK, and metric
     * @return list of scored rows sorted by distance ascending, size <= topK
     */
    public static List<ScoredRow> scan(Iterator<List<Comparable<?>>> rows,
                                       String dataFilePath,
                                       VectorSearchRequest request) {
        float[] queryVector = request.queryVector();
        int topK = request.topK();
        DistanceMetric metric = request.metric();

        // Max-heap: evict the FARTHEST candidate when we exceed topK
        PriorityQueue<ScoredRow> heap = new PriorityQueue<>(
                Math.max(topK, 1),
                Comparator.comparingDouble(ScoredRow::distance).reversed());

        long fileRow = 0;
        while (rows.hasNext()) {
            List<Comparable<?>> row = rows.next();
            if (row.isEmpty() || row.get(0) == null) {
                fileRow++;
                continue;
            }
            float[] candidate = toFloatArray(row.get(0));
            if (candidate.length != queryVector.length) {
                LOG.warn("event=brute_force_dimension_mismatch file={} row={} expected={} actual={}",
                        dataFilePath, fileRow, queryVector.length, candidate.length);
                fileRow++;
                continue;
            }
            double distance = computeDistance(queryVector, candidate, metric);
            ScoredRow scored = new ScoredRow(fileRow, distance);
            heap.offer(scored);
            if (heap.size() > topK) {
                heap.poll(); // evict farthest
            }
            fileRow++;
        }

        List<ScoredRow> result = new ArrayList<>(heap);
        result.sort(Comparator.comparingDouble(ScoredRow::distance));
        return result;
    }

    /**
     * Computes the distance between two vectors using the specified metric.
     * Package-private for testing.
     */
    static double computeDistance(float[] a, float[] b, DistanceMetric metric) {
        return switch (metric) {
            case L2 -> VectorDistanceCalculator.l2Distance(a, b);
            case COSINE -> VectorDistanceCalculator.cosineDistance(a, b);
            case INNER_PRODUCT -> VectorDistanceCalculator.innerProduct(a, b);
        };
    }

    @SuppressWarnings("unchecked")
    private static float[] toFloatArray(Object value) {
        if (value instanceof float[] fa) {
            return fa;
        }
        if (value instanceof List<?> list) {
            float[] result = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                result[i] = ((Number) list.get(i)).floatValue();
            }
            return result;
        }
        throw new IllegalArgumentException(
                "Cannot convert " + value.getClass().getName() + " to float[]");
    }
}
