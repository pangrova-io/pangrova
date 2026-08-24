package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * IVFPQ (Inverted File with Product Quantization) algorithm engine.
 *
 * <p>Package-private: accessed only through {@link VectorStrategyProvider}.
 * k-means clustering (IVF) partitions vectors into cells; product quantization (PQ)
 * compresses residual vectors for memory-efficient approximate search.</p>
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@code ivfpq.nlist} — number of IVF clusters, range [1, 65536], default 256</li>
 *   <li>{@code ivfpq.nprobe} — clusters to probe at search time, range [1, nlist], default 16</li>
 *   <li>{@code ivfpq.m} — number of PQ sub-quantizers, dimension must be divisible by m, default 8</li>
 *   <li>{@code ivfpq.nbits} — bits per sub-quantizer code, one of {4, 8, 16}, default 8</li>
 * </ul>
 */
final class IvfpqEngine {

    private static final String PROP_NLIST = "ivfpq.nlist";
    private static final String PROP_NPROBE = "ivfpq.nprobe";
    private static final String PROP_M = "ivfpq.m";
    private static final String PROP_NBITS = "ivfpq.nbits";
    private static final int DEFAULT_NLIST = 256;
    private static final int DEFAULT_NPROBE = 16;
    private static final int DEFAULT_M = 8;
    private static final int DEFAULT_NBITS = 8;
    /** Upper bound for the number of IVF clusters. */
    private static final int MAX_NLIST = 65536;
    /** Cumulative-new/training ratio above which an incrementally updated index is marked stale. */
    private static final double STALE_INCREMENTAL_RATIO = 0.3;

    VectorIndexData build(List<float[]> vectors, long[] rowOffsets, int dimension, DistanceMetric metric,
                          Map<String, String> properties) {
        requireL2(metric);
        int nlist = clamp(intProp(properties, PROP_NLIST, DEFAULT_NLIST), 1, MAX_NLIST);
        int pqM = intProp(properties, PROP_M, DEFAULT_M);
        int nbits = intProp(properties, PROP_NBITS, DEFAULT_NBITS);
        validateBuildParams(dimension, pqM, nbits);

        // Adjust nlist if more clusters than vectors
        nlist = Math.min(nlist, vectors.size());
        if (vectors.isEmpty()) {
            return emptyIndex(dimension, metric, properties);
        }

        IvfpqIndex index = buildIndex(vectors, rowOffsets, dimension, metric, nlist, pqM, nbits);
        byte[] payload = IvfpqCodec.serialize(index, dimension, metric, nlist, pqM, nbits);
        return new VectorIndexData(VectorAlgorithmType.IVFPQ, dimension, metric,
                vectors.size(), payload, Map.copyOf(properties));
    }

    /** Incrementally encodes new vectors using the existing codebook (no retraining). */
    VectorIndexData encodeIncremental(VectorIndexData existingData, List<float[]> newVectors,
                                      int dimension, DistanceMetric metric,
                                      Map<String, String> properties) {
        if (newVectors.isEmpty()) {
            return existingData;
        }
        IvfpqIndex existingIndex = IvfpqCodec.deserialize(existingData.indexPayload());
        if (existingIndex.vectorCount() == 0) {
            // No existing codebook: fall back to a full build of the new batch.
            // Physical file offsets are unavailable on this incremental path, so
            // assign sequential offsets (matching the prior dense semantics).
            long[] rowOffsets = new long[newVectors.size()];
            for (int i = 0; i < rowOffsets.length; i++) {
                rowOffsets[i] = i;
            }
            return build(newVectors, rowOffsets, dimension, metric, properties);
        }

        List<List<IvfpqIndex.EncodedVector>> updatedLists = appendNewVectors(existingIndex, newVectors, metric);
        int newTotalCount = existingIndex.vectorCount() + newVectors.size();
        IvfpqIndex updatedIndex = new IvfpqIndex(
                existingIndex.centroids(), existingIndex.codebook(), updatedLists,
                dimension, existingIndex.pqM(), existingIndex.nbits(), newTotalCount);

        byte[] payload = IvfpqCodec.serialize(updatedIndex, dimension, metric,
                existingIndex.nlist(), existingIndex.pqM(), existingIndex.nbits());
        Map<String, String> updatedProps = buildIncrementalProps(
                existingData, existingIndex, newTotalCount, properties);

        return new VectorIndexData(VectorAlgorithmType.IVFPQ, dimension, metric,
                newTotalCount, payload, Map.copyOf(updatedProps));
    }

    private List<List<IvfpqIndex.EncodedVector>> appendNewVectors(
            IvfpqIndex index, List<float[]> newVectors, DistanceMetric metric) {
        List<List<IvfpqIndex.EncodedVector>> updatedLists = new ArrayList<>(index.nlist());
        for (int c = 0; c < index.nlist(); c++) {
            updatedLists.add(new ArrayList<>(index.invertedList(c)));
        }
        long baseOffset = index.vectorCount();
        for (int i = 0; i < newVectors.size(); i++) {
            float[] vector = newVectors.get(i);
            int clusterId = KMeansClustering.nearestCentroidIdx(vector, index.centroids(), metric);
            int[] codes = encodeResidual(vector, index.centroids()[clusterId],
                    index.codebook(), new PqShape(index.pqM(), index.subDim(), index.ksub()));
            updatedLists.get(clusterId).add(new IvfpqIndex.EncodedVector(baseOffset + i, codes));
        }
        return updatedLists;
    }

    private Map<String, String> buildIncrementalProps(
            VectorIndexData existingData, IvfpqIndex existingIndex,
            int newTotalCount, Map<String, String> properties) {
        Map<String, String> updatedProps = new java.util.HashMap<>(properties);
        int originalTrainingCount = intProp(existingData.buildParams(),
                "_original_vector_count", existingIndex.vectorCount());
        int cumulativeNew = newTotalCount - originalTrainingCount;
        updatedProps.put("_original_vector_count", String.valueOf(originalTrainingCount));
        if (originalTrainingCount > 0
                && (double) cumulativeNew / originalTrainingCount > STALE_INCREMENTAL_RATIO) {
            updatedProps.put("_stale", "true");
        }
        return updatedProps;
    }

    List<VectorSearchResult.ScoredRow> search(VectorIndexData indexData, VectorSearchRequest request,
                                               Map<String, String> searchParams) {
        int nprobe = clamp(intProp(searchParams, PROP_NPROBE, DEFAULT_NPROBE), 1, MAX_NLIST);
        IvfpqIndex index = IvfpqCodec.deserialize(indexData.indexPayload());
        if (index.vectorCount() == 0) {
            return List.of();
        }
        nprobe = Math.min(nprobe, index.nlist());
        return searchIndex(index, request.queryVector(), request.topK(), nprobe, indexData.metric());
    }

    /** Filtered search: over-fetch by 4x, then keep only rows in the allowed set. */
    List<VectorSearchResult.ScoredRow> search(VectorIndexData indexData, VectorSearchRequest request,
                                               Map<String, String> searchParams,
                                               Set<Long> allowedRows) {
        if (allowedRows == null) {
            return search(indexData, request, searchParams);
        }
        int nprobe = clamp(intProp(searchParams, PROP_NPROBE, DEFAULT_NPROBE), 1, MAX_NLIST);
        IvfpqIndex index = IvfpqCodec.deserialize(indexData.indexPayload());
        if (index.vectorCount() == 0) {
            return List.of();
        }
        nprobe = Math.min(nprobe, index.nlist());
        int overFetchK = Math.min(request.topK() * 4, index.vectorCount());
        List<VectorSearchResult.ScoredRow> candidates =
                searchIndex(index, request.queryVector(), overFetchK, nprobe, indexData.metric());
        List<VectorSearchResult.ScoredRow> filtered = new ArrayList<>();
        for (VectorSearchResult.ScoredRow row : candidates) {
            if (allowedRows.contains(row.rowOffset())) {
                filtered.add(row);
                if (filtered.size() >= request.topK()) {
                    break;
                }
            }
        }
        return filtered;
    }

    // --- Index construction ---

    /** Product-quantization shape: sub-quantizer count, sub-dimension, and codebook size. */
    private record PqShape(int pqM, int subDim, int ksub) {}

    private static IvfpqIndex buildIndex(List<float[]> vectors, long[] rowOffsets, int dimension,
                                         DistanceMetric metric, int nlist, int pqM, int nbits) {
        float[][] vectorArray = vectors.toArray(float[][]::new);

        // Step 1: k-means to find IVF centroids
        float[][] centroids = KMeansClustering.kMeans(vectorArray, nlist, dimension, metric);

        // Step 2: assign vectors to nearest centroid
        int[] assignments = assignToCentroids(vectorArray, centroids, metric);

        // Step 3: train PQ codebook on residuals
        PqShape shape = new PqShape(pqM, dimension / pqM, 1 << nbits);
        float[][][] codebook = trainCodebook(vectorArray, centroids, assignments, shape);

        // Step 4: encode vectors as PQ codes, keyed by physical row offset
        List<List<IvfpqIndex.EncodedVector>> invertedLists = encodeVectors(
                vectorArray, rowOffsets, centroids, assignments, codebook, shape);

        return new IvfpqIndex(centroids, codebook, invertedLists, dimension, pqM, nbits, vectorArray.length);
    }

    private static int[] assignToCentroids(float[][] vectors, float[][] centroids, DistanceMetric metric) {
        int[] assignments = new int[vectors.length];
        for (int i = 0; i < vectors.length; i++) {
            assignments[i] = KMeansClustering.nearestCentroidIdx(vectors[i], centroids, metric);
        }
        return assignments;
    }

    // --- PQ codebook training ---

    private static float[][][] trainCodebook(float[][] vectors, float[][] centroids, int[] assignments,
                                             PqShape shape) {
        float[][][] codebook = new float[shape.pqM()][shape.ksub()][shape.subDim()];
        for (int s = 0; s < shape.pqM(); s++) {
            float[][] subResiduals = extractSubResiduals(vectors, centroids, assignments, s, shape.subDim());
            if (subResiduals.length == 0) {
                continue;
            }
            int actualKsub = Math.min(shape.ksub(), subResiduals.length);
            float[][] subCentroids = KMeansClustering.kMeans(
                    subResiduals, actualKsub, shape.subDim(), DistanceMetric.L2);
            for (int k = 0; k < actualKsub; k++) {
                codebook[s][k] = subCentroids[k];
            }
        }
        return codebook;
    }

    private static float[][] extractSubResiduals(float[][] vectors, float[][] centroids, int[] assignments,
                                                 int subIndex, int subDim) {
        int offset = subIndex * subDim;
        float[][] subResiduals = new float[vectors.length][subDim];
        for (int i = 0; i < vectors.length; i++) {
            float[] centroid = centroids[assignments[i]];
            for (int d = 0; d < subDim; d++) {
                subResiduals[i][d] = vectors[i][offset + d] - centroid[offset + d];
            }
        }
        return subResiduals;
    }

    // --- PQ encoding ---

    private static List<List<IvfpqIndex.EncodedVector>> encodeVectors(
            float[][] vectors, long[] rowOffsets, float[][] centroids, int[] assignments,
            float[][][] codebook, PqShape shape) {
        int nlist = centroids.length;
        List<List<IvfpqIndex.EncodedVector>> invertedLists = new ArrayList<>(nlist);
        for (int c = 0; c < nlist; c++) {
            invertedLists.add(new ArrayList<>());
        }
        for (int i = 0; i < vectors.length; i++) {
            int clusterId = assignments[i];
            int[] codes = encodeResidual(vectors[i], centroids[clusterId], codebook, shape);
            invertedLists.get(clusterId).add(new IvfpqIndex.EncodedVector(rowOffsets[i], codes));
        }
        return invertedLists;
    }

    private static int[] encodeResidual(float[] vector, float[] centroid, float[][][] codebook,
                                        PqShape shape) {
        int[] codes = new int[shape.pqM()];
        for (int s = 0; s < shape.pqM(); s++) {
            int offset = s * shape.subDim();
            float[] subResidual = new float[shape.subDim()];
            for (int d = 0; d < shape.subDim(); d++) {
                subResidual[d] = vector[offset + d] - centroid[offset + d];
            }
            codes[s] = nearestCodeword(subResidual, codebook[s],
                    Math.min(shape.ksub(), codebook[s].length));
        }
        return codes;
    }

    private static int nearestCodeword(float[] subVec, float[][] codewords, int count) {
        int best = 0;
        double bestDist = Double.MAX_VALUE;
        for (int k = 0; k < count; k++) {
            double d = VectorDistanceCalculator.l2Distance(subVec, codewords[k]);
            if (d < bestDist) {
                bestDist = d;
                best = k;
            }
        }
        return best;
    }

    // --- Search with ADC (Asymmetric Distance Computation) ---

    private static List<VectorSearchResult.ScoredRow> searchIndex(
            IvfpqIndex index, float[] query, int topK, int nprobe, DistanceMetric metric) {
        int[] probeList = findNearestCentroids(query, index.centroids(), nprobe, metric);

        // Max-heap to keep top-K closest
        PriorityQueue<VectorSearchResult.ScoredRow> heap = new PriorityQueue<>(
                Comparator.comparingDouble(VectorSearchResult.ScoredRow::distance).reversed());

        // ADC: for each probed centroid, precompute distance tables using the
        // query's residual (query - centroid), not the raw query. PQ codes encode
        // residual vectors, so distances must be computed in residual space.
        for (int probeIdx : probeList) {
            double[][] distTables = precomputeResidualDistanceTables(
                    query, index.centroids()[probeIdx], index.codebook(),
                    new PqShape(index.pqM(), index.subDim(), index.ksub()));
            scanInvertedList(index, probeIdx, distTables, topK, heap);
        }

        List<VectorSearchResult.ScoredRow> results = new ArrayList<>(heap);
        results.sort(Comparator.comparingDouble(VectorSearchResult.ScoredRow::distance));
        return results;
    }

    private static void scanInvertedList(IvfpqIndex index, int clusterId, double[][] distTables,
                                         int topK, PriorityQueue<VectorSearchResult.ScoredRow> heap) {
        List<IvfpqIndex.EncodedVector> list = index.invertedList(clusterId);
        int pqM = index.pqM();
        for (IvfpqIndex.EncodedVector ev : list) {
            // distTables holds SQUARED per-subspace distances; their sum is the
            // total squared L2 in residual space. Take the root so the reported
            // distance is a true (approximate) L2 distance, consistent with the
            // HNSW and brute-force engines.
            double adcDistSq = 0.0;
            for (int s = 0; s < pqM; s++) {
                adcDistSq += distTables[s][ev.codes()[s]];
            }
            VectorSearchResult.ScoredRow row =
                    new VectorSearchResult.ScoredRow(ev.rowOffset(), Math.sqrt(adcDistSq));
            heap.offer(row);
            if (heap.size() > topK) {
                heap.poll();
            }
        }
    }

    /** Precomputes ADC distance tables using the query's residual (query - centroid). */
    private static double[][] precomputeResidualDistanceTables(
            float[] query, float[] centroid, float[][][] codebook, PqShape shape) {
        double[][] tables = new double[shape.pqM()][shape.ksub()];
        for (int s = 0; s < shape.pqM(); s++) {
            int offset = s * shape.subDim();
            float[] residualSub = new float[shape.subDim()];
            for (int d = 0; d < shape.subDim(); d++) {
                residualSub[d] = query[offset + d] - centroid[offset + d];
            }
            for (int k = 0; k < shape.ksub(); k++) {
                // Squared distance: ADC sums per-subspace contributions, and the
                // total squared L2 equals the sum of squared sub-distances.
                tables[s][k] = VectorDistanceCalculator.l2DistanceSquared(residualSub, codebook[s][k]);
            }
        }
        return tables;
    }

    private static int[] findNearestCentroids(float[] query, float[][] centroids,
                                               int nprobe, DistanceMetric metric) {
        int n = centroids.length;
        // Min-heap: O(n + nprobe * log n) vs selection sort O(n * nprobe)
        record CentroidDist(int index, double distance) {}

        java.util.PriorityQueue<CentroidDist> heap = new java.util.PriorityQueue<>(
                n, Comparator.comparingDouble(CentroidDist::distance));
        for (int i = 0; i < n; i++) {
            heap.add(new CentroidDist(i, KMeansClustering.distance(query, centroids[i], metric)));
        }
        int count = Math.min(nprobe, n);
        int[] result = new int[count];
        for (int i = 0; i < count; i++) {
            result[i] = heap.poll().index;
        }
        return result;
    }

    // --- Utility ---

    private static void validateBuildParams(int dimension, int pqM, int nbits) {
        if (dimension % pqM != 0) {
            throw new IllegalArgumentException(
                    "dimension (" + dimension + ") must be divisible by ivfpq.m (" + pqM + ")");
        }
        if (nbits != 4 && nbits != 8 && nbits != 16) {
            throw new IllegalArgumentException("ivfpq.nbits must be 4, 8, or 16, got " + nbits);
        }
    }

    /**
     * IVFPQ encodes residuals of {@code vector - centroid} and computes ADC in L2
     * space. That is mathematically valid only for L2: cosine/inner-product are
     * not translation-invariant, so residual PQ would silently return wrong
     * rankings. Fail fast and steer non-L2 similarity search to HNSW, which
     * handles all metrics correctly.
     */
    private static void requireL2(DistanceMetric metric) {
        if (metric != DistanceMetric.L2) {
            throw new IllegalArgumentException(
                    "IVFPQ supports only the L2 metric (residual product quantization is "
                            + "L2-specific); use algorithm=HNSW for " + metric + " search");
        }
    }

    private static VectorIndexData emptyIndex(int dimension, DistanceMetric metric,
                                              Map<String, String> properties) {
        return new VectorIndexData(VectorAlgorithmType.IVFPQ, dimension, metric,
                0, new byte[0], Map.copyOf(properties));
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
