package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * k-means clustering with k-means++ style seeding, used to train IVF centroids
 * and PQ sub-quantizer codebooks.
 *
 * <p>Extracted from {@link IvfpqEngine} to keep that class under the 500-line quality gate.
 * Clustering is independent of the IVFPQ index layout: it only needs the vectors, the target
 * cluster count and a distance metric, so it forms a self-contained numeric helper.</p>
 */
final class KMeansClustering {

    private static final int MAX_ITERATIONS = 20;
    private static final double DRIFT_THRESHOLD = 1e-6;

    private KMeansClustering() {
    }

    static float[][] kMeans(float[][] vectors, int k, int dimension, DistanceMetric metric) {
        int n = vectors.length;
        k = Math.min(k, n);
        float[][] centroids = initCentroids(vectors, k, dimension);

        int[] assignments = new int[n];
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            // Assign
            boolean changed = false;
            for (int i = 0; i < n; i++) {
                int nearest = nearestCentroidIdx(vectors[i], centroids, metric);
                if (nearest != assignments[i]) {
                    assignments[i] = nearest;
                    changed = true;
                }
            }
            if (!changed) break;
            // Snapshot old centroids for drift check
            float[][] oldCentroids = new float[k][];
            for (int c = 0; c < k; c++) {
                oldCentroids[c] = Arrays.copyOf(centroids[c], dimension);
            }
            // Update centroids
            updateCentroids(vectors, assignments, centroids, k, dimension);
            // Check centroid drift convergence
            if (maxDrift(centroids, oldCentroids, k, dimension) < DRIFT_THRESHOLD) break;
        }
        return centroids;
    }

    private static double maxDrift(float[][] centroids, float[][] oldCentroids, int k, int dimension) {
        double maxDrift = 0.0;
        for (int c = 0; c < k; c++) {
            double drift = 0.0;
            for (int d = 0; d < dimension; d++) {
                double diff = centroids[c][d] - oldCentroids[c][d];
                drift += diff * diff;
            }
            maxDrift = Math.max(maxDrift, drift);
        }
        return maxDrift;
    }

    private static float[][] initCentroids(float[][] vectors, int k, int dimension) {
        float[][] centroids = new float[k][dimension];
        // k-means++ style: first centroid random, rest proportional to distance
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int first = rng.nextInt(vectors.length);
        System.arraycopy(vectors[first], 0, centroids[0], 0, dimension);

        double[] minDist = new double[vectors.length];
        Arrays.fill(minDist, Double.MAX_VALUE);
        for (int c = 1; c < k; c++) {
            updateMinDistances(vectors, centroids[c - 1], minDist);
            int next = weightedRandomSelect(minDist, rng);
            System.arraycopy(vectors[next], 0, centroids[c], 0, dimension);
        }
        return centroids;
    }

    private static void updateMinDistances(float[][] vectors, float[] centroid, double[] minDist) {
        for (int i = 0; i < vectors.length; i++) {
            double d = VectorDistanceCalculator.l2Distance(vectors[i], centroid);
            minDist[i] = Math.min(minDist[i], d * d);
        }
    }

    private static int weightedRandomSelect(double[] weights, ThreadLocalRandom rng) {
        double total = 0;
        for (double w : weights) total += w;
        if (total <= 0) return rng.nextInt(weights.length);
        double r = rng.nextDouble() * total;
        double cumulative = 0;
        for (int i = 0; i < weights.length; i++) {
            cumulative += weights[i];
            if (cumulative >= r) return i;
        }
        return weights.length - 1;
    }

    static int nearestCentroidIdx(float[] vector, float[][] centroids, DistanceMetric metric) {
        int best = 0;
        double bestDist = Double.MAX_VALUE;
        for (int j = 0; j < centroids.length; j++) {
            double d = distance(vector, centroids[j], metric);
            if (d < bestDist) {
                bestDist = d;
                best = j;
            }
        }
        return best;
    }

    private static void updateCentroids(float[][] vectors, int[] assignments, float[][] centroids,
                                        int k, int dimension) {
        int[] counts = new int[k];
        for (float[] c : centroids) Arrays.fill(c, 0f);
        for (int i = 0; i < vectors.length; i++) {
            int c = assignments[i];
            counts[c]++;
            for (int d = 0; d < dimension; d++) {
                centroids[c][d] += vectors[i][d];
            }
        }
        for (int c = 0; c < k; c++) {
            if (counts[c] > 0) {
                for (int d = 0; d < dimension; d++) {
                    centroids[c][d] /= counts[c];
                }
            }
        }
    }

    static double distance(float[] a, float[] b, DistanceMetric metric) {
        return switch (metric) {
            case L2 -> VectorDistanceCalculator.l2Distance(a, b);
            case COSINE -> VectorDistanceCalculator.cosineDistance(a, b);
            case INNER_PRODUCT -> VectorDistanceCalculator.innerProduct(a, b);
        };
    }
}
