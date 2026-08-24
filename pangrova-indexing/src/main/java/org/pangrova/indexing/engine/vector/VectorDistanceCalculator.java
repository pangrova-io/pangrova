package org.pangrova.indexing.engine.vector;

/**
 * Pure Java distance calculations for vector similarity search.
 *
 * <p>All methods are stateless and thread-safe. Each method validates that
 * input arrays have equal length, throwing {@link IllegalArgumentException}
 * on mismatch. No NaN/Infinity handling is performed here — that validation
 * belongs to the index build phase.
 *
 * <p>Distance conventions (lower = more similar):
 * <ul>
 *   <li>{@link #l2Distance}: Euclidean distance, range [0, +∞)</li>
 *   <li>{@link #cosineDistance}: 1 − cosine_similarity, range [0, 2] for non-zero vectors</li>
 *   <li>{@link #innerProduct}: −dot(a, b)</li>
 * </ul>
 */
public final class VectorDistanceCalculator {

    /** Maximum quantized level for SQ8 (8-bit scalar quantization). */
    private static final int SQ8_MAX_LEVEL = 255;

    private VectorDistanceCalculator() {
        // utility class
    }

    /**
     * Euclidean (L2) distance: {@code sqrt(sum((a[i] - b[i])^2))}.
     *
     * <p>Properties: non-negativity (≥ 0), identity (l2Distance(a, a) == 0),
     * symmetry (l2Distance(a, b) == l2Distance(b, a)).
     *
     * @throws IllegalArgumentException if {@code a.length != b.length}
     */
    public static double l2Distance(float[] a, float[] b) {
        return Math.sqrt(l2DistanceSquared(a, b));
    }

    /**
     * Squared Euclidean (L2) distance: {@code sum((a[i] - b[i])^2)}.
     *
     * <p>Avoids the {@link Math#sqrt} of {@link #l2Distance} where only ordering
     * or additive composition across subspaces is required. Product-quantization
     * ADC must accumulate squared sub-distances (the total squared L2 is the sum
     * of per-subspace squared distances); summing non-squared distances would
     * change the ranking and degrade recall.
     *
     * @throws IllegalArgumentException if {@code a.length != b.length}
     */
    public static double l2DistanceSquared(float[] a, float[] b) {
        requireSameDimension(a, b);
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = (double) a[i] - (double) b[i];
            sum += diff * diff;
        }
        return sum;
    }

    /**
     * Cosine distance: {@code 1 - dot(a, b) / (||a|| * ||b||)}.
     *
     * <p>Returns a value in [0, 2] for non-zero vectors. When either vector
     * is a zero vector (all elements zero), returns 1.0 — defined as maximum
     * dissimilarity, since direction is undefined for zero vectors.
     *
     * @throws IllegalArgumentException if {@code a.length != b.length}
     */
    public static double cosineDistance(float[] a, float[] b) {
        requireSameDimension(a, b);
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            double ai = a[i];
            double bi = b[i];
            dot += ai * bi;
            normA += ai * ai;
            normB += bi * bi;
        }
        if (normA == 0.0 || normB == 0.0) {
            return 1.0;
        }
        double similarity = dot / (Math.sqrt(normA) * Math.sqrt(normB));
        // Clamp to [-1, 1] to guard against floating-point drift
        similarity = Math.max(-1.0, Math.min(1.0, similarity));
        return 1.0 - similarity;
    }

    /**
     * Negative inner (dot) product: {@code -dot(a, b)}.
     *
     * <p>Lower values indicate more similar vectors. Assumes vectors are
     * normalized; for unnormalized vectors prefer {@link #l2Distance} or
     * {@link #cosineDistance}.
     *
     * @throws IllegalArgumentException if {@code a.length != b.length}
     */
    public static double innerProduct(float[] a, float[] b) {
        requireSameDimension(a, b);
        double dot = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * (double) b[i];
        }
        return -dot;
    }

    private static void requireSameDimension(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                    "Dimension mismatch: a.length=" + a.length + ", b.length=" + b.length);
        }
    }

    /**
     * Quantized L2 distance using SQ8 (8-bit scalar quantization).
     *
     * <p>Converts float vectors to byte representations on-the-fly using the
     * provided min/max range per dimension, then computes L2 distance in
     * quantized space. Reduces memory bandwidth by 4× compared to float32.</p>
     *
     * <p>The returned distance is in quantized integer space and is NOT directly
     * comparable to float32 L2 distances. Use only for ranking (ordering is preserved).</p>
     *
     * @param a         first vector
     * @param b         second vector
     * @param dimMin    per-dimension minimum values (length must equal vector dimension)
     * @param dimScale  per-dimension scale factors: {@code 255.0 / (max - min)} (length must equal vector dimension)
     * @return approximate L2 distance in quantized space
     * @throws IllegalArgumentException if array dimensions do not match
     */
    public static double l2DistanceSQ8(float[] a, float[] b,
                                       float[] dimMin, float[] dimScale) {
        requireSameDimension(a, b);
        requireQuantizationParams(a, dimMin, dimScale);
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            int qa = quantizeSq8(a[i], dimMin[i], dimScale[i]);
            int qb = quantizeSq8(b[i], dimMin[i], dimScale[i]);
            int diff = qa - qb;
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Quantized inner product using SQ8 (8-bit scalar quantization).
     *
     * <p>The returned value is in quantized integer space and is NOT directly
     * comparable to float32 inner product values. Use only for ranking.</p>
     *
     * @param a         first vector
     * @param b         second vector
     * @param dimMin    per-dimension minimum values (length must equal vector dimension)
     * @param dimScale  per-dimension scale factors: {@code 255.0 / (max - min)} (length must equal vector dimension)
     * @return approximate negative inner product in quantized space
     * @throws IllegalArgumentException if array dimensions do not match
     */
    public static double innerProductSQ8(float[] a, float[] b,
                                         float[] dimMin, float[] dimScale) {
        requireSameDimension(a, b);
        requireQuantizationParams(a, dimMin, dimScale);
        double dot = 0.0;
        for (int i = 0; i < a.length; i++) {
            int qa = quantizeSq8(a[i], dimMin[i], dimScale[i]);
            int qb = quantizeSq8(b[i], dimMin[i], dimScale[i]);
            dot += qa * qb;
        }
        return -dot;
    }

    private static int quantizeSq8(float value, float dimMin, float dimScale) {
        return Math.min(SQ8_MAX_LEVEL, Math.max(0, Math.round((value - dimMin) * dimScale)));
    }

    private static void requireQuantizationParams(float[] vector, float[] dimMin, float[] dimScale) {
        if (dimMin.length != vector.length) {
            throw new IllegalArgumentException(
                    "dimMin length mismatch: expected " + vector.length + ", got " + dimMin.length);
        }
        if (dimScale.length != vector.length) {
            throw new IllegalArgumentException(
                    "dimScale length mismatch: expected " + vector.length + ", got " + dimScale.length);
        }
    }
}
