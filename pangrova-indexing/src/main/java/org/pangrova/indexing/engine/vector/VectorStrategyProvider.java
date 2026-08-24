package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.IndexStrategyProvider;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.vector.DimensionMismatchException;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.MetricMismatchException;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Vector index strategy supporting HNSW and IVFPQ approximate nearest neighbor algorithms.
 *
 * <p>Registered via {@link java.util.ServiceLoader} as the sole provider for
 * {@link IndexType#VECTOR}. Algorithm dispatch is internal: {@code build()} and
 * {@code vectorSearch()} delegate to package-private {@link HnswEngine} or
 * {@link IvfpqEngine} based on the {@code algorithm} property.</p>
 *
 * <p>Traditional {@link #evaluate} always returns {@link IndexEvalResult#KEEP}
 * because vector indexes do not participate in file-level skip/keep filtering.</p>
 */
public final class VectorStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_ALGORITHM = "algorithm";
    private static final String PROP_DIMENSION = "dimension";
    private static final String PROP_METRIC = "metric";
    private static final String PROP_MAX_MEMORY = "max_memory_bytes";
    private static final String PROP_SQ_BITS = "sq_bits";
    private static final String PROP_BQ_ENABLED = "bq_enabled";
    private static final String PROP_FP16 = "fp16";
    private static final long DEFAULT_MAX_MEMORY = 256L * 1024 * 1024; // 256 MB

    /** Maximum magnitude representable in IEEE 754 binary16. */
    private static final float FP16_MAX_MAGNITUDE = 65504.0f;
    /** Mask that clears the low 13 mantissa bits, keeping the top 10 (fp16 precision). */
    private static final int FP16_MANTISSA_CLEAR_MASK = 0xFFFFE000;

    private final HnswEngine hnswEngine = new HnswEngine();
    private final IvfpqEngine ivfpqEngine = new IvfpqEngine();
    private final DiskAnnVectorOperations diskAnnOperations = new DiskAnnVectorOperations();

    @Override
    public IndexType supportedType() {
        return IndexType.VECTOR;
    }

    @Override
    public boolean supportsVectorSearch() {
        return true;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        VectorAlgorithmType algorithm = parseAlgorithm(properties);
        int dimension = intProp(properties, PROP_DIMENSION, -1);
        DistanceMetric metric = parseMetric(properties);
        long maxMemory = longProp(properties, PROP_MAX_MEMORY, DEFAULT_MAX_MEMORY);
        int sqBits = intProp(properties, PROP_SQ_BITS, 0);
        boolean bqEnabled = Boolean.parseBoolean(properties.getOrDefault(PROP_BQ_ENABLED, "false"));
        boolean fp16Enabled = Boolean.parseBoolean(properties.getOrDefault(PROP_FP16, "false"));

        CollectedVectors collected = collectVectors(values, dimension);
        List<float[]> vectors = collected.vectors();
        long[] rowOffsets = collected.rowOffsets();
        if (dimension < 1 && !vectors.isEmpty()) {
            dimension = vectors.get(0).length;
        }
        validateMemoryBudget(algorithm, vectors.size(), dimension, maxMemory, properties);

        vectors = applyQuantization(vectors, fp16Enabled, sqBits, bqEnabled);

        return switch (algorithm) {
            case HNSW -> hnswEngine.build(vectors, rowOffsets, dimension, metric, properties);
            case IVFPQ -> ivfpqEngine.build(vectors, rowOffsets, dimension, metric, properties);
            case DISK_ANN -> diskAnnOperations.build(vectors, dimension, metric, properties);
        };
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        // Vector indexes do not participate in file-level skip/keep filtering.
        return IndexEvalResult.KEEP;
    }

    /**
     * Returns whether this provider supports incremental index updates.
     *
     * <p>For vector indexes, incremental build is algorithm-dependent: IVFPQ
     * encodes new vectors into the existing codebook, and HNSW inserts new nodes
     * into the deserialized graph. DiskANN has no incremental merge and rejects
     * it (callers must rebuild from the full source set).</p>
     *
     * @return {@code true} — HNSW and IVFPQ support incremental build
     */
    @Override
    public boolean supportsIncrementalBuild() {
        return true;
    }

    /**
     * Incrementally updates existing vector index data with new values.
     *
     * <p>For IVFPQ: encodes new vectors into the existing codebook and inverted
     * lists without retraining centroids. For HNSW: inserts the new vectors as
     * additional nodes into the deserialized graph. For DiskANN: unsupported —
     * throws {@link UnsupportedOperationException} (no incremental merge; the
     * caller must rebuild from the full source set).</p>
     */
    @Override
    public IndexData buildIncremental(IndexData existingData,
                                      Iterator<List<Comparable<?>>> newValues,
                                      Map<String, String> properties) {
        if (!(existingData instanceof VectorIndexData vectorData)) {
            throw new IllegalArgumentException(
                    "Expected VectorIndexData but got " + existingData.getClass().getName());
        }
        int dimension = vectorData.dimension();
        DistanceMetric metric = vectorData.metric();
        List<float[]> newVectors = collectVectors(newValues, dimension).vectors();
        if (newVectors.isEmpty()) {
            return existingData;
        }
        return switch (vectorData.algorithmType()) {
            case IVFPQ -> ivfpqEngine.encodeIncremental(
                    vectorData, newVectors, dimension, metric, properties);
            case HNSW -> hnswEngine.buildIncremental(vectorData, newVectors, properties);
            case DISK_ANN -> diskAnnOperations.rebuild(
                    vectorData, newVectors, dimension, metric);
        };
    }

    @Override
    public List<VectorSearchResult.ScoredRow> vectorSearch(
            IndexData indexData, VectorSearchRequest request, Map<String, String> searchParams) {
        if (!(indexData instanceof VectorIndexData vectorData)) {
            throw new IllegalArgumentException("Expected VectorIndexData but got " + indexData.getClass().getName());
        }
        if (request.metric() != vectorData.metric()) {
            throw new MetricMismatchException(request.metric(), vectorData.metric());
        }
        if (request.queryVector().length != vectorData.dimension()) {
            throw new DimensionMismatchException(vectorData.dimension(), request.queryVector().length);
        }
        return switch (vectorData.algorithmType()) {
            case HNSW -> hnswEngine.search(vectorData, request, searchParams);
            case IVFPQ -> ivfpqEngine.search(vectorData, request, searchParams);
            case DISK_ANN -> diskAnnOperations.search(vectorData, request, searchParams);
        };
    }

    /**
     * Filtered vector search: delegates to engine-specific filtered search
     * with a row-level allow-set for pre-filter, iterative-scan, or
     * predicate-aware strategies.
     *
     * @param indexData    the vector index
     * @param request      search request (includes filtered strategy)
     * @param searchParams engine-specific search parameters
     * @param allowedRows  set of row offsets that pass the predicate filter
     *                     (null = unfiltered)
     * @return scored rows matching both ANN proximity and predicate filter
     */
    public List<VectorSearchResult.ScoredRow> filteredVectorSearch(
            IndexData indexData, VectorSearchRequest request,
            Map<String, String> searchParams, Set<Long> allowedRows) {
        if (!(indexData instanceof VectorIndexData vectorData)) {
            throw new IllegalArgumentException(
                    "Expected VectorIndexData but got " + indexData.getClass().getName());
        }
        if (allowedRows == null) {
            return vectorSearch(indexData, request, searchParams);
        }
        return switch (vectorData.algorithmType()) {
            case HNSW -> hnswEngine.search(vectorData, request, searchParams, allowedRows);
            case IVFPQ -> ivfpqEngine.search(vectorData, request, searchParams, allowedRows);
            case DISK_ANN -> diskAnnOperations.searchFiltered(
                    vectorData, request, searchParams, allowedRows);
        };
    }

    /**
     * Incrementally encodes new vectors into an existing IVFPQ index without retraining.
     * Reuses existing centroids and PQ codebook. Marks the index STALE when cumulative
     * new vectors exceed 30% of the original training set.
     *
     * <p>For HNSW, incremental builds are per-file independent graphs handled by
     * {@link org.pangrova.indexing.build.IndexBuildCoordinator} — no special method needed.</p>
     *
     * @param existingData the previously built IVFPQ index
     * @param newVectors   new vectors to encode
     * @param dimension    vector dimension
     * @param metric       distance metric
     * @param properties   build properties
     * @return updated VectorIndexData, or the original if algorithm is not IVFPQ
     */
    public VectorIndexData encodeIncremental(VectorIndexData existingData, List<float[]> newVectors,
                                             int dimension, DistanceMetric metric,
                                             Map<String, String> properties) {
        if (existingData.algorithmType() != VectorAlgorithmType.IVFPQ) {
            return existingData;
        }
        return ivfpqEngine.encodeIncremental(existingData, newVectors, dimension, metric, properties);
    }

    private static List<float[]> applyQuantization(List<float[]> vectors, boolean fp16Enabled,
                                                   int sqBits, boolean bqEnabled) {
        int quantizationCount = (fp16Enabled ? 1 : 0) + (sqBits > 0 ? 1 : 0) + (bqEnabled ? 1 : 0);
        if (quantizationCount > 1) {
            throw new IllegalArgumentException(
                    "Only one quantization method may be enabled at a time: "
                            + "fp16=" + fp16Enabled + " sq_bits=" + sqBits + " bq_enabled=" + bqEnabled);
        }
        if (fp16Enabled) {
            return quantizeFp16(vectors);
        }
        if (sqBits > 0) {
            return quantizeScalar(vectors, sqBits);
        }
        if (bqEnabled) {
            return quantizeBinaryDequantized(vectors);
        }
        return vectors;
    }

    /**
     * Converts the generic row iterator to a list of float arrays paired with
     * each vector's physical 0-based row position in the data file.
     * Each row is expected to contain a single column value that is either
     * a {@code float[]} or a {@code Number[]} / {@code List<Number>}.
     * Null rows are skipped (NULL vectors excluded from index build) but still
     * advance the physical row counter, so the retained {@code rowOffsets} match
     * the positions reported by {@code BruteForceVectorScanner} — ANN and
     * brute-force results must be interchangeable for the same file.
     */
    private static CollectedVectors collectVectors(Iterator<List<Comparable<?>>> values, int expectedDimension) {
        List<float[]> vectors = new ArrayList<>();
        java.util.List<Long> offsets = new ArrayList<>();
        long rowIndex = 0;
        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (row.isEmpty() || row.get(0) == null) {
                rowIndex++;
                continue;
            }
            float[] vector = toFloatArray(row.get(0));
            validateVector(vector);
            if (expectedDimension > 0 && vector.length != expectedDimension) {
                throw new DimensionMismatchException(expectedDimension, vector.length);
            }
            vectors.add(vector);
            offsets.add(rowIndex);
            rowIndex++;
        }
        long[] rowOffsets = new long[offsets.size()];
        for (int i = 0; i < rowOffsets.length; i++) {
            rowOffsets[i] = offsets.get(i);
        }
        return new CollectedVectors(vectors, rowOffsets);
    }

    /** Non-null vectors paired with their physical 0-based file row offsets (parallel arrays). */
    private record CollectedVectors(List<float[]> vectors, long[] rowOffsets) {}

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
        throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to float[]");
    }

    private static void validateVector(float[] vector) {
        for (float v : vector) {
            if (Float.isNaN(v) || Float.isInfinite(v)) {
                throw new IllegalArgumentException("Vector contains NaN or Infinity values");
            }
        }
    }

    private static void validateMemoryBudget(
            VectorAlgorithmType algorithm, int vectorCount, int dimension,
            long maxMemory, Map<String, String> properties) {
        long estimated = VectorBuildMemoryEstimator.estimatePeakMemory(
                vectorCount, dimension, algorithm, properties);
        if (estimated > maxMemory) {
            throw new IllegalArgumentException(
                    "Estimated memory " + estimated + " bytes exceeds budget " + maxMemory + " bytes"
                            + " for " + algorithm + " index with " + vectorCount + " vectors of dimension "
                                    + dimension);
        }
    }

    private static VectorAlgorithmType parseAlgorithm(Map<String, String> properties) {
        String algo = properties.get(PROP_ALGORITHM);
        if (algo == null) {
            return VectorAlgorithmType.HNSW; // default
        }
        return VectorAlgorithmType.valueOf(algo.toUpperCase());
    }

    private static DistanceMetric parseMetric(Map<String, String> properties) {
        String metric = properties.get(PROP_METRIC);
        if (metric == null) {
            return DistanceMetric.L2; // default
        }
        return DistanceMetric.valueOf(metric.toUpperCase());
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }

    private static long longProp(Map<String, String> props, String key, long defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Long.parseLong(val);
    }

    /**
     * Scalar quantization: clamp each float to [min, max] per dimension,
     * quantize to the given bit width, then dequantize back to float.
     * Reduces memory during graph construction while preserving distance ordering.
     */
    static List<float[]> quantizeScalar(List<float[]> vectors, int bits) {
        if (vectors.isEmpty()) {
            return vectors;
        }
        int dim = vectors.get(0).length;
        DimensionRange range = computeDimensionRange(vectors, dim);
        float[] mins = range.mins();
        float[] maxs = range.maxs();
        int levels = (1 << bits) - 1;
        List<float[]> result = new ArrayList<>(vectors.size());
        for (float[] v : vectors) {
            float[] q = new float[dim];
            for (int d = 0; d < dim; d++) {
                float span = maxs[d] - mins[d];
                if (span == 0.0f) {
                    q[d] = v[d];
                } else {
                    int bucket = Math.round((v[d] - mins[d]) / span * levels);
                    q[d] = mins[d] + ((float) bucket / levels) * span;
                }
            }
            result.add(q);
        }
        return result;
    }

    /** Per-dimension minimum and maximum values across all vectors. */
    private record DimensionRange(float[] mins, float[] maxs) {}

    private static DimensionRange computeDimensionRange(List<float[]> vectors, int dim) {
        float[] mins = new float[dim];
        float[] maxs = new float[dim];
        System.arraycopy(vectors.get(0), 0, mins, 0, dim);
        System.arraycopy(vectors.get(0), 0, maxs, 0, dim);
        for (float[] v : vectors) {
            for (int d = 0; d < dim; d++) {
                if (v[d] < mins[d]) mins[d] = v[d];
                if (v[d] > maxs[d]) maxs[d] = v[d];
            }
        }
        return new DimensionRange(mins, maxs);
    }

    /**
     * Binary quantization: each dimension is mapped to +1 (≥ mean) or -1 (< mean).
     * Returns dequantized float vectors for compatibility with existing distance functions.
     * Reduces effective precision to 1 bit per dimension.
     */
    static List<float[]> quantizeBinaryDequantized(List<float[]> vectors) {
        if (vectors.isEmpty()) {
            return vectors;
        }
        int dim = vectors.get(0).length;
        float[] means = new float[dim];
        for (float[] v : vectors) {
            for (int d = 0; d < dim; d++) {
                means[d] += v[d];
            }
        }
        for (int d = 0; d < dim; d++) {
            means[d] /= vectors.size();
        }
        List<float[]> result = new ArrayList<>(vectors.size());
        for (float[] v : vectors) {
            float[] q = new float[dim];
            for (int d = 0; d < dim; d++) {
                q[d] = v[d] >= means[d] ? 1.0f : -1.0f;
            }
            result.add(q);
        }
        return result;
    }

    /**
     * FP16 quantization: truncates the low 13 mantissa bits of each float,
     * simulating IEEE 754 binary16 precision without requiring Java 20+
     * {@code Float.float16ToFloat}.
     *
     * <p>Values outside the FP16 representable range [-65504, 65504] are
     * clamped before truncation.</p>
     */
    static List<float[]> quantizeFp16(List<float[]> vectors) {
        List<float[]> result = new ArrayList<>(vectors.size());
        for (float[] v : vectors) {
            float[] q = new float[v.length];
            for (int d = 0; d < v.length; d++) {
                q[d] = fp16RoundTrip(v[d]);
            }
            result.add(q);
        }
        return result;
    }

    /**
     * Simulates an FP32 -> FP16 -> FP32 round-trip by zeroing the low
     * 13 mantissa bits (IEEE 754 binary32 has 23 mantissa bits; binary16
     * has 10, so we keep only the top 10).
     *
     * <p>Clamps to the FP16 max representable magnitude (65504) before
     * truncation. NaN is preserved as-is.</p>
     */
    static float fp16RoundTrip(float value) {
        if (Float.isNaN(value)) {
            return Float.NaN;
        }
        // Clamp to FP16 max range
        value = Math.max(-FP16_MAX_MAGNITUDE, Math.min(FP16_MAX_MAGNITUDE, value));
        int bits = Float.floatToRawIntBits(value);
        // Zero the low 13 mantissa bits to simulate FP16 precision
        bits &= FP16_MANTISSA_CLEAR_MASK;
        return Float.intBitsToFloat(bits);
    }

}
