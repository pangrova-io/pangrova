package org.pangrova.indexing.config;

/**
 * Immutable configuration for vector index algorithms.
 *
 * <p>Organized as nested records to keep all vector-specific tuning
 * in a single config entry point while respecting the ≤4 constructor
 * params governance rule at each level.</p>
 *
 * @param hnsw    HNSW algorithm parameters
 * @param ivfpq   IVFPQ algorithm parameters
 * @param hybrid  hybrid search strategy parameters
 * @param build   build resource constraints
 */
public record VectorIndexConfig(
        HnswConfig hnsw,
        IvfpqConfig ivfpq,
        HybridConfig hybrid,
        BuildConfig build
) {

    public VectorIndexConfig {
        if (hnsw == null) hnsw = HnswConfig.defaults();
        if (ivfpq == null) ivfpq = IvfpqConfig.defaults();
        if (hybrid == null) hybrid = HybridConfig.defaults();
        if (build == null) build = BuildConfig.defaults();
    }

    public static VectorIndexConfig defaults() {
        return new VectorIndexConfig(null, null, null, null);
    }

    /**
     * HNSW graph construction and search parameters.
     *
     * @param m              max connections per node per layer, range [4, 64]
     * @param efConstruction build-time candidate list size, range [M, 4096]
     * @param efSearch       search-time candidate list size, range [1, 4096]
     */
    public record HnswConfig(int m, int efConstruction, int efSearch) {

        /** Upper bound for efConstruction and efSearch candidate list sizes. */
        private static final int MAX_EF = 4096;
        /** Default build-time candidate list size. */
        private static final int DEFAULT_EF_CONSTRUCTION = 200;

        public HnswConfig {
            if (m < 4 || m > 64) {
                throw new IllegalArgumentException("hnsw.M must be in [4, 64], got " + m);
            }
            if (efConstruction < m || efConstruction > MAX_EF) {
                throw new IllegalArgumentException(
                        "hnsw.efConstruction must be in [" + m + ", " + MAX_EF + "], got "
                                + efConstruction);
            }
            if (efSearch < 1 || efSearch > MAX_EF) {
                throw new IllegalArgumentException(
                        "hnsw.efSearch must be in [1, " + MAX_EF + "], got " + efSearch);
            }
        }

        public static HnswConfig defaults() {
            return new HnswConfig(16, DEFAULT_EF_CONSTRUCTION, 64);
        }
    }

    /**
     * IVFPQ clustering and quantization parameters.
     *
     * @param nlist  number of IVF clusters, range [1, 65536]
     * @param nprobe clusters to probe at search time, range [1, nlist]
     * @param m      number of PQ sub-quantizers (dimension must be divisible by m)
     * @param nbits  bits per sub-quantizer code, one of {4, 8, 16}
     */
    public record IvfpqConfig(int nlist, int nprobe, int m, int nbits) {

        /** Upper bound for the number of IVF clusters. */
        private static final int MAX_NLIST = 65536;

        public IvfpqConfig {
            if (nlist < 1 || nlist > MAX_NLIST) {
                throw new IllegalArgumentException(
                        "ivfpq.nlist must be in [1, " + MAX_NLIST + "], got " + nlist);
            }
            if (nprobe < 1 || nprobe > nlist) {
                throw new IllegalArgumentException(
                        "ivfpq.nprobe must be in [1, " + nlist + "], got " + nprobe);
            }
            if (m < 1) {
                throw new IllegalArgumentException("ivfpq.m must be positive, got " + m);
            }
            if (!isValidNbits(nbits)) {
                throw new IllegalArgumentException("ivfpq.nbits must be 4, 8, or 16, got " + nbits);
            }
        }

        private static boolean isValidNbits(int nbits) {
            return nbits == 4 || nbits == 8 || nbits == 16;
        }

        public static IvfpqConfig defaults() {
            return new IvfpqConfig(256, 16, 8, 8);
        }
    }

    /**
     * Hybrid search strategy parameters for combining ANN with pre-filters.
     *
     * @param threshold           selectivity threshold in (0, 1) for strategy selection
     * @param overFetchMultiplier multiplier for over-fetching candidates, must be ≥ 1.0
     */
    public record HybridConfig(double threshold, double overFetchMultiplier) {

        /** Default selectivity threshold for strategy selection. */
        private static final double DEFAULT_THRESHOLD = 0.1;
        /** Default over-fetch multiplier. */
        private static final double DEFAULT_OVER_FETCH_MULTIPLIER = 2.0;

        public HybridConfig {
            if (threshold <= 0.0 || threshold >= 1.0) {
                throw new IllegalArgumentException(
                        "hybrid.threshold must be in (0, 1), got " + threshold);
            }
            if (overFetchMultiplier < 1.0) {
                throw new IllegalArgumentException(
                        "hybrid.overFetchMultiplier must be >= 1.0, got " + overFetchMultiplier);
            }
        }

        public static HybridConfig defaults() {
            return new HybridConfig(DEFAULT_THRESHOLD, DEFAULT_OVER_FETCH_MULTIPLIER);
        }
    }

    /**
     * Build resource constraints for vector index construction.
     *
     * @param maxMemoryBytes            peak memory budget in bytes, must be ≥ 1 MB
     * @param scalarQuantizationBits    scalar quantization bit width (0=disabled, 4 or 8 for SQ4/SQ8)
     * @param binaryQuantizationEnabled whether to enable binary quantization (1-bit per dimension)
     */
    public record BuildConfig(long maxMemoryBytes,
                              int scalarQuantizationBits, boolean binaryQuantizationEnabled) {

        private static final long MIN_MEMORY_BYTES = 1024L * 1024; // 1 MB

        public BuildConfig {
            if (maxMemoryBytes < MIN_MEMORY_BYTES) {
                throw new IllegalArgumentException(
                        "build.maxMemoryBytes must be >= 1 MB, got " + maxMemoryBytes);
            }
            if (scalarQuantizationBits != 0 && scalarQuantizationBits != 4
                    && scalarQuantizationBits != 8) {
                throw new IllegalArgumentException(
                        "build.scalarQuantizationBits must be 0 (disabled), 4, or 8, got "
                                + scalarQuantizationBits);
            }
        }

        public static BuildConfig defaults() {
            return new BuildConfig(256L * 1024 * 1024, 0, false);
        }
    }
}
