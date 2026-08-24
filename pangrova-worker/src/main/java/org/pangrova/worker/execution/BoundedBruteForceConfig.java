package org.pangrova.worker.execution;

/**
 * Configuration for bounded brute-force vector search fallback.
 *
 * <p>When ANN index coverage is incomplete, unindexed files are scanned via
 * linear distance computation. This config limits the brute-force scan to
 * prevent unbounded resource consumption on very large unindexed files.</p>
 *
 * @param maxRowsPerFile  maximum rows to scan per unindexed file (0 = unlimited)
 * @param maxFilesTotal   maximum total unindexed files to scan per query (0 = unlimited)
 * @param timeoutMs       per-file scan timeout in milliseconds (0 = no timeout)
 */
public record BoundedBruteForceConfig(
        long maxRowsPerFile,
        int maxFilesTotal,
        long timeoutMs) {

    /** Default config: 100K rows per file, 50 files max, 30s timeout. */
    public static final BoundedBruteForceConfig DEFAULT =
            new BoundedBruteForceConfig(100_000, 50, 30_000);

    public BoundedBruteForceConfig {
        if (maxRowsPerFile < 0) {
            throw new IllegalArgumentException("maxRowsPerFile must be non-negative");
        }
        if (maxFilesTotal < 0) {
            throw new IllegalArgumentException("maxFilesTotal must be non-negative");
        }
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must be non-negative");
        }
    }
}
