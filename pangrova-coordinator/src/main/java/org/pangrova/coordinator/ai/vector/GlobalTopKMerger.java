package org.pangrova.coordinator.ai.vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Coordinator-side global Top-K merge for vector search results collected
 * from multiple Workers.
 *
 * <p>Each Worker returns its local Top-K results (from ANN index + brute-force
 * fallback); this merger produces the global Top-K by heap-merging all worker
 * results. Pre-filter predicates (WHERE clauses) are retained from the query
 * plan and applied post-merge if not already enforced at the scan level.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class GlobalTopKMerger {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalTopKMerger.class);

    private GlobalTopKMerger() {}

    /**
     * A scored row from a worker's local vector search result.
     *
     * @param workerId   the worker that produced this result
     * @param rowId      row identifier (file-local offset or unique ID)
     * @param distance   distance/score from the query vector (lower = closer for L2/cosine)
     * @param filePath   the data file containing this row
     * @param fromBruteForce whether this result came from a brute-force scan (no index)
     */
    public record ScoredRow(String workerId, long rowId, double distance, String filePath,
                            boolean fromBruteForce) {
        public ScoredRow {
            Objects.requireNonNull(workerId, "workerId required");
            Objects.requireNonNull(filePath, "filePath required");
        }

        /**
         * Convenience constructor for non-brute-force results (backward compatible).
         */
        public ScoredRow(String workerId, long rowId, double distance, String filePath) {
            this(workerId, rowId, distance, filePath, false);
        }
    }

    /**
     * The merged global Top-K result.
     *
     * @param rows            merged rows sorted by ascending distance
     * @param totalScanned    total rows scanned across all workers
     * @param workerCount     number of workers that contributed results
     * @param bruteForceCount number of brute-force scanned files across all workers
     */
    public record MergeResult(
            List<ScoredRow> rows,
            long totalScanned,
            int workerCount,
            int bruteForceCount) {

        public MergeResult {
            rows = List.copyOf(rows);
        }
    }

    /**
     * Merges per-worker vector search results into a global Top-K result set.
     *
     * @param workerResults list of per-worker result sets
     * @param topK          global Top-K to retain
     * @return the merged result
     */
    public static MergeResult merge(List<List<ScoredRow>> workerResults, int topK) {
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        Objects.requireNonNull(workerResults, "workerResults required");

        // Max-heap (evict largest distance when heap exceeds topK)
        PriorityQueue<ScoredRow> heap = new PriorityQueue<>(
                Math.max(topK, 1),
                Comparator.comparingDouble(ScoredRow::distance).reversed());

        Set<String> seen = new HashSet<>();
        long totalScanned = 0;
        int bruteForceScanned = 0;

        for (List<ScoredRow> workerResult : workerResults) {
            for (ScoredRow row : workerResult) {
                totalScanned++;
                if (row.fromBruteForce()) {
                    bruteForceScanned++;
                }
                String dedupKey = row.filePath() + ":" + row.rowId();
                if (!seen.add(dedupKey)) {
                    continue;
                }
                heap.offer(row);
                if (heap.size() > topK) {
                    heap.poll();
                }
            }
        }

        List<ScoredRow> merged = new ArrayList<>(heap);
        merged.sort(Comparator.comparingDouble(ScoredRow::distance));

        LOG.debug("event=global_topk_merged topK={} workers={} totalScanned={} bruteForce={} resultSize={}",
                topK, workerResults.size(), totalScanned, bruteForceScanned, merged.size());

        return new MergeResult(merged, totalScanned, workerResults.size(), bruteForceScanned);
    }
}
