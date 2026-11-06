package org.pangrova.indexing.api.evaluation;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.connector.api.index.IndexEvalResult;

import java.util.List;
import java.util.Map;

/**
 * Index evaluation context attached to a split by the coordinator.
 *
 * <p>Contains two types of information:
 * <ul>
 *   <li>{@code fileResults}: Pre-computed L0/L1 evaluation results from the
 *       coordinator. Files marked SKIP are filtered out by
 *       {@link IndexEvaluatedSplit#dataFilePaths()}.</li>
 *   <li>{@code evalHints}: Per-file index hints for worker-side L2 evaluation.
 *       The worker loads index data and evaluates these hints before reading
 *       data files, enabling data skipping without coordinator memory pressure.</li>
 * </ul>
 *
 * @param fileResults per-data-file L0/L1 evaluation results (dataFilePath → result)
 * @param evalHints   per-data-file L2 evaluation hints for worker (dataFilePath → hints)
 */
@Experimental
public record SplitIndexEvalResult(
        Map<String, IndexEvalResult> fileResults,
        Map<String, List<IndexEvalHint>> evalHints) {

    /** Backward-compatible constructor without eval hints. */
    public SplitIndexEvalResult(Map<String, IndexEvalResult> fileResults) {
        this(fileResults, Map.of());
    }

    public SplitIndexEvalResult {
        fileResults = fileResults != null ? Map.copyOf(fileResults) : Map.of();
        evalHints = evalHints != null ? Map.copyOf(evalHints) : Map.of();
    }

    /** Returns true if the given data file was evaluated and can be skipped. */
    public boolean shouldSkipFile(String dataFilePath) {
        return fileResults.getOrDefault(dataFilePath, IndexEvalResult.UNKNOWN)
                == IndexEvalResult.SKIP;
    }

    /** Returns the list of data files that should be kept (not SKIP). */
    public List<String> keepableFiles(List<String> allFiles) {
        return allFiles.stream()
                .filter(f -> !shouldSkipFile(f))
                .toList();
    }

    /** Returns true if there are L2 eval hints for the worker to process. */
    public boolean hasEvalHints() {
        return !evalHints.isEmpty();
    }

    /** Returns L2 eval hints for a specific data file, or empty list. */
    public List<IndexEvalHint> hintsForFile(String dataFilePath) {
        return evalHints.getOrDefault(dataFilePath, List.of());
    }

    /** Empty result — no pre-computed evaluations or hints. */
    public static SplitIndexEvalResult empty() {
        return new SplitIndexEvalResult(Map.of(), Map.of());
    }
}
