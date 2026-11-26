package org.pangrova.indexing.pruning;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.indexing.api.evaluation.CatalogIndexEvaluation;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexFileSelection;
import org.pangrova.indexing.evaluation.IndexEvaluationFailures;
import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.connector.api.index.NativeIndexCapability;
import org.pangrova.connector.api.index.NativeIndexEvaluator;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.evaluation.IndexEvalHint;
import org.pangrova.indexing.api.evaluation.IndexEvaluatedSplit;
import org.pangrova.indexing.api.evaluation.SplitIndexEvalResult;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.RangeQuery;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Filters splits using a hybrid L0/L1 coordinator + L2 worker architecture.
 * L0: file column statistics (min/max). L1: connector-native evaluation.
 * Surviving files get {@link IndexEvalHint} for worker-side L2 evaluation.
 * Safety: ordinary evaluation failures retain the split; authority and cancellation abort the query.
 */
@ThreadSafe
public final class IndexSplitFilter implements AutoCloseable {
    private static final int EXECUTOR_SHUTDOWN_SECONDS = 30;

    private static final Logger LOG = LoggerFactory.getLogger(IndexSplitFilter.class);
    private static final int PARALLEL_THRESHOLD = 100;
    /** Skip L1 when L0 already pruned this fraction of splits. */
    private static final double L0_PRUNE_SKIP_THRESHOLD = 0.95;
    /** Skip an index hint when its file mapping coverage is below this fraction. */
    private static final double COVERAGE_SKIP_THRESHOLD = 0.50;

    private final IndexMetadataStore metadataStore;
    private final IndexEvalConfig evalConfig;
    private final ExecutorService evalPool;

    /**
     * @param metadataStore     index metadata access
     * @param evalConfig        evaluation configuration (metrics, timeout, parallelism)
     */
    public IndexSplitFilter(
            IndexMetadataStore metadataStore,
            IndexEvalConfig evalConfig) {
        this.metadataStore = Objects.requireNonNull(metadataStore);
        this.evalConfig = Objects.requireNonNull(evalConfig);
        this.evalPool = evalConfig.evalParallelism() > 0
                ? ManagedExecutorFactory.newFixedThreadPool("index-eval", evalConfig.evalParallelism())
                : null;
    }

    @Override
    public void close() {
        ResourceCloser.shutdownExecutor(evalPool, java.time.Duration.ofSeconds(EXECUTOR_SHUTDOWN_SECONDS));
    }

    /** Filters splits by evaluating index hints (convenience overload without snapshot). */
    public List<ConnectorSplit> filterSplits(
            List<ConnectorSplit> splits,
            List<IndexHint> indexHints,
            String catalogName,
            CatalogIndexEvaluation accessorResolver) {
        return filterSplits(splits, indexHints, catalogName, accessorResolver, null);
    }

    /** Filters splits with snapshot-aware consistency (convenience overload without L0/LIMIT). */
    public List<ConnectorSplit> filterSplits(
            List<ConnectorSplit> splits,
            List<IndexHint> indexHints,
            String catalogName,
            CatalogIndexEvaluation accessorResolver,
            String querySnapshotToken) {
        return filterSplits(splits, indexHints, catalogName, accessorResolver,
                querySnapshotToken, null, 0);
    }

    /**
     * Filters splits with full context: snapshot consistency, L0 statistics, LIMIT pruning.
     * Pipeline: L0 (min/max) → L1 (native eval) → attach L2 hints for worker.
     */
    public List<ConnectorSplit> filterSplits(
            List<ConnectorSplit> splits,
            List<IndexHint> indexHints,
            String catalogName,
            CatalogIndexEvaluation accessorResolver,
            String querySnapshotToken,
            Map<String, List<FileColumnStatistics>> fileStatistics,
            long limitK) {
        IndexEvaluationFailures.checkInterrupted();
        if (indexHints.isEmpty()) {
            return splits;
        }

        List<String> dataFilePaths = splits.stream()
                .flatMap(split -> split.dataFilePaths().stream())
                .distinct().toList();
        long metadataStart = System.nanoTime();
        Map<String, IndexFileSelection> selections = new HashMap<>();
        for (IndexHint hint : indexHints) {
            var selection = new IndexFileSelection(hint.generation(), querySnapshotToken);
            var previous = selections.putIfAbsent(hint.indexId(), selection);
            if (previous != null && !previous.equals(selection)) {
                throw new IllegalArgumentException("Conflicting generation selections for " + hint.indexId());
            }
        }
        var indexFileMappings = metadataStore.batchGetIndexArtifacts(selections, dataFilePaths);
        if (evalConfig.evalMetrics() != null) {
            evalConfig.evalMetrics().recordMetadataBatchLatency(
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - metadataStart));
        }
        var qualifying = new java.util.HashSet<String>();
        for (String indexId : selections.keySet()) {
            var mappings = indexFileMappings.getOrDefault(indexId, Map.of());
            if (!shouldSkipLowCoverage(indexId, mappings.size(), dataFilePaths.size()) && !mappings.isEmpty()) {
                qualifying.add(indexId);
            }
        }
        List<IndexHint> effectiveHints = indexHints.stream()
                .filter(hint -> qualifying.contains(hint.indexId())).toList();
        if (effectiveHints.isEmpty()) {
            return splits;
        }
        // Resolve NativeIndexEvaluator and capabilities ONCE
        NativeIndexEvaluator nativeEvaluator = resolveNativeEvaluator(
                catalogName, accessorResolver);
        List<NativeIndexCapability> nativeCapabilities = nativeEvaluator != null
                ? resolveNativeCapabilities(catalogName, accessorResolver)
                : List.of();

        var ctx = new SplitEvalContext(
                nativeEvaluator, nativeCapabilities,
                indexFileMappings, querySnapshotToken,
                fileStatistics, limitK, null);

        boolean parallel = evalPool != null
                && (long) splits.size() * effectiveHints.size() > PARALLEL_THRESHOLD;

        long evalStart = System.currentTimeMillis();
        List<ConnectorSplit> result = parallel
                ? filterParallel(splits, effectiveHints, ctx)
                : filterSerial(splits, effectiveHints, ctx);

        // LIMIT pruning: if query has LIMIT k, try to reduce to fully-matching files
        result = applyLimitPruning(result, effectiveHints, ctx);

        if (evalConfig.evalMetrics() != null) {
            long evalDuration = System.currentTimeMillis() - evalStart;
            int skipped = splits.size() - result.size();
            evalConfig.evalMetrics().recordFilterCall(evalDuration, splits.size(), skipped);
            if (parallel) {
                evalConfig.evalMetrics().recordParallelTaskCount(splits.size());
            }
        }

        return result;
    }

    private NativeIndexEvaluator resolveNativeEvaluator(
            String catalogName, CatalogIndexEvaluation resolver) {
        if (resolver == null) {
            return null;
        }
        try {
            NativeIndexEvaluator evaluator = resolver.resolveNativeIndexEvaluator(catalogName);
            if (evaluator == null) {
                LOG.warn("event=native_evaluator_unavailable catalog={} "
                        + "detail=L1 index filtering disabled, only L0 statistics pruning active",
                        catalogName);
            }
            return evaluator;
        } catch (org.pangrova.security.api.exception.AccessDeniedException | SecurityException authorityFailure) {
            throw authorityFailure;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("event=native_evaluator_resolve_failed catalog={} error={} "
                    + "detail=L1 index filtering disabled, only L0 statistics pruning active",
                    catalogName, e.getMessage());
            if (evalConfig.evalMetrics() != null) {
                evalConfig.evalMetrics().recordNativeEvalFailure();
            }
            return null;
        }
    }

    private List<NativeIndexCapability> resolveNativeCapabilities(
            String catalogName, CatalogIndexEvaluation resolver) {
        try {
            return resolver.resolveNativeCapabilities(catalogName);
        } catch (org.pangrova.security.api.exception.AccessDeniedException | SecurityException authorityFailure) {
            throw authorityFailure;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.debug("event=native_capabilities_resolve_failed catalog={}", catalogName);
            return List.of();
        }
    }

    // ── Serial filtering ────────────────────────────────────────────────

    private List<ConnectorSplit> filterSerial(
            List<ConnectorSplit> splits,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        List<ConnectorSplit> result = new ArrayList<>(splits.size());
        int l0PruneCount = 0;
        for (ConnectorSplit split : splits) {
            List<String> filePaths = split.dataFilePaths();
            if (filePaths.isEmpty() || shouldSkipL1Eval(l0PruneCount, splits.size())) {
                result.add(split);
                continue;
            }
            if (tryL0PruneAllFiles(filePaths, indexHints, ctx)) {
                l0PruneCount++;
                continue;
            }
            ConnectorSplit evaluated = evaluateAndWrapSplit(split, filePaths, indexHints, ctx);
            if (evaluated != null) {
                result.add(evaluated);
            }
        }
        return List.copyOf(result);
    }

    private boolean shouldSkipL1Eval(int l0PruneCount, int totalSplits) {
        if (totalSplits <= 0 || (double) l0PruneCount / totalSplits <= L0_PRUNE_SKIP_THRESHOLD) {
            return false;
        }
        if (evalConfig.evalMetrics() != null) {
            evalConfig.evalMetrics().recordCostSkip();
        }
        return true;
    }

    // ── Parallel filtering ──────────────────────────────────────────────

    private List<ConnectorSplit> filterParallel(
            List<ConnectorSplit> splits,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        L0PhaseResult l0Result = runL0Phase(splits, indexHints, ctx);
        if (shouldSkipL1Eval(l0Result.pruneCount, splits.size())) {
            return List.copyOf(l0Result.survivors);
        }
        return runL1Phase(l0Result.survivors, indexHints, ctx);
    }

    private record L0PhaseResult(List<ConnectorSplit> survivors, int pruneCount) {}

    private L0PhaseResult runL0Phase(List<ConnectorSplit> splits, List<IndexHint> indexHints,
                                     SplitEvalContext ctx) {
        List<ConnectorSplit> survivors = new ArrayList<>(splits.size());
        int pruneCount = 0;
        for (ConnectorSplit split : splits) {
            List<String> filePaths = split.dataFilePaths();
            if (filePaths.isEmpty() || !tryL0PruneAllFiles(filePaths, indexHints, ctx)) {
                survivors.add(split);
            } else {
                pruneCount++;
            }
        }
        return new L0PhaseResult(survivors, pruneCount);
    }

    private List<ConnectorSplit> runL1Phase(List<ConnectorSplit> survivors, List<IndexHint> indexHints,
                                            SplitEvalContext ctx) {
        List<CompletableFuture<ConnectorSplit>> futures = survivors.stream()
                .map(split -> CompletableFuture.supplyAsync(
                        () -> evaluateAndWrapSplit(split, split.dataFilePaths(),
                                indexHints, ctx), evalPool))
                .toList();
        return collectL1Results(survivors, futures);
    }

    List<ConnectorSplit> collectL1Results(List<ConnectorSplit> survivors,
                                                  List<CompletableFuture<ConnectorSplit>> futures) {
        List<ConnectorSplit> result = new ArrayList<>(survivors.size());
        try {
            for (int i = 0; i < survivors.size(); i++) {
                ConnectorSplit evaluated = awaitFuture(futures.get(i), survivors.get(i), i);
                if (evaluated != null) {
                    result.add(evaluated);
                }
            }
            return List.copyOf(result);
        } catch (RuntimeException failure) {
            futures.forEach(future -> future.cancel(true));
            throw failure;
        }
    }

    ConnectorSplit awaitFuture(CompletableFuture<ConnectorSplit> future,
                                       ConnectorSplit fallback, int index) {
        try {
            IndexEvaluationFailures.checkInterrupted();
            return future.get(evalConfig.evalTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            IndexEvaluationFailures.checkInterrupted();
            if (evalConfig.evalMetrics() != null) {
                evalConfig.evalMetrics().recordTimeout();
            }
            LOG.warn("event=index_eval_timeout splitIndex={} timeoutMs={}", index, evalConfig.evalTimeoutMs());
            return fallback;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("event=index_eval_parallel_failed error={}", e.getMessage());
            return fallback;
        }
    }

    // ── L0 pruning (delegated to L0StatisticsPruner) ──────────────────

    private boolean tryL0PruneAllFiles(
            List<String> filePaths,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        boolean pruned = L0StatisticsPruner.tryPruneAllFiles(filePaths, indexHints, ctx);
        if (pruned && evalConfig.evalMetrics() != null) {
            evalConfig.evalMetrics().recordL0Prune();
        }
        return pruned;
    }

    // ── L1 + Hint generation (per-split) ────────────────────────────────

    /** Evaluates a split through L1 (native) + generates L2 hints. Returns null if all files pruned. */
    private ConnectorSplit evaluateAndWrapSplit(
            ConnectorSplit split,
            List<String> dataFilePaths,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        if (dataFilePaths.isEmpty()) {
            return split;
        }
        Map<String, IndexEvalResult> fileResults = new HashMap<>();
        Map<String, List<IndexEvalHint>> fileHints = new HashMap<>();
        boolean anyKeep = false;
        for (String dataFilePath : dataFilePaths) {
            anyKeep |= evaluateSingleFile(
                    dataFilePath, split, indexHints, ctx, fileResults, fileHints);
        }
        return anyKeep
                ? new IndexEvaluatedSplit(split, new SplitIndexEvalResult(fileResults, fileHints))
                : null;
    }

    private boolean evaluateSingleFile(String dataFilePath, ConnectorSplit split,
                                       List<IndexHint> indexHints, SplitEvalContext ctx,
                                       Map<String, IndexEvalResult> fileResults,
                                       Map<String, List<IndexEvalHint>> fileHints) {
        if (AvailabilityWindowChecker.isOutsideAvailabilityWindow(split, indexHints, ctx)) {
            return recordAvailabilityKeep(dataFilePath, fileResults);
        }
        return evaluateFileWithIndex(dataFilePath, indexHints, ctx, fileResults, fileHints);
    }

    private boolean recordAvailabilityKeep(String dataFilePath,
                                           Map<String, IndexEvalResult> fileResults) {
        fileResults.put(dataFilePath, IndexEvalResult.KEEP);
        if (evalConfig.evalMetrics() != null) {
            evalConfig.evalMetrics().recordAvailabilityGapKeep();
        }
        return true;
    }

    private boolean evaluateFileWithIndex(String dataFilePath, List<IndexHint> indexHints,
                                          SplitEvalContext ctx,
                                          Map<String, IndexEvalResult> fileResults,
                                          Map<String, List<IndexEvalHint>> fileHints) {
        IndexEvalResult l1Result = evaluateL1(dataFilePath, indexHints, ctx);
        if (l1Result == IndexEvalResult.SKIP) {
            fileResults.put(dataFilePath, IndexEvalResult.SKIP);
            return false;
        }
        List<IndexEvalHint> hints = buildEvalHints(dataFilePath, indexHints, ctx);
        if (!hints.isEmpty()) {
            fileHints.put(dataFilePath, hints);
        }
        fileResults.put(dataFilePath, IndexEvalResult.KEEP);
        return true;
    }

    /** L1: Native connector evaluation. Returns SKIP if no match, KEEP otherwise. */
    private IndexEvalResult evaluateL1(
            String dataFilePath,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        if (ctx.nativeEvaluator() == null) {
            return IndexEvalResult.KEEP;
        }
        trackDeleteFiles(dataFilePath, ctx);
        return invokeNativeEval(dataFilePath, indexHints, ctx);
    }

    private void trackDeleteFiles(String dataFilePath, SplitEvalContext ctx) {
        if (ctx.hasDeleteFiles(dataFilePath)) {
            LOG.debug("event=index_eval_with_delete_files file={}", dataFilePath);
            if (evalConfig.evalMetrics() != null) {
                evalConfig.evalMetrics().recordEvalWithDeleteFiles();
            }
        }
    }

    private IndexEvalResult invokeNativeEval(String dataFilePath, List<IndexHint> indexHints,
                                             SplitEvalContext ctx) {
        try {
            IndexEvaluationFailures.checkInterrupted();
            List<IndexPredicate> predicates = indexHints.stream()
                    .map(IndexHint::predicate).toList();
            IndexEvalResult nativeResult = ctx.nativeEvaluator().evaluate(
                    dataFilePath, predicates, ctx.nativeCapabilities());
            IndexEvaluationFailures.checkInterrupted();
            return nativeResult == IndexEvalResult.SKIP
                    ? IndexEvalResult.SKIP : IndexEvalResult.KEEP;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("event=native_eval_failed file={} error={} "
                    + "detail=file retained conservatively",
                    dataFilePath, e.getMessage());
            if (evalConfig.evalMetrics() != null) {
                evalConfig.evalMetrics().recordNativeEvalFailure();
            }
            return IndexEvalResult.KEEP;
        }
    }

    /** Builds L2 eval hints from index metadata for a single data file. No index data loaded. */
    private List<IndexEvalHint> buildEvalHints(
            String dataFilePath,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        List<IndexEvalHint> hints = new ArrayList<>();
        for (IndexHint hint : indexHints) {
            IndexEvalHint evalHint = buildSingleHint(dataFilePath, hint, ctx);
            if (evalHint != null) {
                hints.add(evalHint);
            }
        }
        return hints;
    }

    private IndexEvalHint buildSingleHint(String dataFilePath, IndexHint hint, SplitEvalContext ctx) {
        if (hint.indexType() == IndexType.BLOOM_FILTER
                && !hasNativeBloomKeyContract(hint.predicate())) {
            // The protocol string variant also carries Calcite DATE/TIMESTAMP
            // and DECIMAL fallback values. Their toString() is not the index
            // writer's verified canonical key, so Native must not prune them.
            return null;
        }
        if (hint.indexType() == IndexType.BTREE && !hasNativeBTreeContract(hint)) {
            return null;
        }
        Map<String, IndexFileMapping> fileMappings = ctx.indexFileMappings()
                .getOrDefault(hint.indexId(), Map.of());
        IndexFileMapping mapping = fileMappings.get(dataFilePath);
        if (mapping == null || mapping.generation() != hint.generation()) {
            return null;
        }
        return new IndexEvalHint(hint.indexId(), hint.indexType(),
                hint.predicate(), mapping.indexFilePath(), mapping.generation(),
                mapping.indexSizeBytes(), mapping.checksum());
    }

    static boolean hasNativeBTreeContract(IndexHint hint) {
        if (hint.descriptor().columnNames().size() != 1) {
            return false;
        }
        if (hint.predicate() instanceof EqualTo equal) {
            return hasNativeBTreeKeyContract(equal.value());
        }
        if (hint.predicate() instanceof RangeQuery range) {
            return (range.lower() != null || range.upper() != null)
                    && (range.lower() == null || hasNativeBTreeKeyContract(range.lower()))
                    && (range.upper() == null || hasNativeBTreeKeyContract(range.upper()));
        }
        if (hint.predicate() instanceof InList inList) {
            return !inList.values().isEmpty()
                    && inList.values().stream().allMatch(IndexSplitFilter::hasNativeBTreeKeyContract);
        }
        return false;
    }

    private static boolean hasNativeBTreeKeyContract(Comparable<?> value) {
        return value instanceof String || value instanceof Integer || value instanceof Long
                || value instanceof Double || value instanceof Float;
    }

    private static boolean hasNativeBloomKeyContract(IndexPredicate predicate) {
        if (predicate instanceof EqualTo equal) {
            return hasNativeBloomKeyContract(equal.value());
        }
        if (predicate instanceof InList in) {
            return in.values().stream().allMatch(IndexSplitFilter::hasNativeBloomKeyContract);
        }
        return false;
    }

    private static boolean hasNativeBloomKeyContract(Comparable<?> value) {
        return value instanceof String || value instanceof Long
                || value instanceof Integer || value instanceof Boolean;
    }

    private boolean shouldSkipLowCoverage(String indexId, int mappedFiles, int totalDataFiles) {
        if (totalDataFiles <= 0) {
            return false;
        }
        double coverage = (double) mappedFiles / totalDataFiles;
        if (coverage >= COVERAGE_SKIP_THRESHOLD) {
            return false;
        }
        LOG.debug("event=index_coverage_skip indexId={} coverage={}", indexId, coverage);
        if (evalConfig.evalMetrics() != null) {
            evalConfig.evalMetrics().recordCoverageSkip();
        }
        return true;
    }

    // ── LIMIT pruning (delegated to LimitPruningStrategy) ──────────────

    private List<ConnectorSplit> applyLimitPruning(
            List<ConnectorSplit> filtered,
            List<IndexHint> indexHints,
            SplitEvalContext ctx) {
        return LimitPruningStrategy.apply(filtered, indexHints, ctx);
    }
}
