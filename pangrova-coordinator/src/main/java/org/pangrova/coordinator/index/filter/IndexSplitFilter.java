package org.pangrova.coordinator.index.filter;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.coordinator.index.IndexDescriptorCache;
import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.control.IndexMetadataStore;
import org.pangrova.indexing.eval.IndexEvaluationFailures;
import org.pangrova.planner.index.IndexHint;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.eval.IndexEvalHint;
import org.pangrova.spi.index.eval.IndexEvaluatedSplit;
import org.pangrova.spi.index.eval.SplitIndexEvalResult;
import org.pangrova.spi.index.predicate.EqualTo;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.InList;
import org.pangrova.spi.index.statistics.FileColumnStatistics;
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
    private final IndexDescriptorCache descriptorCache;
    private final IndexEvalConfig evalConfig;
    private final ExecutorService evalPool;

    /**
     * @param metadataStore     index metadata access
     * @param descriptorCache   cached descriptor lookups (nullable — falls back to store)
     * @param evalConfig        evaluation configuration (metrics, timeout, parallelism)
     */
    public IndexSplitFilter(
            IndexMetadataStore metadataStore,
            IndexDescriptorCache descriptorCache,
            IndexEvalConfig evalConfig) {
        this.metadataStore = Objects.requireNonNull(metadataStore);
        this.descriptorCache = descriptorCache;
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
            ConnectorAccessorResolver accessorResolver) {
        return filterSplits(splits, indexHints, catalogName, accessorResolver, null);
    }

    /** Filters splits with snapshot-aware consistency (convenience overload without L0/LIMIT). */
    public List<ConnectorSplit> filterSplits(
            List<ConnectorSplit> splits,
            List<IndexHint> indexHints,
            String catalogName,
            ConnectorAccessorResolver accessorResolver,
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
            ConnectorAccessorResolver accessorResolver,
            String querySnapshotToken,
            Map<String, List<FileColumnStatistics>> fileStatistics,
            long limitK) {
        IndexEvaluationFailures.checkInterrupted();
        if (indexHints.isEmpty()) {
            return splits;
        }

        // Batch-fetch index file mappings — two-phase: counts first, then full for qualifying
        List<String> indexIds = indexHints.stream()
                .map(IndexHint::indexId).distinct().toList();
        List<String> dataFilePaths = splits.stream()
                .flatMap(s -> s.dataFilePaths().stream())
                .distinct().toList();
        long metadataStart = System.currentTimeMillis();

        // Phase 1: lightweight count query to filter low-coverage indexes
        Map<String, Integer> coverageCounts = metadataStore.getIndexFileMappingCounts(
                indexIds, dataFilePaths);
        int totalFiles = dataFilePaths.size();
        LOG.debug("event=coverage_check indexIds={} totalFiles={} coverageCounts={}",
                indexIds, totalFiles, coverageCounts);
        List<String> qualifyingIndexIds = indexIds.stream()
                .filter(id -> {
                    int count = coverageCounts.getOrDefault(id, 0);
                    if (totalFiles > 0
                            && (double) count / totalFiles < COVERAGE_SKIP_THRESHOLD) {
                        if (evalConfig.evalMetrics() != null) {
                            evalConfig.evalMetrics().recordCoverageSkip();
                        }
                        LOG.debug("event=index_coverage_skip indexId={} count={} total={} ratio={}",
                                id, count, totalFiles, (double) count / totalFiles);
                        return false;
                    }
                    return true;
                })
                .toList();

        // Phase 2: full mapping fetch only for qualifying indexes
        Map<String, Map<String, String>> indexFileMappings;
        if (qualifyingIndexIds.isEmpty()) {
            indexFileMappings = Map.of();
        } else {
            indexFileMappings = metadataStore.batchGetIndexFileMappings(
                    qualifyingIndexIds, dataFilePaths, querySnapshotToken);
        }
        if (evalConfig.evalMetrics() != null) {
            evalConfig.evalMetrics().recordMetadataBatchLatency(System.currentTimeMillis() - metadataStart);
        }

        // Filter index hints to only qualifying indexes
        List<IndexHint> effectiveHints = indexHints.stream()
                .filter(h -> qualifyingIndexIds.contains(h.indexId()))
                .toList();
        if (effectiveHints.isEmpty()) {
            return splits;
        }

        // Batch-fetch descriptors for qualifying indexes only
        Map<String, IndexDescriptor> descriptorsByIndex = fetchDescriptors(qualifyingIndexIds);

        // Resolve NativeIndexEvaluator and capabilities ONCE
        NativeIndexEvaluator nativeEvaluator = resolveNativeEvaluator(
                catalogName, accessorResolver);
        List<NativeIndexCapability> nativeCapabilities = nativeEvaluator != null
                ? resolveNativeCapabilities(catalogName, accessorResolver)
                : List.of();

        var ctx = new SplitEvalContext(
                nativeEvaluator, nativeCapabilities,
                indexFileMappings, descriptorsByIndex, querySnapshotToken,
                fileStatistics, limitK, null, dataFilePaths.size());

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

    private Map<String, IndexDescriptor> fetchDescriptors(List<String> indexIds) {
        if (descriptorCache != null) {
            return descriptorCache.findByIds(indexIds);
        }
        return metadataStore.findIndexesByIds(indexIds);
    }

    private NativeIndexEvaluator resolveNativeEvaluator(
            String catalogName, ConnectorAccessorResolver resolver) {
        try {
            NativeIndexEvaluator evaluator = resolver.resolveNativeIndexEvaluator(catalogName);
            if (evaluator == null) {
                LOG.warn("event=native_evaluator_unavailable catalog={} "
                        + "detail=L1 index filtering disabled, only L0 statistics pruning active",
                        catalogName);
            }
            return evaluator;
        } catch (org.pangrova.spi.security.AccessDeniedException | SecurityException authorityFailure) {
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
            String catalogName, ConnectorAccessorResolver resolver) {
        try {
            return resolver.resolveNativeCapabilities(catalogName);
        } catch (org.pangrova.spi.security.AccessDeniedException | SecurityException authorityFailure) {
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
        if (AvailabilityWindowChecker.isOutsideAvailabilityWindow(dataFilePath, split, indexHints, ctx)) {
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
        IndexDescriptor descriptor = ctx.descriptorsByIndex().get(hint.indexId());
        if (descriptor == null) {
            return null;
        }
        if (descriptor.indexType() == IndexType.BLOOM_FILTER
                && !hasNativeBloomKeyContract(hint.predicate())) {
            // The protocol string variant also carries Calcite DATE/TIMESTAMP
            // and DECIMAL fallback values. Their toString() is not the index
            // writer's verified canonical key, so Native must not prune them.
            return null;
        }
        Map<String, String> fileMappings = ctx.indexFileMappings()
                .getOrDefault(hint.indexId(), Map.of());
        if (shouldSkipLowCoverage(hint.indexId(), fileMappings.size(), ctx.totalDataFiles())) {
            return null;
        }
        String indexFilePath = fileMappings.get(dataFilePath);
        if (indexFilePath == null) {
            return null;
        }
        return new IndexEvalHint(hint.indexId(), descriptor.indexType(),
                hint.predicate(), indexFilePath, descriptor.generation());
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
