package org.pangrova.worker.task.index;

import org.pangrova.protocol.index.IndexEvalHintProtoCodec;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.eval.IndexEvalDescriptor;
import org.pangrova.spi.index.eval.IndexEvalHint;
import org.pangrova.spi.index.eval.IndexFileEvaluator;
import org.pangrova.spi.index.eval.IndexFileEvaluatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Worker-side L2 index evaluation filter.
 *
 * <p>Examines {@link IndexEvalHintInfo} messages attached to splits by the
 * coordinator. For each hint, loads the index file and evaluates the predicate.
 * Splits where ALL data files are SKIP are removed before native execution.</p>
 *
 * <p>The {@link IndexFileEvaluator} implementation is discovered via
 * {@link ServiceLoader} at first use. If no implementation is found on the
 * classpath, hints are ignored and all splits pass through (conservative).</p>
 */
public final class IndexEvalSplitFilter {

    private static final Logger LOG = LoggerFactory.getLogger(IndexEvalSplitFilter.class);

    private final IndexFileEvaluator evaluator;

    private IndexEvalSplitFilter(IndexFileEvaluator evaluator) {
        this.evaluator = evaluator;
    }

    /**
     * Creates a filter by discovering an IndexFileEvaluator via ServiceLoader.
     * Returns null if no evaluator is available (indexing jar not on classpath).
     */
    public static IndexEvalSplitFilter tryCreate() {
        try {
            ServiceLoader<IndexFileEvaluatorFactory> loader =
                    ServiceLoader.load(IndexFileEvaluatorFactory.class);
            for (IndexFileEvaluatorFactory factory : loader) {
                IndexFileEvaluator evaluator = factory.create();
                LOG.info("event=index_eval_filter_initialized factory={}",
                        factory.getClass().getName());
                return new IndexEvalSplitFilter(evaluator);
            }
        } catch (Exception e) {
            LOG.debug("event=index_eval_filter_init_failed error={}", e.getMessage());
        }
        return null;
    }

    /**
     * Filters splits by evaluating L2 index hints.
     *
     * <p>For each split with hints, evaluates all hints. If any hint returns
     * SKIP for a data file, that file is considered prunable. If ALL data files
     * in a split are prunable, the entire split is removed.</p>
     *
     * <p>Conservative: any evaluation failure keeps the split.</p>
     *
     * @param splits decoded SplitInfo messages from TaskRequest
     * @return filtered list (subset of input, never larger)
     */
    public List<SplitInfo> filterSplits(List<SplitInfo> splits) {
        if (evaluator == null) {
            return splits;
        }
        List<SplitInfo> result = new ArrayList<>(splits.size());
        int skipped = evaluateAndFilter(splits, result);
        if (skipped > 0) {
            LOG.info("event=worker_l2_index_filter total={} skipped={} kept={}",
                    splits.size(), skipped, result.size());
        }
        return result;
    }

    private int evaluateAndFilter(List<SplitInfo> splits, List<SplitInfo> result) {
        int skipped = 0;
        for (SplitInfo split : splits) {
            if (split.getIndexEvalHintsCount() == 0 || shouldKeepSplit(split)) {
                result.add(split);
            } else {
                skipped++;
            }
        }
        return skipped;
    }

    private boolean shouldKeepSplit(SplitInfo split) {
        List<IndexEvalHint> hints = IndexEvalHintProtoCodec.extractHints(split);
        String splitFilePath = split.getFilePath();
        if (hints.size() > 1) {
            return evaluateMultiHintKeep(hints, splitFilePath);
        }
        for (IndexEvalHint hint : hints) {
            if (evaluateHintSkips(hint, splitFilePath)) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateMultiHintKeep(List<IndexEvalHint> hints, String splitFilePath) {
        List<IndexEvalResult> results = new ArrayList<>(hints.size());
        for (IndexEvalHint hint : hints) {
            try {
                IndexEvalDescriptor descriptor = IndexEvalDescriptor.fromHint(hint);
                IndexEvalResult result = evaluator.evaluateFile(
                        descriptor, splitFilePath, hint.indexFilePath(),
                        hint.predicate(), null);
                results.add(result);
            } catch (Exception e) {
                LOG.debug("event=worker_l2_multi_hint_eval_failed file={} indexId={} error={}",
                        splitFilePath, hint.indexId(), e.getMessage());
                results.add(IndexEvalResult.UNKNOWN);
            }
        }
        IndexEvalResult aggregated = aggregateResults(results);
        if (aggregated == IndexEvalResult.SKIP) {
            LOG.debug("event=worker_l2_multi_hint_skip file={} hintCount={}", splitFilePath, hints.size());
            return false;
        }
        return true;
    }

    /**
     * Aggregates multiple evaluation results with AND semantics:
     * any SKIP → SKIP, any KEEP + no SKIP → KEEP, all UNKNOWN → UNKNOWN.
     *
     * <p>AND semantics is correct because the coordinator generates one hint
     * per predicate extracted from the WHERE clause, and SQL WHERE predicates
     * are AND-connected. If any single predicate proves the data file contains
     * no matching rows, the entire split can be skipped.</p>
     *
     * <p>This method MUST NOT be used for OR-connected predicates. If OR
     * support is added, the aggregation must change to: all SKIP → SKIP,
     * any KEEP → KEEP.</p>
     */
    public static IndexEvalResult aggregateResults(List<IndexEvalResult> results) {
        boolean hasKeep = false;
        for (IndexEvalResult r : results) {
            if (r == IndexEvalResult.SKIP) {
                return IndexEvalResult.SKIP;
            }
            if (r == IndexEvalResult.KEEP) {
                hasKeep = true;
            }
        }
        return hasKeep ? IndexEvalResult.KEEP : IndexEvalResult.UNKNOWN;
    }

    private boolean evaluateHintSkips(IndexEvalHint hint, String splitFilePath) {
        try {
            IndexEvalDescriptor descriptor = toDescriptor(hint);
            IndexEvalResult evalResult = evaluator.evaluateFile(
                    descriptor,
                    splitFilePath,
                    hint.indexFilePath(),
                    hint.predicate(),
                    null);
            if (evalResult == IndexEvalResult.SKIP) {
                LOG.debug("event=worker_l2_skip file={} indexId={}",
                        splitFilePath, hint.indexId());
                return true;
            }
        } catch (Exception e) {
            LOG.debug("event=worker_l2_eval_failed file={} indexId={} error={}",
                    splitFilePath, hint.indexId(), e.getMessage());
        }
        return false;
    }

    /**
     * Builds a worker-side {@link IndexEvalDescriptor} carrying only the
     * three fields the evaluator actually consumes. Replaces the previous
     * "_eval" placeholder {@code IndexDescriptor}, which leaked sentinel
     * values into logs, metrics, and any downstream code that read the
     * catalog/schema/table fields.
     */
    static IndexEvalDescriptor toDescriptor(IndexEvalHint hint) {
        return IndexEvalDescriptor.fromHint(hint);
    }
}
