package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import org.pangrova.planner.api.adaptive.AdaptiveDecision;

import java.util.Map;
import java.util.stream.Collectors;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.planner.core.analysis.SubPlanReoptimizer;
import org.pangrova.planner.core.adaptive.AdaptiveQueryExecutor;
import org.pangrova.planner.core.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.adaptive.JoinStrategy;
import org.pangrova.planner.core.adaptive.PartitionCoalescer;
import org.pangrova.planner.api.adaptive.ReOptimizationResult;
import org.pangrova.planner.core.adaptive.ShuffleStageStats;
import org.pangrova.planner.core.adaptive.SkewJoinHandler;
import org.pangrova.planner.api.adaptive.SkewResult;
import org.pangrova.protocol.rest.shuffle.ShuffleStageStatsReport;
import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.pangrova.shuffle.api.SkewDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator-side AQE handler that evaluates post-stage-completion
 * adaptive decisions using real Worker-reported shuffle statistics.
 *
 * <p>Integrates {@link AdaptiveQueryExecutor} for re-optimization triggers,
 * {@link PartitionCoalescer} for small-partition merging, and
 * {@link SkewJoinHandler} for skew detection. Split recommendations do not establish range execution support.</p>
 *
 * <p>Gated by {@link AdaptiveQueryConfig#enabled()}. When disabled,
 * {@link #evaluateStageCompletion} is a no-op returning {@link AdaptiveDecision#NONE}.
 * {@link #buildStatsFromReport} executes unconditionally (it has no config dependency).</p>
 */
public final class AdaptiveQueryEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveQueryEvaluator.class);

    private final AdaptiveQueryConfig config;
    private final AdaptiveQueryExecutor adaptiveQueryExecutor;
    private final PartitionCoalescer partitionCoalescer;
    private final SkewJoinHandler skewJoinHandler;
    private final SubPlanReoptimizer subPlanReoptimizer;

    /** Creates an adaptive evaluator with its executor and reoptimizer. */
    public AdaptiveQueryEvaluator(SkewDetector skewDetector, AdaptiveQueryConfig config,
                                  AdaptiveQueryExecutor adaptiveQueryExecutor,
                                  SubPlanReoptimizer subPlanReoptimizer) {
        this.config = config != null ? config : AdaptiveQueryConfig.DISABLED;
        this.adaptiveQueryExecutor = adaptiveQueryExecutor;
        this.partitionCoalescer = new PartitionCoalescer();
        this.skewJoinHandler = new SkewJoinHandler(skewDetector);
        this.subPlanReoptimizer = subPlanReoptimizer;
    }

    public AdaptiveQueryEvaluator(SkewDetector skewDetector, AdaptiveQueryConfig config,
                                  AdaptiveQueryExecutor adaptiveQueryExecutor) {
        this(skewDetector, config, adaptiveQueryExecutor, null);
    }

    /**
     * Evaluates adaptive re-optimization for a completed stage using
     * real shuffle statistics from Worker reports.
     *
     * <p>Returns an {@link AdaptiveDecision} capturing all AQE recommendations.
     * On the successful evaluation path the decision is also stored on the Stage
     * via {@link Stage#setAdaptiveDecision}; on the disabled or exception paths
     * the Stage is not modified. When AQE is disabled, returns
     * {@link AdaptiveDecision#NONE} immediately.</p>
     */
    public AdaptiveDecision evaluateStageCompletion(Stage finished) {
        if (!config.enabled() || adaptiveQueryExecutor == null) {
            return AdaptiveDecision.NONE;
        }
        try {
            ShuffleStageStats stats = buildStatsFromReport(finished);
            AdaptiveDecision decision = buildDecision(finished, stats);
            finished.setAdaptiveDecision(decision);
            logDecision(finished, decision);
            return decision;
        } catch (Exception e) {
            LOG.warn("event=aqe_evaluation_error stageId={} error={}",
                    finished.stageId(), e.getMessage(), e);
            return AdaptiveDecision.NONE;
        }
    }

    /**
     * Returns the sub-plan reoptimizer, or null if not configured.
     * Used by the DAG dispatch engine to reoptimize downstream plan fragments
     * when a stage decision indicates re-optimization is needed.
     */
    public SubPlanReoptimizer subPlanReoptimizer() {
        return subPlanReoptimizer;
    }

    private AdaptiveDecision buildDecision(Stage finished, ShuffleStageStats stats) {
        ReOptimizationResult reOpt = evaluateReOptimization(finished, stats);
        CoalesceResult coalesce = evaluatePartitionCoalescing(finished, stats);
        SkewResult skew = evaluateSkewHandling(finished, stats);
        ParallelismEvaluation parallelism = evaluateDynamicParallelism(finished, stats);
        return new AdaptiveDecision(
                reOpt.triggered(), reOpt.newJoinStrategy(),
                coalesce, skew, parallelism.adjusted(), parallelism.planned());
    }

    private static void logDecision(Stage finished, AdaptiveDecision decision) {
        if (decision.hasAdjustments()) {
            LOG.info("event=aqe_decision stageId={} adjustments=true "
                    + "reopt={} coalesce={} skew={} parallelism={}->{}",
                    finished.stageId(), decision.reOptimizationTriggered(),
                    decision.coalesceResult().isCoalesced(), decision.skewResult().hasSkew(),
                    decision.plannedParallelism(), decision.adjustedParallelism());
        }
    }

    /** Builds shuffle statistics from a completed stage report. */
    public ShuffleStageStats buildStatsFromReport(Stage finished) {
        ShuffleStageStatsReport report = finished.shuffleStatsReport();
        String stageId = String.valueOf(finished.stageId());
        if (report == null) {
            return new ShuffleStageStats(stageId, 0L, 0L, 0L, 0L, Map.of());
        }
        Map<Integer, ShufflePartitionStats> spiStats = convertPartitionStats(report);
        long actualBytes = report.shuffleBytes();
        long actualRows = report.shuffleRows();
        long estimatedBytes = finished.plan().estimatedShuffleBytes();
        long estimatedRows = finished.plan().estimatedShuffleRows();
        // When planner statistics are unavailable (-1), fall back to actual values
        // to prevent shouldReOptimize from unconditionally triggering.
        // When real estimates are available, shouldReOptimize can detect significant
        // deviations and trigger join strategy switching (sort-merge → broadcast hash).
        if (estimatedBytes < 0) {
            estimatedBytes = actualBytes;
        }
        if (estimatedRows < 0) {
            estimatedRows = actualRows;
        }
        return new ShuffleStageStats(
                stageId,
                actualBytes,
                actualRows,
                estimatedBytes,
                estimatedRows,
                spiStats);
    }

    private static Map<Integer, ShufflePartitionStats> convertPartitionStats(
            ShuffleStageStatsReport report) {
        if (report.partitionStats().isEmpty()) {
            return Map.of();
        }
        return report.partitionStats().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> new ShufflePartitionStats(
                                e.getKey(),
                                e.getValue().dataBytes(),
                                e.getValue().rowCount())));
    }

    private ReOptimizationResult evaluateReOptimization(Stage finished, ShuffleStageStats stats) {
        ReOptimizationResult result = adaptiveQueryExecutor.processStageCompletion(
                stats, JoinStrategy.SORT_MERGE_JOIN, config.broadcastThresholdBytes());
        if (result.triggered()) {
            LOG.info("event=aqe_reoptimize_triggered stageId={} reason={} "
                    + "actual={} joinStrategyChange={}",
                    finished.stageId(), result.reason(),
                    stats.actualDataSize(),
                    result.newJoinStrategy().map(Enum::name).orElse("none"));
        } else {
            LOG.debug("event=aqe_reoptimize_skipped stageId={} actual={}",
                    finished.stageId(), stats.actualDataSize());
        }
        return result;
    }

    private CoalesceResult evaluatePartitionCoalescing(Stage finished, ShuffleStageStats stats) {
        CoalesceResult result = partitionCoalescer.evaluate(stats, config);
        if (result.isCoalesced()) {
            LOG.info("event=aqe_partition_coalesce stageId={} reason=small_partitions "
                    + "originalPartitions={} coalescedPartitions={}",
                    finished.stageId(),
                    result.originalCount(), result.coalescedCount());
        }
        return result;
    }

    private SkewResult evaluateSkewHandling(Stage finished, ShuffleStageStats stats) {
        SkewResult result = skewJoinHandler.detectSkew(stats, config);
        if (result.hasSkew()) {
            LOG.info("event=aqe_skew_detected stageId={} reason=data_skew "
                    + "skewedPartitions={} medianBytes={} maxBytes={}",
                    finished.stageId(),
                    result.skewedPartitions().size(),
                    result.medianBytes(), result.maxBytes());
        }
        return result;
    }

    private record ParallelismEvaluation(int planned, int adjusted) {}

    private ParallelismEvaluation evaluateDynamicParallelism(Stage finished, ShuffleStageStats stats) {
        int planned = finished.plan().parallelism();
        int adjusted = adaptiveQueryExecutor.evaluateParallelism(stats, planned);
        if (adjusted != planned) {
            LOG.info("event=aqe_dynamic_parallelism stageId={} planned={} adjusted={}",
                    finished.stageId(), planned, adjusted);
        }
        return new ParallelismEvaluation(planned, adjusted);
    }
}
